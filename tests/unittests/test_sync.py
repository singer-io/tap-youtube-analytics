import unittest
import singer
from unittest.mock import patch, MagicMock, call
from tap_youtube_analytics.sync import sync, update_currently_syncing, write_schema
from tap_youtube_analytics.client import Client
from singer.catalog import Catalog
from singer import Transformer

class TestSync(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock(spec=Client)
        self.mock_config = {"key": "value"}
        self.mock_catalog = MagicMock(spec=Catalog)
        self.mock_state = {}

    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing")
    @patch("tap_youtube_analytics.streams.STREAMS")
    def test_sync_success(self, mock_streams, mock_get_currently_syncing, mock_transformer):
        """Test successful sync"""
        mock_stream_instance = MagicMock() 
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 10
        mock_stream_instance.parent = None
        
        mock_stream_class = MagicMock()
        mock_stream_class.return_value = mock_stream_instance
        
        # Mock STREAMS as a dictionary with proper lookup
        mock_streams.__getitem__.side_effect = lambda stream_name: mock_stream_class
        mock_streams.__contains__.return_value = True

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.stream = "test_stream"
        self.mock_catalog.get_selected_streams.return_value = [mock_catalog_stream]
        self.mock_catalog.get_stream.return_value = mock_catalog_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

        # Verify sync was called
        mock_stream_instance.sync.assert_called_once()

    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing")
    @patch("tap_youtube_analytics.streams.STREAMS")
    def test_sync_with_parent_stream(self, mock_streams, mock_get_currently_syncing, mock_transformer):
        """Test sync with parent-child stream relationship"""

        # Create separate mock instances for child and parent streams
        mock_child_instance = MagicMock()
        mock_child_instance.is_selected.return_value = True
        mock_child_instance.sync.return_value = 5
        mock_child_instance.parent = "parent_stream"

        mock_parent_instance = MagicMock()
        mock_parent_instance.is_selected.return_value = True
        mock_parent_instance.sync.return_value = 10
        mock_parent_instance.parent = None
        mock_parent_instance.children = ["child_stream"]
        mock_parent_instance.child_to_sync = []
        mock_parent_instance.write_schema = MagicMock()

        # Create mock classes that return the appropriate instances
        mock_child_class = MagicMock()
        mock_child_class.return_value = mock_child_instance

        mock_parent_class = MagicMock()
        mock_parent_class.return_value = mock_parent_instance

        # Mock STREAMS to return different classes based on stream name
        def mock_stream_factory(stream_name):
            if stream_name == "child_stream":
                return mock_child_class
            elif stream_name == "parent_stream":
                return mock_parent_class
            else:
                return mock_child_class  # fallback

        mock_streams.__getitem__.side_effect = mock_stream_factory
        mock_streams.__contains__.return_value = True

        # Set up catalog streams for both child and parent
        mock_child_catalog_stream = MagicMock()
        mock_child_catalog_stream.stream = "child_stream"

        mock_parent_catalog_stream = MagicMock()
        mock_parent_catalog_stream.stream = "parent_stream"

        # Only child stream is initially selected
        self.mock_catalog.get_selected_streams.return_value = [mock_child_catalog_stream]

        # Mock get_stream to return appropriate catalog stream based on name
        def mock_catalog_get_stream(stream_name):
            if stream_name == "child_stream":
                return mock_child_catalog_stream
            elif stream_name == "parent_stream":
                return mock_parent_catalog_stream

        self.mock_catalog.get_stream.side_effect = mock_catalog_get_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

        # Verify child stream sync was NOT called (child streams are skipped)
        mock_child_instance.sync.assert_not_called()

        # Verify parent stream sync WAS called (parent gets added and synced)
        mock_parent_instance.sync.assert_called_once()

        # Verify write_schema was called on parent stream
        mock_parent_instance.write_schema.assert_called_once()

    @patch("singer.write_state")
    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    def test_update_currently_syncing(self, mock_set_currently_syncing, mock_get_currently_syncing, mock_write_state):
        """Test update_currently_syncing function"""
        mock_get_currently_syncing.return_value = "test_stream"
        state = {"currently_syncing": "test_stream"}

        # Test with stream_name=None
        update_currently_syncing(state, None)
        self.assertNotIn("currently_syncing", state)
        mock_write_state.assert_called_once_with(state)
        
        # Test with a stream name
        state = {}
        update_currently_syncing(state, "new_stream")
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_with(state)

    @patch("tap_youtube_analytics.streams.STREAMS")
    def test_write_schema(self, mock_streams):
        """Test write_schema function"""
        # Create a mock stream that doesn't cause recursion
        mock_stream_instance = MagicMock()
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.children = ["child_stream"]
        mock_stream_instance.write_schema = MagicMock()
        
        # Set child_to_sync as a simple list to avoid recursion
        mock_stream_instance.child_to_sync = []
        
        # Create a separate mock for child streams to avoid recursion
        mock_child_stream = MagicMock()
        mock_child_stream.is_selected.return_value = True
        mock_child_stream.children = []  # No children to prevent recursion
        mock_child_stream.write_schema = MagicMock()
        mock_child_stream.child_to_sync = []
        
        def mock_stream_factory(stream_name):
            if stream_name == "child_stream":
                return lambda client, catalog_stream: mock_child_stream
            else:
                return lambda client, catalog_stream: mock_stream_instance
        
        mock_streams.__getitem__.side_effect = mock_stream_factory
        mock_streams.__contains__.return_value = True

        # Mock the catalog to return appropriate streams
        mock_child_catalog_stream = MagicMock()
        mock_child_catalog_stream.stream = "child_stream"
        self.mock_catalog.get_stream.return_value = mock_child_catalog_stream

        # Call the function
        write_schema(mock_stream_instance, self.mock_client, ["child_stream"], self.mock_catalog)

        # Verify write_schema was called on the main stream
        mock_stream_instance.write_schema.assert_called_once()


if __name__ == "__main__":
    unittest.main()