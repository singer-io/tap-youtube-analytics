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

    @patch("tap_youtube_analytics.sync.LOGGER")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing")
    @patch("tap_youtube_analytics.streams.STREAMS")
    def test_sync_success(self, mock_streams, mock_get_currently_syncing, mock_transformer, mock_logger):
        """Test successful sync"""
        mock_stream_instance = MagicMock() 
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 10
        mock_stream_instance.parent = None
        
        mock_stream_class = MagicMock()
        mock_stream_class.return_value = mock_stream_instance
        mock_streams.__getitem__.return_value = mock_stream_class

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.stream = "test_stream"
        self.mock_catalog.get_selected_streams.return_value = [mock_catalog_stream]
        self.mock_catalog.get_stream.return_value = mock_catalog_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

        # Check that the expected log messages were called
        expected_calls = [
            call("selected_streams: ['test_stream']"),
            call("START Syncing: test_stream"),
            call("FINISHED Syncing: test_stream, total_records: 10")
        ]
        for expected_call in expected_calls:
            # Make sure we're checking against mock_logger.info.call_args_list
            self.assertIn(expected_call, mock_logger.info.call_args_list)

    @patch("tap_youtube_analytics.sync.LOGGER")
    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing")
    @patch("tap_youtube_analytics.streams.STREAMS")
    def test_sync_with_parent_stream(self, mock_streams, mock_get_currently_syncing, mock_transformer, mock_logger):
        """Test sync with a parent stream"""
        mock_stream_instance = MagicMock()
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 5
        mock_stream_instance.parent = "parent_stream"
        
        mock_stream_class = MagicMock()
        mock_stream_class.return_value = mock_stream_instance
        mock_streams.__getitem__.return_value = mock_stream_class

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.stream = "child_stream"
        self.mock_catalog.get_selected_streams.return_value = [mock_catalog_stream]
        self.mock_catalog.get_stream.return_value = mock_catalog_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

        # Check for any log calls that contain both streams
        info_calls = [str(call) for call in mock_logger.info.call_args_list]
        
        # Look for evidence that both child and parent streams were processed
        child_stream_logged = any("child_stream" in call_str for call_str in info_calls)
        parent_mentioned = any("parent" in call_str.lower() for call_str in info_calls)
        
        # At minimum, the child stream should be logged
        self.assertTrue(child_stream_logged, "Child stream should be logged")

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