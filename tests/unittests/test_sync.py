import unittest
from unittest.mock import patch, MagicMock
import singer
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
    @patch("tap_youtube_analytics.sync.streams")  # patched where it's used
    def test_sync_success(self, mock_streams_module, mock_get_currently_syncing, mock_transformer):
        """Test successful sync"""
        mock_stream_instance = MagicMock()
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 10
        mock_stream_instance.parent = None

        mock_stream_class = MagicMock()
        mock_stream_class.return_value = mock_stream_instance
        mock_streams_module.STREAMS = {"test_stream": mock_stream_class}

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.stream = "test_stream"
        self.mock_catalog.get_selected_streams.return_value = [mock_catalog_stream]
        self.mock_catalog.get_stream.return_value = mock_catalog_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        # Should run without raising
        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

        # Basic sanity checks
        mock_stream_instance.sync.assert_called_once()
        self.assertIn("test_stream", self.mock_state.get("currently_syncing", "") or "" or "")

    @patch("singer.Transformer")
    @patch("singer.get_currently_syncing")
    @patch("tap_youtube_analytics.sync.streams")  # patched where it's used
    def test_sync_with_parent_stream(self, mock_streams_module, mock_get_currently_syncing, mock_transformer):
        """Test sync with a parent stream"""
        # Child has a parent; sync should enqueue parent and continue (no KeyError)
        mock_stream_instance = MagicMock()
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.sync.return_value = 5
        mock_stream_instance.parent = "parent_stream"

        mock_stream_class = MagicMock()
        mock_stream_class.return_value = mock_stream_instance
        mock_streams_module.STREAMS = {
            "child_stream": mock_stream_class,
            "parent_stream": mock_stream_class,
        }

        mock_catalog_stream = MagicMock()
        mock_catalog_stream.stream = "child_stream"
        self.mock_catalog.get_selected_streams.return_value = [mock_catalog_stream]
        self.mock_catalog.get_stream.return_value = mock_catalog_stream

        mock_get_currently_syncing.return_value = None
        mock_transformer.return_value = MagicMock(spec=Transformer)

        # Should run without raising
        sync(self.mock_client, self.mock_config, self.mock_catalog, self.mock_state)

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

    @patch("tap_youtube_analytics.sync.streams")  # patched where it's used
    def test_write_schema(self, mock_streams_module):
        """Test write_schema function"""
        # Create a mock stream that doesn't cause recursion
        mock_stream_instance = MagicMock()
        mock_stream_instance.is_selected.return_value = True
        mock_stream_instance.children = ["child_stream"]
        mock_stream_instance.write_schema = MagicMock()
        mock_stream_instance.child_to_sync = []

        # Create a separate mock for child streams to avoid recursion
        mock_child_stream = MagicMock()
        mock_child_stream.is_selected.return_value = True
        mock_child_stream.children = []  # No children to prevent recursion
        mock_child_stream.write_schema = MagicMock()
        mock_child_stream.child_to_sync = []

        # Mock the STREAMS dictionary on the streams module
        mock_child_stream_class = MagicMock(return_value=mock_child_stream)
        mock_streams_module.STREAMS = {"child_stream": mock_child_stream_class}

        # Mock the catalog to return appropriate streams
        mock_child_catalog_stream = MagicMock()
        mock_child_catalog_stream.stream = "child_stream"
        self.mock_catalog.get_stream.return_value = mock_child_catalog_stream

        # Call the function
        write_schema(mock_stream_instance, self.mock_client, ["child_stream"], self.mock_catalog)

        # Verify write_schema was called on the main stream and the child was queued
        mock_stream_instance.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream_instance.child_to_sync), 1)
        self.assertIs(mock_stream_instance.child_to_sync[0], mock_child_stream)


if __name__ == "__main__":
    unittest.main()
