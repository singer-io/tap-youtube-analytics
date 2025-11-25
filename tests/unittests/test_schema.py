import unittest
from unittest.mock import patch, MagicMock, mock_open
from tap_youtube_analytics.schema import get_abs_path, load_schema_references, _load_schema_for_stream, get_schemas
from tap_youtube_analytics.streams import STREAMS
import os

class TestSchema(unittest.TestCase):
    @patch("os.path.dirname")
    @patch("os.path.realpath")
    def test_get_abs_path(self, mock_realpath, mock_dirname):
        """Test get_abs_path function"""
        mock_realpath.return_value = "/path/to/schema.py"
        mock_dirname.return_value = "/path/to"
        result = get_abs_path("schemas/shared")
        self.assertEqual(result, "/path/to/schemas/shared")

    @patch("tap_youtube_analytics.schema.os.path.exists")
    @patch("tap_youtube_analytics.schema.os.listdir")
    @patch("tap_youtube_analytics.schema.get_abs_path")
    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("json.load")
    def test_load_schema_references(self, mock_json_load, mock_open_file, mock_get_abs_path, mock_listdir, mock_exists):
        """Test load_schema_references function"""
        mock_exists.return_value = True
        mock_listdir.return_value = ["shared1.json", "shared2.json"]
        mock_get_abs_path.return_value = "/mocked/path/schemas/shared"
        mock_json_load.return_value = {"key": "value"}

        result = load_schema_references()
        
        # Verify the function was called and returned data
        self.assertIsInstance(result, dict)
        # Check that files were processed (exact keys depend on implementation)
        mock_listdir.assert_called_once()
        mock_exists.assert_called()

    @patch("tap_youtube_analytics.schema.os.path.exists")
    @patch("tap_youtube_analytics.schema.get_abs_path")
    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("json.load")
    def test_load_schema_for_stream_candidate_exists(self, mock_json_load, mock_open_file, mock_get_abs_path, mock_exists):
        """Test _load_schema_for_stream when candidate schema exists"""
        mock_exists.side_effect = lambda path: "stream1.json" in path
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        mock_json_load.return_value = {"key": "value"}
        
        result = _load_schema_for_stream("stream1")
        self.assertEqual(result, {"key": "value"})

    @patch("tap_youtube_analytics.schema.os.path.exists")
    @patch("tap_youtube_analytics.schema.get_abs_path")
    @patch("builtins.open", new_callable=mock_open, read_data='{"key": "value"}')
    @patch("json.load")
    def test_load_schema_for_stream_fallback_exists(self, mock_json_load, mock_open_file, mock_get_abs_path, mock_exists):
        """Test _load_schema_for_stream when fallback schema exists"""
        mock_exists.side_effect = lambda path: "reports.json" in path
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        mock_json_load.return_value = {"key": "value"}
        
        result = _load_schema_for_stream("nonexistent_stream")
        self.assertEqual(result, {"key": "value"})

    @patch("tap_youtube_analytics.schema.os.path.exists")
    @patch("tap_youtube_analytics.schema.get_abs_path")
    def test_load_schema_for_stream_no_schema(self, mock_get_abs_path, mock_exists):
        """Test _load_schema_for_stream when no schema exists"""
        mock_exists.return_value = False
        mock_get_abs_path.side_effect = lambda x: f"/mocked/path/{x}"
        
        with self.assertRaises(FileNotFoundError):
            _load_schema_for_stream("nonexistent_stream")

    @patch("tap_youtube_analytics.schema.load_schema_references")
    @patch("tap_youtube_analytics.schema._load_schema_for_stream")
    @patch("tap_youtube_analytics.schema.STREAMS", {
        "stream1": MagicMock(key_properties=["id"], replication_keys=["updated_at"], replication_method="INCREMENTAL"),
        "stream2": MagicMock(key_properties=["key"], replication_keys=[], replication_method=None),
    })
    @patch("singer.resolve_schema_references")
    @patch("singer.metadata.get_standard_metadata")
    @patch("singer.metadata.to_map")
    @patch("singer.metadata.write")
    @patch("singer.metadata.to_list")
    def test_get_schemas(
        self,
        mock_to_list,
        mock_write,
        mock_to_map,
        mock_get_standard_metadata,
        mock_resolve_schema_references,
        mock_load_schema_for_stream,
        mock_load_schema_references,
    ):
        """Test get_schemas function"""
        mock_load_schema_references.return_value = {"shared/ref.json": {"type": "object"}}
        mock_load_schema_for_stream.side_effect = lambda stream_name: {
            "type": "object",
            "properties": {"field1": {"type": "string"}}
        }
        mock_resolve_schema_references.side_effect = lambda raw_schema, refs: raw_schema
        mock_get_standard_metadata.return_value = [{"metadata": {"inclusion": "automatic"}}]
        mock_to_map.return_value = {"": {"table-key-properties": ["id"]}}
        mock_write.side_effect = lambda m_map, path, key, value: m_map
        mock_to_list.return_value = [{"breadcrumb": [], "metadata": {"inclusion": "automatic"}}]

        schemas, field_metadata = get_schemas()

        self.assertIn("stream1", schemas)
        self.assertIn("stream2", schemas)
        self.assertIn("stream1", field_metadata)
        self.assertIn("stream2", field_metadata)


if __name__ == "__main__":
    unittest.main()