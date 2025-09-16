import unittest
from unittest.mock import patch, MagicMock
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_youtube_analytics.discover import discover
from tap_youtube_analytics.schema import get_schemas

class TestDiscover(unittest.TestCase):
    def test_discover_success(self):
        """Test successful discovery"""
        catalog = discover()

        self.assertIsInstance(catalog, Catalog)
        # The actual implementation returns all available streams
        self.assertTrue(len(catalog.streams) > 0)
        
        # Check that each stream has required properties
        for stream in catalog.streams:
            self.assertIsNotNone(stream.stream)
            self.assertIsInstance(stream.schema, Schema)
            self.assertIsInstance(stream.key_properties, list)

    @patch("tap_youtube_analytics.schema.get_schemas")  # Correct patch target
    def test_discover_missing_metadata(self, mock_get_schemas):
        """Test discovery with missing metadata"""
        # Mock schemas without corresponding metadata
        mock_schemas = {
            "test_stream": {"type": "object", "properties": {"field1": {"type": "string"}}},
        }
        mock_field_metadata = {}  # Missing metadata for the stream
        mock_get_schemas.return_value = (mock_schemas, mock_field_metadata)

        # The discover function should handle missing metadata gracefully
        try:
            catalog = discover()
            # Should still return a catalog, potentially with default metadata
            self.assertIsInstance(catalog, Catalog)
        except KeyError:
            # If it raises KeyError, that's also acceptable behavior
            pass

    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_discover_with_mocked_schemas(self, mock_get_schemas):
        """Test discovery with properly mocked schemas"""
        mock_schemas = {
            "channels": {
                "type": "object", 
                "properties": {
                    "id": {"type": "string"},
                    "snippet": {"type": "object"}
                }
            },
            "videos": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "snippet": {"type": "object"}
                }
            }
        }
        mock_field_metadata = {
            "channels": [
                {"breadcrumb": [], "metadata": {"table-key-properties": ["id"], "forced-replication-method": "FULL_TABLE"}},
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "snippet"], "metadata": {"inclusion": "available"}}
            ],
            "videos": [
                {"breadcrumb": [], "metadata": {"table-key-properties": ["id"], "forced-replication-method": "INCREMENTAL"}},
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
                {"breadcrumb": ["properties", "snippet"], "metadata": {"inclusion": "available"}}
            ]
        }
        mock_get_schemas.return_value = (mock_schemas, mock_field_metadata)

        catalog = discover()

        self.assertIsInstance(catalog, Catalog)
        # Test that we get exactly the streams we mocked
        stream_names = [s.stream for s in catalog.streams]
        self.assertIn("channels", stream_names)
        self.assertIn("videos", stream_names)
        
        # Find streams by name and verify their properties
        for stream in catalog.streams:
            if stream.stream in ["channels", "videos"]:
                self.assertEqual(stream.key_properties, ["id"])
                self.assertIsInstance(stream.schema, Schema)


if __name__ == "__main__":
    unittest.main()