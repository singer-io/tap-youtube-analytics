"""Unit tests for tap_youtube_analytics.discover."""
import unittest
from unittest.mock import MagicMock, patch

from singer.catalog import Catalog, Schema

from tap_youtube_analytics.discover import (
    DATA_API_STREAMS,
    _check_data_api_access,
    _check_reporting_api_access,
    check_stream_access,
    discover,
)
from tap_youtube_analytics.exceptions import (
    YoutubeAnalyticsBadRequestError,
    YoutubeAnalyticsForbiddenError,
    YoutubeAnalyticsNoAccessibleStreamsError,
    YoutubeAnalyticsUnauthorizedError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_AUTH_ERRORS = (YoutubeAnalyticsUnauthorizedError, YoutubeAnalyticsForbiddenError)

# Representative stream names used to build minimal mock schemas
_DATA_STREAM = "channels"        # must be in DATA_API_STREAMS
_REPORT_STREAM = "channel_basic" # must NOT be in DATA_API_STREAMS


def _minimal_schemas(*stream_names):
    """Return (schemas, field_metadata) dicts with the minimum content needed by discover()."""
    schemas = {
        n: {"type": "object", "properties": {"id": {"type": "string"}}}
        for n in stream_names
    }
    meta = {
        n: [{"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}]
        for n in stream_names
    }
    return schemas, meta


def _make_client(channels_side_effect=None, jobs_side_effect=None):
    """Build a MagicMock client that raises per-path side effects."""
    client = MagicMock()
    client.reporting_url = "https://youtubereporting.googleapis.com/v1"

    if channels_side_effect or jobs_side_effect:
        def _get(*args, **kwargs):
            path = kwargs.get("path", "")
            if path == "channels" and channels_side_effect:
                raise channels_side_effect
            if path == "jobs" and jobs_side_effect:
                raise jobs_side_effect
            return {}
        client.get.side_effect = _get

    return client


# ---------------------------------------------------------------------------
# TestCheckStreamAccess
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):
    """Generic helper: check_stream_access."""

    def test_returns_true_when_probe_succeeds(self):
        result = check_stream_access(
            "channels",
            probe_fn=lambda: None,
            auth_error_types=_AUTH_ERRORS,
        )
        self.assertTrue(result)

    def test_returns_false_on_401(self):
        def _raise():
            raise YoutubeAnalyticsUnauthorizedError("401")
        result = check_stream_access("channels", probe_fn=_raise, auth_error_types=_AUTH_ERRORS)
        self.assertFalse(result)

    def test_returns_false_on_403(self):
        def _raise():
            raise YoutubeAnalyticsForbiddenError("403")
        result = check_stream_access("channels", probe_fn=_raise, auth_error_types=_AUTH_ERRORS)
        self.assertFalse(result)

    def test_reraises_non_auth_error_when_fallback_false(self):
        def _raise():
            raise RuntimeError("unexpected")
        with self.assertRaises(RuntimeError):
            check_stream_access(
                "channels",
                probe_fn=_raise,
                auth_error_types=_AUTH_ERRORS,
                fallback_accessible=False,
            )

    def test_returns_true_on_non_auth_error_when_fallback_true(self):
        def _raise():
            raise RuntimeError("400 Bad Request")
        result = check_stream_access(
            "channels",
            probe_fn=_raise,
            auth_error_types=_AUTH_ERRORS,
            fallback_accessible=True,
        )
        self.assertTrue(result)


# ---------------------------------------------------------------------------
# TestCheckDataApiAccess
# ---------------------------------------------------------------------------

class TestCheckDataApiAccess(unittest.TestCase):

    def test_returns_true_when_probe_succeeds(self):
        client = _make_client()
        self.assertTrue(_check_data_api_access(client))
        call_kwargs = client.get.call_args.kwargs
        self.assertEqual(call_kwargs.get("path"), "channels")
        self.assertIn("mine", call_kwargs.get("params", {}))

    def test_returns_false_on_401(self):
        client = _make_client(channels_side_effect=YoutubeAnalyticsUnauthorizedError("401"))
        self.assertFalse(_check_data_api_access(client))

    def test_returns_false_on_403(self):
        client = _make_client(channels_side_effect=YoutubeAnalyticsForbiddenError("403"))
        self.assertFalse(_check_data_api_access(client))

    def test_returns_true_on_400_because_fallback_accessible(self):
        """A 400 (non-auth error) is treated as auth-OK due to fallback_accessible=True."""
        client = _make_client(channels_side_effect=YoutubeAnalyticsBadRequestError("400"))
        self.assertTrue(_check_data_api_access(client))


# ---------------------------------------------------------------------------
# TestCheckReportingApiAccess
# ---------------------------------------------------------------------------

class TestCheckReportingApiAccess(unittest.TestCase):

    def test_returns_true_when_probe_succeeds(self):
        client = _make_client()
        self.assertTrue(_check_reporting_api_access(client))
        call_kwargs = client.get.call_args.kwargs
        self.assertEqual(call_kwargs.get("path"), "jobs")
        self.assertEqual(call_kwargs.get("url"), client.reporting_url)

    def test_returns_false_on_401(self):
        client = _make_client(jobs_side_effect=YoutubeAnalyticsUnauthorizedError("401"))
        self.assertFalse(_check_reporting_api_access(client))

    def test_returns_false_on_403(self):
        client = _make_client(jobs_side_effect=YoutubeAnalyticsForbiddenError("403"))
        self.assertFalse(_check_reporting_api_access(client))

    def test_reraises_non_auth_error_fallback_false(self):
        """Reporting API uses fallback_accessible=False, so non-auth errors propagate."""
        client = _make_client(jobs_side_effect=RuntimeError("network failure"))
        with self.assertRaises(RuntimeError):
            _check_reporting_api_access(client)


# ---------------------------------------------------------------------------
# TestDiscover
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_all_accessible_returns_all_streams(self, mock_schemas, mock_data, mock_reporting):
        mock_schemas.return_value = _minimal_schemas(_DATA_STREAM, _REPORT_STREAM)
        mock_data.return_value = True
        mock_reporting.return_value = True

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertIn(_DATA_STREAM, stream_ids)
        self.assertIn(_REPORT_STREAM, stream_ids)

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_data_api_inaccessible_excludes_data_streams(self, mock_schemas, mock_data, mock_reporting):
        mock_schemas.return_value = _minimal_schemas(_DATA_STREAM, _REPORT_STREAM)
        mock_data.return_value = False
        mock_reporting.return_value = True

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertNotIn(_DATA_STREAM, stream_ids)
        self.assertIn(_REPORT_STREAM, stream_ids)

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_reporting_api_inaccessible_excludes_report_streams(self, mock_schemas, mock_data, mock_reporting):
        mock_schemas.return_value = _minimal_schemas(_DATA_STREAM, _REPORT_STREAM)
        mock_data.return_value = True
        mock_reporting.return_value = False

        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        self.assertIn(_DATA_STREAM, stream_ids)
        self.assertNotIn(_REPORT_STREAM, stream_ids)

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_both_inaccessible_raises_exception(self, mock_schemas, mock_data, mock_reporting):
        mock_schemas.return_value = _minimal_schemas(_DATA_STREAM, _REPORT_STREAM)
        mock_data.return_value = False
        mock_reporting.return_value = False

        with self.assertRaises(YoutubeAnalyticsNoAccessibleStreamsError) as ctx:
            discover(MagicMock())
        self.assertIn("No stream endpoints are accessible", str(ctx.exception))

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_api_checks_called_once_per_discover(self, mock_schemas, mock_data, mock_reporting):
        """discover() must probe each API exactly once regardless of stream count."""
        mock_schemas.return_value = _minimal_schemas(
            "channels", "videos", "playlists", _REPORT_STREAM, "channel_demographics"
        )
        mock_data.return_value = True
        mock_reporting.return_value = True

        discover(MagicMock())
        mock_data.assert_called_once()
        mock_reporting.assert_called_once()

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_catalog_entries_are_valid(self, mock_schemas, mock_data, mock_reporting):
        """Each CatalogEntry must have tap_stream_id, schema and key_properties."""
        mock_schemas.return_value = _minimal_schemas(_DATA_STREAM, _REPORT_STREAM)
        mock_data.return_value = True
        mock_reporting.return_value = True

        catalog = discover(MagicMock())
        self.assertIsInstance(catalog, Catalog)
        for entry in catalog.streams:
            self.assertIsNotNone(entry.tap_stream_id)
            self.assertIsInstance(entry.schema, Schema)
            self.assertIsNotNone(entry.key_properties)

    @patch("tap_youtube_analytics.discover._check_reporting_api_access")
    @patch("tap_youtube_analytics.discover._check_data_api_access")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_data_api_streams_set_is_correct(self, mock_schemas, mock_data, mock_reporting):
        """_DATA_STREAM must be in DATA_API_STREAMS; _REPORT_STREAM must not be."""
        self.assertIn(_DATA_STREAM, DATA_API_STREAMS)
        self.assertNotIn(_REPORT_STREAM, DATA_API_STREAMS)


if __name__ == "__main__":
    unittest.main()