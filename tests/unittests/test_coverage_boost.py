import json
import importlib
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from singer import metadata
from singer.catalog import CatalogEntry, Schema

import tap_youtube_analytics as tap_main
from tap_youtube_analytics.client import Client, raise_for_error
from tap_youtube_analytics.discover import discover
from tap_youtube_analytics.exceptions import (
    YoutubeAnalyticsBackoffError,
    YoutubeAnalyticsError,
    YoutubeAnalyticsForbiddenError,
    YoutubeAnalyticsNotFoundError,
    YoutubeAnalyticsRateLimitError,
)
from tap_youtube_analytics.streams.abstracts import (
    BaseStream,
    FullTableStream,
    IncrementalStream,
    ReportStream,
)
from tap_youtube_analytics.streams.channels import Channels
from tap_youtube_analytics.streams.playlists import Playlists
from tap_youtube_analytics.streams.playlist_items import PlaylistItems
from tap_youtube_analytics.streams.videos import Videos


class DummyCounter:
    def __init__(self):
        self.value = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def increment(self):
        self.value += 1


def make_catalog_entry(stream_name: str, key_properties=None, properties=None):
    key_properties = key_properties or ["id"]
    schema_dict = {
        "type": "object",
        "properties": properties
        or {
            "id": {"type": ["null", "string"]},
            "updated_at": {"type": ["null", "string"]},
            "published_at": {"type": ["null", "string"]},
            "create_time": {"type": ["null", "string"]},
        },
    }
    mdata = metadata.get_standard_metadata(
        schema=schema_dict,
        key_properties=key_properties,
        valid_replication_keys=["updated_at", "published_at", "create_time"],
        replication_method="INCREMENTAL",
    )
    m_map = metadata.to_map(mdata)
    m_map = metadata.write(m_map, (), "selected", True)
    mdata = metadata.to_list(m_map)
    return CatalogEntry(
        stream=stream_name,
        tap_stream_id=stream_name,
        key_properties=key_properties,
        schema=Schema.from_dict(schema_dict),
        metadata=mdata,
    )


class DummyBase(BaseStream):
    tap_stream_id = "dummy_base"
    replication_method = "FULL_TABLE"
    replication_keys = []
    key_properties = ["id"]
    data_key = "items"
    path = "x"

    def sync(self, state, transformer, parent_obj=None):
        return 0


class DummyIncremental(IncrementalStream):
    tap_stream_id = "dummy_incremental"
    replication_keys = ["updated_at"]
    key_properties = ["id"]
    path = "x"
    data_key = "items"


class DummyFullTable(FullTableStream):
    tap_stream_id = "dummy_full"
    key_properties = ["id"]
    path = "x"
    data_key = "items"


class DummyReport(ReportStream):
    tap_stream_id = "dummy_report"
    key_properties = ["dimensions_hash_key", "date"]
    replication_keys = ["create_time"]
    path = "reports"
    data_key = "rows"
    report_type = "dummy_report_type"
    dimensions = ["date"]


class DummyNoRepl(IncrementalStream):
    tap_stream_id = "dummy_no_repl"
    replication_keys = []
    key_properties = ["id"]
    path = "x"
    data_key = "items"


class TestEntrypointCoverage(unittest.TestCase):
    def test_ensure_refresh_token_paths(self):
        cfg = {"refresh_token": "x"}
        tap_main.ensure_refresh_token(cfg)
        self.assertEqual(cfg["refresh_token"], "x")

        cfg = {"oauth_credentials": {"refresh_token": "y"}}
        tap_main.ensure_refresh_token(cfg)
        self.assertEqual(cfg["refresh_token"], "y")

        cfg = {"oauth": {"refresh_token": "z"}}
        tap_main.ensure_refresh_token(cfg)
        self.assertEqual(cfg["refresh_token"], "z")

        with self.assertRaises(ValueError):
            tap_main.ensure_refresh_token({})

    @patch("tap_youtube_analytics.discover")
    def test_do_discover_writes_json(self, mock_discover):
        catalog = MagicMock()
        catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = catalog

        with patch("tap_youtube_analytics.json.dump") as mock_dump:
            tap_main.do_discover(MagicMock())
        mock_dump.assert_called_once()

    @patch("tap_youtube_analytics.sync")
    @patch("tap_youtube_analytics.do_discover")
    @patch("tap_youtube_analytics.Client")
    @patch("singer.utils.parse_args")
    def test_main_discover_and_sync_branches(self, mock_parse_args, mock_client_cls, mock_do_discover, mock_sync):
        parsed = SimpleNamespace(
            config={
                "client_id": "id",
                "client_secret": "sec",
                "channel_ids": "c1",
                "start_date": "2023-01-01T00:00:00Z",
                "user_agent": "ua",
                "refresh_token": "rt",
            },
            state=None,
            discover=True,
            catalog=None,
        )
        mock_parse_args.return_value = parsed
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        tap_main.main()
        mock_do_discover.assert_called_once_with(mock_client)

        parsed.discover = False
        parsed.catalog = MagicMock()
        tap_main.main()
        mock_sync.assert_called_once()

    @patch("tap_youtube_analytics.sync")
    @patch("tap_youtube_analytics.do_discover")
    @patch("tap_youtube_analytics.Client")
    @patch("singer.utils.parse_args")
    def test_main_no_catalog_no_discover(self, mock_parse_args, mock_client_cls, mock_do_discover, mock_sync):
        parsed = SimpleNamespace(
            config={
                "client_id": "id",
                "client_secret": "sec",
                "channel_ids": "c1",
                "start_date": "2023-01-01T00:00:00Z",
                "user_agent": "ua",
                "refresh_token": "rt",
            },
            state={},
            discover=False,
            catalog=None,
        )
        mock_parse_args.return_value = parsed
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        tap_main.main()
        mock_do_discover.assert_not_called()
        mock_sync.assert_not_called()

    @patch("tap_youtube_analytics.sync")
    @patch("tap_youtube_analytics.Client")
    @patch("singer.utils.parse_args")
    def test_main_uses_state_when_present(self, mock_parse_args, mock_client_cls, mock_sync):
        parsed = SimpleNamespace(
            config={
                "client_id": "id",
                "client_secret": "sec",
                "channel_ids": "c1",
                "start_date": "2023-01-01T00:00:00Z",
                "user_agent": "ua",
                "refresh_token": "rt",
            },
            state={"bookmarks": {"x": {"k": "v"}}},
            discover=False,
            catalog=MagicMock(),
        )
        mock_parse_args.return_value = parsed
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__.return_value = mock_client

        tap_main.main()
        self.assertEqual(mock_sync.call_args.kwargs["state"], parsed.state)


class TestClientCoverage(unittest.TestCase):
    def setUp(self):
        self.config = {
            "client_id": "cid",
            "client_secret": "secret",
            "refresh_token": "refresh",
            "user_agent": "ua",
            "request_timeout": 1,
        }
        with patch.object(Client, "check_api_credentials"):
            self.client = Client(self.config)
        self.client._Client__access_token = "token"
        self.client._Client__expires = datetime.now(timezone.utc) + timedelta(hours=1)

    def test_raise_for_error_json_decode_branch(self):
        resp = MagicMock()
        resp.status_code = 400
        resp.json.side_effect = Exception("bad json")
        with self.assertRaises(YoutubeAnalyticsError):
            raise_for_error(resp)

    def test_enter_exit_and_helpers(self):
        with patch.object(self.client, "check_api_credentials") as mock_check:
            self.client.__enter__()
            mock_check.assert_called_once()
        with patch.object(self.client._session, "close") as mock_close:
            self.client.__exit__(None, None, None)
            mock_close.assert_called_once()

    def test_get_report_success(self):
        response = MagicMock()
        response.status_code = 200
        response.iter_lines.return_value = [b"a,b", b"1,2"]
        response.__enter__.return_value = response
        response.__exit__.return_value = None

        with patch.object(self.client._session, "request", return_value=response):
            rows = list(self.client.get_report(url="https://download.test", endpoint="rep"))
        self.assertEqual(rows, [{"a": "1", "b": "2"}])

    def test_get_report_rate_limit(self):
        response = MagicMock()
        response.status_code = 429
        response.__enter__.return_value = response
        response.__exit__.return_value = None
        identity_decorator = lambda *args, **kwargs: (lambda func: func)
        with patch("tap_youtube_analytics.client.backoff.on_exception", side_effect=identity_decorator):
            with patch.object(self.client._session, "request", return_value=response):
                with self.assertRaises(YoutubeAnalyticsRateLimitError):
                    list(self.client.get_report(url="https://download.test", endpoint="rep"))

    def test_get_report_server_error_backoff(self):
        response = MagicMock()
        response.status_code = 500
        response.__enter__.return_value = response
        response.__exit__.return_value = None
        identity_decorator = lambda *args, **kwargs: (lambda func: func)
        with patch("tap_youtube_analytics.client.backoff.on_exception", side_effect=identity_decorator):
            with patch.object(self.client._session, "request", return_value=response):
                with self.assertRaises(YoutubeAnalyticsBackoffError):
                    list(self.client.get_report(url="https://download.test"))

    def test_make_request_raw_success(self):
        response = MagicMock()
        response.status_code = 200
        response.text = "ok"
        with patch.object(self.client._session, "request", return_value=response):
            result = self.client.get_raw(url="https://x", endpoint="ep")
        self.assertEqual(result, "ok")

        with patch.object(self.client._session, "request", return_value=response):
            result2 = self.client.get_raw(url="https://x")
        self.assertEqual(result2, "ok")

    def test_make_request_paths_and_post_data(self):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        with patch.object(self.client._session, "request", return_value=response) as mock_request:
            self.client.get(path="channels", endpoint="e")
            self.client.post(url="https://base", path="jobs", data={"a": 1}, endpoint="e2")

        calls = mock_request.call_args_list
        self.assertTrue(any(c.args[1] == "https://www.googleapis.com/youtube/v3/channels" for c in calls))
        self.assertTrue(any(c.args[1] == "https://base/jobs" for c in calls))

    def test_check_api_credentials_non_200_raises(self):
        client = Client(self.config)
        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = {"message": "bad"}
        with patch.object(client, "_session") as sess:
            sess.post.return_value = resp
            with self.assertRaises(YoutubeAnalyticsError):
                client.check_api_credentials.__wrapped__(client)

    def test_check_api_credentials_500_raises_backoff(self):
        client = Client(self.config)
        resp = MagicMock()
        resp.status_code = 500
        with patch.object(client, "_session") as sess:
            sess.post.return_value = resp
            with self.assertRaises(YoutubeAnalyticsBackoffError):
                client.check_api_credentials.__wrapped__(client)

    def test_get_report_non_200_non_429_raises(self):
        response = MagicMock()
        response.status_code = 400
        response.json.return_value = {"message": "bad"}
        response.__enter__.return_value = response
        response.__exit__.return_value = None
        with patch.object(self.client._session, "request", return_value=response):
            with self.assertRaises(YoutubeAnalyticsError):
                list(self.client.get_report(url="https://download.test", endpoint="rep"))

    def test_make_request_and_raw_error_branches(self):
        resp = MagicMock()
        resp.status_code = 400
        resp.json.return_value = {"message": "bad"}
        with patch.object(self.client._session, "request", return_value=resp):
            with self.assertRaises(YoutubeAnalyticsError):
                self.client._Client__make_request.__wrapped__(self.client, "GET", path="channels", endpoint="e")
            with self.assertRaises(YoutubeAnalyticsError):
                self.client._Client__make_request_raw.__wrapped__(self.client, "GET", url="https://x", endpoint="e")

    def test_make_request_raw_500_and_429(self):
        resp_500 = MagicMock()
        resp_500.status_code = 500
        with patch.object(self.client._session, "request", return_value=resp_500):
            with self.assertRaises(YoutubeAnalyticsBackoffError):
                self.client._Client__make_request_raw.__wrapped__(self.client, "GET", url="https://x", endpoint="e")

        resp_429 = MagicMock()
        resp_429.status_code = 429
        with patch.object(self.client._session, "request", return_value=resp_429):
            with self.assertRaises(YoutubeAnalyticsRateLimitError):
                self.client._Client__make_request_raw.__wrapped__(self.client, "GET", url="https://x", endpoint="e")


class TestSchemaDiscoverExtraCoverage(unittest.TestCase):
    @patch("tap_youtube_analytics.schema.os.path.exists", return_value=True)
    @patch("tap_youtube_analytics.schema.os.path.isfile", return_value=True)
    @patch("tap_youtube_analytics.schema.os.listdir", return_value=["a.json"])
    @patch("tap_youtube_analytics.schema.get_abs_path", return_value="/tmp/shared")
    @patch("json.load", return_value={"x": 1})
    @patch("builtins.open")
    def test_load_schema_references_reads_files(self, mock_open_file, *_):
        from tap_youtube_analytics.schema import load_schema_references

        refs = load_schema_references()
        self.assertIn("shared/a.json", refs)
        mock_open_file.assert_called_once()

    @patch("tap_youtube_analytics.discover._apply_access_checks")
    @patch("tap_youtube_analytics.discover.get_schemas")
    def test_discover_exception_path_logs_and_raises(self, mock_get_schemas, _):
        mock_get_schemas.return_value = ({"bad": {"type": "object"}}, {})
        with patch("tap_youtube_analytics.discover.LOGGER") as mock_logger:
            with self.assertRaises(Exception):
                discover(MagicMock())
            self.assertTrue(mock_logger.error.called)

    def test_list_available_reporting_job_types_uses_page_token(self):
        discover_module = importlib.import_module("tap_youtube_analytics.discover")

        client = MagicMock()
        client.reporting_url = "https://reporting.test"
        client.get.side_effect = [
            {
                "jobs": [{"reportTypeId": "rt_a"}],
                "nextPageToken": "token-2",
            },
            {
                "jobs": [{"reportTypeId": "rt_b"}],
            },
        ]

        report_types = discover_module._list_available_reporting_job_types(client)

        self.assertEqual(report_types, {"rt_a", "rt_b"})
        second_call_params = client.get.call_args_list[1].kwargs["params"]
        self.assertEqual(second_call_params["pageToken"], "token-2")

    def test_check_reporting_stream_access_short_circuits_when_job_type_exists(self):
        discover_module = importlib.import_module("tap_youtube_analytics.discover")

        client = MagicMock()
        stream_obj = SimpleNamespace(report_type="rt_existing")

        with patch.dict("tap_youtube_analytics.discover.STREAMS", {"existing_stream": stream_obj}, clear=True):
            is_accessible = discover_module._check_reporting_stream_access(
                client,
                "existing_stream",
                {"rt_existing"},
            )

        self.assertTrue(is_accessible)
        client.post.assert_not_called()

    def test_check_reporting_stream_access_short_circuits_without_report_type(self):
        discover_module = importlib.import_module("tap_youtube_analytics.discover")

        client = MagicMock()
        stream_obj = SimpleNamespace()

        with patch.dict("tap_youtube_analytics.discover.STREAMS", {"no_type_stream": stream_obj}, clear=True):
            is_accessible = discover_module._check_reporting_stream_access(
                client,
                "no_type_stream",
                {"unused"},
            )

        self.assertTrue(is_accessible)
        client.post.assert_not_called()

    def test_get_schemas_marks_replication_keys_automatic(self):
        from tap_youtube_analytics import schema as schema_module

        stream_obj = SimpleNamespace(
            key_properties=["id"],
            replication_keys=["updated_at"],
            replication_method="INCREMENTAL",
            parent_stream_id=None,
        )
        raw_schema = {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "updated_at": {"type": ["null", "string"]},
            },
        }

        with patch.dict("tap_youtube_analytics.schema.STREAMS", {"test_stream": stream_obj}, clear=True):
            with patch("tap_youtube_analytics.schema.load_schema_references", return_value={}):
                with patch("tap_youtube_analytics.schema._load_schema_for_stream", return_value=raw_schema):
                    with patch("tap_youtube_analytics.schema.singer.resolve_schema_references", return_value=raw_schema):
                        _, field_metadata = schema_module.get_schemas()

        metadata_map = metadata.to_map(field_metadata["test_stream"])
        self.assertEqual(
            metadata_map[("properties", "updated_at")]["inclusion"],
            "automatic",
        )


class TestStreamsExtraCoverage(unittest.TestCase):
    def _make_client(self):
        client = MagicMock()
        client.base_url = "https://data.test"
        client.reporting_url = "https://reporting.test"
        client.config = {
            "start_date": "2023-01-01T00:00:00Z",
            "channel_ids": "c1, c2",
            "user_agent": "ua",
        }
        return client

    def test_base_get_records_nonreport_and_report(self):
        client = self._make_client()
        entry = make_catalog_entry("dummy_base")
        stream = DummyBase(client, entry)

        responses = [
            {"pageInfo": {"totalResults": 2}, "items": [None, {"id": 1}], "nextPageToken": "n1"},
            {"pageInfo": {"totalResults": 2}, "items": [{"id": 2}]},
        ]
        client.get.side_effect = responses
        rows = list(stream.get_records(isreport=False))
        self.assertEqual(rows, [{"id": 1}, {"id": 2}])

        client.get.side_effect = [
            {
                "rows": [["2023-01-01", "x"], ["bad"]],
                "columnHeaders": [{"name": "date"}, {"name": "val"}],
            }
        ]
        stream.data_key = "rows"
        report_rows = list(stream.get_records(isreport=True))
        self.assertEqual(report_rows, [{"date": "2023-01-01", "val": "x"}])

    def test_base_get_records_empty_page_limit_and_dim_warning(self):
        client = self._make_client()
        stream = DummyBase(client, make_catalog_entry("dummy_base"))
        client.get.side_effect = [
            {"pageInfo": {"totalResults": 0}, "items": [], "nextPageToken": "n1"},
            {"pageInfo": {"totalResults": 0}, "items": [], "nextPageToken": "n2"},
            {"pageInfo": {"totalResults": 0}, "items": [], "nextPageToken": "n3"},
        ]
        rows = list(stream.get_records())
        self.assertEqual(rows, [])

        BaseStream._dim_lookup_map = {"country": {"US": "United States"}}
        with patch("tap_youtube_analytics.streams.abstracts.LOGGER") as mock_logger:
            transformed = stream.transform_report_record(
                {"country": "ZZ", "date": "2023-01-01"},
                ["country"],
                {"id": "r", "reportTypeId": "rt", "name": "nm", "createTime": "2023-01-02T00:00:00Z"},
            )
        self.assertEqual(transformed["country"], "ZZ")
        self.assertTrue(mock_logger.warning.called)

    def test_base_get_records_empty_response_break(self):
        client = self._make_client()
        stream = DummyBase(client, make_catalog_entry("dummy_base"))
        client.get.return_value = {}
        self.assertEqual(list(stream.get_records()), [])

    def test_base_helpers_and_write_schema_error(self):
        client = self._make_client()
        entry = make_catalog_entry("dummy_base")
        stream = DummyBase(client, entry)

        self.assertEqual(stream.modify_object({"a": 1}), {"a": 1})
        self.assertEqual(stream.get_url_endpoint(), "https://data.test/x")

        rec = {"published_at": "2023-01-01"}
        stream.append_times_to_dates(rec)
        self.assertEqual(rec["published_at"], "2023-01-01T00:00:00Z")

        with patch("tap_youtube_analytics.streams.abstracts.write_schema", side_effect=OSError("x")):
            with self.assertRaises(OSError):
                stream.write_schema()

        BaseStream._dim_lookup_map = None
        with patch("tap_youtube_analytics.streams.abstracts.os.path.exists", return_value=True):
            with patch("builtins.open", unittest.mock.mock_open(read_data='{"a": {"b": "c"}}')):
                loaded = stream._load_dim_lookup_map()
                self.assertIn("a", loaded)

    def test_append_dates_and_bookmark_migration_branches(self):
        client = self._make_client()
        stream = DummyIncremental(client, make_catalog_entry("dummy_incremental"))

        class BadLenStr(str):
            def __len__(self):
                raise TypeError("bad-len")

            def __bool__(self):
                return True

        bad = {"published_at": BadLenStr("2023-01-01")}
        stream.append_times_to_dates(bad)

        with_time = {"updated_at": "2023-01-01T12:00:00"}
        stream.append_times_to_dates(with_time)
        self.assertEqual(with_time["updated_at"], "2023-01-01T12:00:00Z")

        no_repl = DummyNoRepl(client, make_catalog_entry("dummy_no_repl"))
        state = {"x": 1}
        self.assertEqual(no_repl.write_bookmark(state, "dummy_no_repl", key=None, value="2023"), state)

        state2 = {"bookmarks": {"dummy_incremental": None}}
        stream._migrate_legacy_bookmark(state2, "dummy_incremental", "updated_at")
        self.assertEqual(state2["bookmarks"]["dummy_incremental"], {})

        state3 = {"bookmarks": "not-a-dict"}
        stream._migrate_legacy_bookmark(state3, "dummy_incremental", "updated_at")
        self.assertEqual(state3["bookmarks"], "not-a-dict")

        state4 = {"bookmarks": {}}
        stream._migrate_legacy_bookmark(state4, "dummy_incremental", "updated_at")
        self.assertEqual(state4["bookmarks"], {})

        stream._migrate_legacy_bookmark("not-a-dict", "dummy_incremental", "updated_at")

        class WeirdRecord(dict):
            def __init__(self):
                super().__init__({"published_at": "seed"})
                self.calls = 0

            def __getitem__(self, key):
                self.calls += 1
                if self.calls == 1:
                    return "seed"
                class ExplodingLen(str):
                    def __len__(self):
                        raise TypeError("len-fail")
                    def __bool__(self):
                        return True
                return ExplodingLen("2023-01-01")

        stream.append_times_to_dates(WeirdRecord())

    def test_incremental_and_fulltable_sync(self):
        client = self._make_client()
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, *_: rec

        inc = DummyIncremental(client, make_catalog_entry("dummy_incremental"))
        inc.get_records = MagicMock(return_value=iter([
            {"id": "1", "updated_at": "2023-01-02T00:00:00Z"},
            {"id": "2", "updated_at": "2022-12-30T00:00:00Z"},
        ]))
        child = MagicMock()
        inc.child_to_sync = [child]
        state = {"bookmarks": {"dummy_incremental": {"updated_at": "2023-01-01T00:00:00Z"}}}

        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with patch("tap_youtube_analytics.streams.abstracts.write_record") as mock_write:
                count = inc.sync(state=state, transformer=transformer)

        self.assertEqual(count, 1)
        mock_write.assert_called_once()
        child.sync.assert_called_once()

        ft = DummyFullTable(client, make_catalog_entry("dummy_full"))
        ft.get_records = MagicMock(return_value=iter([{"id": "x"}]))
        ft_child = MagicMock()
        ft.child_to_sync = [ft_child]
        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with patch("tap_youtube_analytics.streams.abstracts.write_record") as mock_write:
                count = ft.sync(state={}, transformer=transformer)
        self.assertEqual(count, 1)
        self.assertEqual(mock_write.call_count, 1)
        ft_child.sync.assert_called_once()

    def test_report_stream_get_records_and_sync_branches(self):
        client = self._make_client()
        stream = DummyReport(client, make_catalog_entry("dummy_report", key_properties=["dimensions_hash_key", "date"]))
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, *_: rec

        # get_records: job create path + report parsing
        client.get.side_effect = [
            {"jobs": []},
            {"reports": [{"id": "r1", "downloadUrl": "https://d", "createTime": "2023-01-03T00:00:00Z"}]},
        ]
        client.post.return_value = {"id": "job1", "reportTypeId": stream.report_type}
        client.get_report.return_value = iter([{"date": "2023-01-02", "metric": "1"}])

        records = list(stream.get_records(isreport=True))
        self.assertEqual(len(records), 1)

        # sync: include None item, invalid timestamp, older timestamp, and one valid
        now = datetime.now(timezone.utc)
        state = {"bookmarks": {"dummy_report": {"create_time": (now + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}}}

        def fake_items(*args, **kwargs):
            return iter([
                None,
                ({"date": "2023-01-01"}, {"id": "r2", "reportTypeId": "x", "name": "n", "createTime": "not-a-date"}),
                ({"date": "2023-01-01"}, {"id": "r3", "reportTypeId": "x", "name": "n", "createTime": "2000-01-01T00:00:00Z"}),
                ({"date": "2023-01-01"}, {"id": "r4", "reportTypeId": "x", "name": "n", "createTime": "2100-01-01T00:00:00Z"}),
            ])

        stream.get_records = fake_items
        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with patch("tap_youtube_analytics.streams.abstracts.write_record") as mock_write:
                count = stream.sync(state=state, transformer=transformer)
        self.assertEqual(count, 2)
        self.assertEqual(mock_write.call_count, 2)

        # forbidden path in sync
        stream.get_records = MagicMock(side_effect=YoutubeAnalyticsForbiddenError("403"))
        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with self.assertRaises(YoutubeAnalyticsForbiddenError):
                stream.sync(state=state, transformer=transformer)

    def test_report_stream_extra_branches(self):
        client = self._make_client()
        stream = DummyReport(client, make_catalog_entry("dummy_report", key_properties=["dimensions_hash_key", "date"]))

        self.assertEqual(stream._normalize_datetime("2023-01-01"), "2023-01-01T00:00:00Z")
        self.assertEqual(stream._normalize_datetime("2023-01-01T01:00:00"), "2023-01-01T01:00:00Z")
        self.assertIsNone(stream._normalize_datetime(None))
        stream.update_params(updated_since="2023-01-01", end_time="2023-01-10")
        self.assertIn("startTimeBefore", stream.params)

        client.get.return_value = {
            "pageInfo": {"totalResults": 1},
            "rows": [["x"]],
        }
        self.assertEqual(list(stream.get_records(isreport=False)), [])

        client.post.return_value = {}
        client.get.side_effect = [None]
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [{"jobs": []}]
        client.post.return_value = {}
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [{"jobs": [{"reportTypeId": stream.report_type}]}]
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [
            {"jobs": []},
            {"jobs": [{"reportTypeId": stream.report_type, "id": "job2"}]},
            None,
        ]
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [
            {"jobs": [{"reportTypeId": stream.report_type, "id": "job3"}]},
            {"reports": [{"id": "r1"}]},
        ]
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [
            {"jobs": [{"reportTypeId": stream.report_type, "id": "job4"}]},
            {"reports": [{"id": "r2", "downloadUrl": "https://download"}]},
        ]
        client.get_report.return_value = iter([])
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = [
            {"jobs": [{"reportTypeId": stream.report_type, "id": "job5"}]},
            {"reports": [{"id": "r3", "downloadUrl": "https://download"}]},
        ]
        client.get_report.side_effect = Exception("csv-fail")
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.get.side_effect = YoutubeAnalyticsForbiddenError("403")
        with self.assertRaises(YoutubeAnalyticsForbiddenError):
            list(stream.get_records(isreport=True))

        client.get.side_effect = YoutubeAnalyticsError("err")
        with self.assertRaises(YoutubeAnalyticsError):
            list(stream.get_records(isreport=True))

        client.get.side_effect = Exception("boom")
        with self.assertRaises(Exception):
            list(stream.get_records(isreport=True))

        client.get.side_effect = [
            {"jobs": []},
        ]
        client.post.side_effect = YoutubeAnalyticsNotFoundError("404")
        self.assertEqual(list(stream.get_records(isreport=True)), [])

        client.post.side_effect = None
        client.get.side_effect = [
            {"jobs": [{"reportTypeId": stream.report_type, "id": "job6"}]},
            {"reports": [], "nextPageToken": "p2"},
            None,
        ]
        self.assertEqual(list(stream.get_records(isreport=True)), [])

    def test_report_sync_bookmark_parse_fallback(self):
        client = self._make_client()
        stream = DummyReport(client, make_catalog_entry("dummy_report", key_properties=["dimensions_hash_key", "date"]))
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, *_: rec

        state = {"bookmarks": {"dummy_report": {"create_time": "not-a-datetime"}}}
        stream.get_records = lambda **kwargs: iter([])

        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            count = stream.sync(state=state, transformer=transformer)
        self.assertEqual(count, 0)

        child = MagicMock()
        stream.child_to_sync = [child]
        stream.get_records = lambda **kwargs: iter([
            {"date": "2100-01-01"},
        ])
        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            count = stream.sync(state={"bookmarks": {"dummy_report": {"create_time": "2023-01-01T00:00:00Z"}}}, transformer=transformer)
        self.assertEqual(count, 1)
        child.sync.assert_called_once()

    def test_videos_and_playlist_items_remaining_branches(self):
        client = self._make_client()
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, *_: rec

        videos = Videos(client, make_catalog_entry("videos"))
        videos.get_records = MagicMock(return_value=iter([
            {"id": {}, "snippet": {"publishedAt": "2023-01-01T00:00:00Z"}},
            {"id": {"videoId": "v1"}, "snippet": {}},
            {"id": {"videoId": "v2"}, "snippet": {"publishedAt": "bad-datetime"}},
            {"id": {"videoId": "v3"}, "snippet": {"publishedAt": "1900-01-01T00:00:00Z"}},
        ]))
        with patch.object(videos, "_fetch_and_emit_videos", return_value=(0, "2023-01-01T00:00:00Z")):
            with patch("tap_youtube_analytics.streams.videos.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
                videos.sync(state={"bookmarks": {"videos": {"published_at": "2023-01-01T00:00:00Z"}}}, transformer=transformer)

        videos.get_records = MagicMock(return_value=iter([
            {"snippet": {"publishedAt": "2023-01-02T00:00:00Z"}},
        ]))
        with self.assertRaises(ValueError):
            videos._fetch_and_emit_videos(
                video_ids=["v1"],
                last_dttm=datetime(2023, 1, 1, tzinfo=timezone.utc),
                current_max_bookmark_date="2023-01-01T00:00:00Z",
                transformer=transformer,
                counter=DummyCounter(),
                total_written=0,
                state={},
            )

        videos.get_records = MagicMock(return_value=iter([
            {"id": "v1", "snippet": {"publishedAt": "2000-01-01T00:00:00Z"}},
            {"id": "v2", "snippet": {"publishedAt": "2100-01-01T00:00:00Z"}},
        ]))
        total, bookmark = videos._fetch_and_emit_videos(
            video_ids=["v1", "v2"],
            last_dttm=datetime(2023, 1, 1, tzinfo=timezone.utc),
            current_max_bookmark_date="2023-01-01T00:00:00Z",
            transformer=transformer,
            counter=DummyCounter(),
            total_written=0,
            state={},
        )
        self.assertEqual(total, 0)
        self.assertEqual(bookmark, "2023-01-01T00:00:00Z")

        child = MagicMock()
        videos.child_to_sync = [child]
        videos.get_records = MagicMock(return_value=iter([
            {"id": "v3", "snippet": {"publishedAt": "2100-01-01T00:00:00Z"}},
        ]))
        total, _ = videos._fetch_and_emit_videos(
            video_ids=["v3"],
            last_dttm=datetime(2023, 1, 1, tzinfo=timezone.utc),
            current_max_bookmark_date="2023-01-01T00:00:00Z",
            transformer=transformer,
            counter=DummyCounter(),
            total_written=0,
            state={},
        )
        self.assertEqual(total, 1)
        child.sync.assert_called_once()

        playlist_items = PlaylistItems(client, make_catalog_entry("playlist_items"))
        playlist_items.client.config["channel_ids"] = "c1"
        playlist_items.get_records = MagicMock(side_effect=[iter([{"id": "pl1"}]), iter([
            {"id": "it1", "snippet": {"publishedAt": "2000-01-01T00:00:00Z"}},
        ])])
        with patch("tap_youtube_analytics.streams.playlist_items.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            count = playlist_items.sync(
                state={"bookmarks": {"playlist_items": {"published_at": "2023-01-01T00:00:00Z"}}},
                transformer=transformer,
            )
        self.assertEqual(count, 0)

        playlist_items.client.config["channel_ids"] = "c1"
        child = MagicMock()
        playlist_items.child_to_sync = [child]
        playlist_items.get_records = MagicMock(side_effect=[
            iter([{"id": "pl2"}]),
            iter([{"id": "it2", "snippet": {"publishedAt": "2100-01-01T00:00:00Z"}}]),
        ])
        with patch("tap_youtube_analytics.streams.playlist_items.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            count = playlist_items.sync(
                state={"bookmarks": {"playlist_items": {"published_at": "2023-01-01T00:00:00Z"}}},
                transformer=transformer,
            )
        self.assertEqual(count, 1)
        child.sync.assert_called_once()

        playlist_items.get_records = MagicMock(side_effect=[iter([{"id": "pl1"}]), iter([{"snippet": {"publishedAt": "2023-01-02T00:00:00Z"}}])])
        with patch("tap_youtube_analytics.streams.playlist_items.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with self.assertRaises(ValueError):
                playlist_items.sync(
                    state={"bookmarks": {"playlist_items": {"published_at": "2023-01-01T00:00:00Z"}}},
                    transformer=transformer,
                )

    def test_channels_playlists_and_dim_map_branches(self):
        client = self._make_client()

        ch = Channels(client, make_catalog_entry("channels"))
        params = ch.update_params()
        self.assertIn("id", params)

        pl = Playlists(client, make_catalog_entry("playlists"))
        pl.get_records = MagicMock(return_value=iter([None, {"id": "p1", "snippet": {"publishedAt": "2023-01-01T00:00:00Z"}}]))
        transformer = MagicMock()
        transformer.transform.side_effect = lambda rec, *_: rec
        with patch("tap_youtube_analytics.streams.playlists.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with patch("tap_youtube_analytics.streams.playlists.write_record") as mock_write:
                count = pl.sync(state={}, transformer=transformer)
        self.assertEqual(count, 1)
        self.assertEqual(mock_write.call_count, 1)

        base = DummyBase(client, make_catalog_entry("dummy_base"))
        BaseStream._dim_lookup_map = None
        with patch("tap_youtube_analytics.streams.abstracts.os.path.exists", return_value=False):
            m = base._load_dim_lookup_map()
            self.assertEqual(m, {})

        BaseStream._dim_lookup_map = None
        with patch("tap_youtube_analytics.streams.abstracts.os.path.exists", return_value=True):
            with patch("builtins.open", side_effect=OSError("boom")):
                m = base._load_dim_lookup_map()
                self.assertEqual(m, {})

        pl_with_child = Playlists(client, make_catalog_entry("playlists"))
        pl_with_child.get_records = MagicMock(return_value=iter([{"id": "p2"}]))
        child = MagicMock()
        pl_with_child.child_to_sync = [child]
        with patch("tap_youtube_analytics.streams.playlists.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            pl_with_child.sync(state={}, transformer=transformer)
        child.sync.assert_called_once()


class TestSyncCoverage(unittest.TestCase):
    def test_write_schema_and_sync_missing_streams(self):
        sync_mod = importlib.import_module("tap_youtube_analytics.sync")

        stream = MagicMock()
        stream.is_selected.return_value = True
        stream.children = ["missing_child"]
        stream.child_to_sync = []
        stream.write_schema = MagicMock()

        with patch.object(sync_mod, "LOGGER") as mock_logger:
            with patch("tap_youtube_analytics.sync.streams.STREAMS", {}):
                sync_mod.write_schema(stream, MagicMock(), [], MagicMock())
        self.assertTrue(mock_logger.warning.called)

        mock_catalog = MagicMock()
        s1 = MagicMock()
        s1.stream = "unknown_stream"
        mock_catalog.get_selected_streams.return_value = [s1]

        with patch.object(sync_mod, "LOGGER") as mock_logger:
            with patch("tap_youtube_analytics.sync.streams.STREAMS", {}):
                with patch("singer.Transformer") as transformer:
                    transformer.return_value.__enter__.return_value = MagicMock()
                    sync_mod.sync(MagicMock(), {}, mock_catalog, {})
        self.assertTrue(mock_logger.warning.called)


if __name__ == "__main__":
    unittest.main()
