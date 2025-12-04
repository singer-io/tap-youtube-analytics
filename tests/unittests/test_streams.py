import unittest
from unittest.mock import MagicMock, patch

from dateutil import parser
from singer import metadata
from singer.catalog import CatalogEntry, Schema

import humps

from tap_youtube_analytics.streams.playlist_items import PlaylistItems
from tap_youtube_analytics.streams.reports import ChannelBasicStream
from tap_youtube_analytics.streams.videos import Videos

if not hasattr(humps, "decamelize"):
    humps.decamelize = lambda value: value


class DummyCounter:
    def __init__(self):
        self.value = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def increment(self):
        self.value += 1


def build_catalog_entry(stream_cls, extra_properties=None):
    base_properties = {
        "id": {"type": ["null", "string"]},
        "published_at": {"type": ["null", "string"]},
        "dimensions_hash_key": {"type": ["null", "string"]},
        "date": {"type": ["null", "string"]},
        "create_time": {"type": ["null", "string"]},
        "channel_id": {"type": ["null", "string"]},
        "video_id": {"type": ["null", "string"]},
        "live_or_on_demand": {"type": ["null", "string"]},
        "subscribed_status": {"type": ["null", "string"]},
        "country_code": {"type": ["null", "string"]},
    }

    if extra_properties:
        base_properties.update(extra_properties)

    schema_dict = {"type": "object", "properties": base_properties}
    schema = Schema.from_dict(schema_dict)

    mdata = metadata.get_standard_metadata(
        schema=schema_dict,
        key_properties=stream_cls.key_properties,
        valid_replication_keys=stream_cls.replication_keys or [],
        replication_method=getattr(stream_cls, "replication_method", None),
    )

    m_map = metadata.to_map(mdata)
    m_map = metadata.write(m_map, (), "selected", True)
    mdata = metadata.to_list(m_map)

    return CatalogEntry(
        stream=stream_cls.tap_stream_id,
        tap_stream_id=stream_cls.tap_stream_id,
        key_properties=stream_cls.key_properties,
        schema=schema,
        metadata=mdata,
    )


class TestReportStream(unittest.TestCase):
    def setUp(self):
        self.transformer = MagicMock()
        self.transformer.transform.side_effect = lambda record, *_: record

        self.client = MagicMock()
        self.client.config = {"start_date": "2023-01-01T00:00:00Z"}
        self.client.reporting_url = "https://reports.test"
        self.client.base_url = "https://data.test"

        self.catalog_entry = build_catalog_entry(ChannelBasicStream)

    def test_creates_reporting_job_and_updates_bookmark(self):
        state = {
            "bookmarks": {
                ChannelBasicStream.tap_stream_id: "2023-01-01T00:00:00Z"
            }
        }

        # Configure client responses for job pagination and report retrieval
        job_calls = iter([
            {"jobs": [{"reportTypeId": "different"}], "nextPageToken": "page2"},
            {"jobs": []},
        ])

        report_calls = iter([
            {
                "reports": [
                    {
                        "id": "r1",
                        "downloadUrl": "https://download.test/r1",
                        "startTime": "2023-01-02T00:00:00Z",
                        "endTime": "2023-01-02T23:59:59Z",
                        "createTime": "2023-01-03T00:00:00Z",
                        "reportTypeId": "channel_basic",
                        "name": "channel_basic",
                    }
                ]
            }
        ])

        def get_side_effect(url=None, params=None, endpoint=None):
            if endpoint.endswith("/jobs"):
                return next(job_calls)
            if endpoint.endswith("/reports"):
                try:
                    result = next(report_calls)
                except StopIteration:
                    return {"reports": []}
                return result
            return {}

        self.client.get.side_effect = get_side_effect
        self.client.post.return_value = {
            "id": "job123",
            "reportTypeId": "channel_basic",
            "name": "channel_basic",
        }
        def report_rows(**kwargs):
            return iter([
                {
                    "date": "2023-01-02",
                    "channel_id": "chan",
                    "video_id": "vid",
                    "live_or_on_demand": "on_demand",
                    "subscribed_status": "subscribed",
                    "country_code": "US",
                    "views": "10",
                }
            ])

        self.client.get_report.side_effect = report_rows

        write_calls = {}

        def bookmark_side_effect(state_arg, stream, key, value):
            write_calls["stream"] = stream
            write_calls["key"] = key
            write_calls["value"] = value
            state_arg.setdefault("bookmarks", {}).setdefault(stream, {})[key] = value
            return state_arg

        stream = ChannelBasicStream(self.client, self.catalog_entry)

        with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
            with patch("tap_youtube_analytics.streams.abstracts.write_record") as mock_write_record:
                with patch("tap_youtube_analytics.streams.abstracts.write_bookmark", side_effect=bookmark_side_effect) as mock_write_bookmark:
                    result = stream.sync(state=state, transformer=self.transformer)

        # Job creation should occur after pagination fails to find a match
        self.client.post.assert_called_once()

        # Records should be written once with the transformed payload
        mock_write_record.assert_called_once()
        write_args = mock_write_record.call_args[0]
        self.assertEqual(write_args[0], ChannelBasicStream.tap_stream_id)
        self.assertEqual(write_args[1]["report_id"], "r1")

        # Bookmark should be updated to the report create time
        mock_write_bookmark.assert_called_once()
        self.assertEqual(write_calls["stream"], ChannelBasicStream.tap_stream_id)
        self.assertEqual(write_calls["key"], "create_time")
        parsed_bookmark = parser.isoparse(write_calls["value"])
        expected_ts = parser.isoparse("2023-01-03T00:00:00Z")
        self.assertEqual(parsed_bookmark, expected_ts)

        self.assertEqual(result, 1)


class TestPlaylistItemsStream(unittest.TestCase):
    def setUp(self):
        self.transformer = MagicMock()
        self.transformer.transform.side_effect = lambda record, *_: record

        self.client = MagicMock()
        self.client.config = {
            "channel_ids": "channel_a",
            "start_date": "2023-01-01T00:00:00Z",
        }
        self.client.base_url = "https://data.test"

        self.catalog_entry = build_catalog_entry(PlaylistItems)

    def test_incremental_sync_filters_old_records(self):
        state = {
            "bookmarks": {
                PlaylistItems.tap_stream_id: "2023-01-01T00:00:00Z"
            }
        }

        def mocked_get_records(stream_self, isreport=False):
            if stream_self.path == "playlists":
                return iter([{"id": "playlist_1"}])
            if stream_self.path == "playlistItems":
                # The incremental request should not include unsupported filters
                assert "publishedAfter" not in stream_self.params
                return iter([
                    {
                        "id": "item_latest",
                        "snippet": {
                            "publishedAt": "2023-01-02T00:00:00Z",
                            "published_at": "2023-01-02T00:00:00Z",
                        },
                    },
                    {
                        "id": "item_old",
                        "snippet": {
                            "publishedAt": "2022-12-31T00:00:00Z",
                            "published_at": "2022-12-31T00:00:00Z",
                        },
                    },
                ])
            return iter([])

        stream = PlaylistItems(self.client, self.catalog_entry)

        with patch.object(PlaylistItems, "get_records", new=mocked_get_records):
            with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
                with patch("tap_youtube_analytics.streams.playlist_items.write_record") as mock_write_record:
                    bookmark_updates = {}

                    def bookmark_side_effect(state_arg, stream_name, key, value):
                        bookmark_updates["value"] = value
                        bookmark_updates["stream"] = stream_name
                        bookmark_updates["key"] = key
                        state_arg.setdefault("bookmarks", {}).setdefault(stream_name, {})[key] = value
                        return state_arg

                    with patch("tap_youtube_analytics.streams.abstracts.write_bookmark", side_effect=bookmark_side_effect) as mock_write_bookmark:
                        result = stream.sync(state=state, transformer=self.transformer)

        mock_write_record.assert_called_once()
        write_args = mock_write_record.call_args[0]
        self.assertEqual(write_args[0], PlaylistItems.tap_stream_id)
        self.assertEqual(write_args[1]["id"], "item_latest")

        mock_write_bookmark.assert_called_once()
        parsed = parser.isoparse(bookmark_updates["value"])
        self.assertEqual(parsed, parser.isoparse("2023-01-02T00:00:00Z"))
        self.assertEqual(result, 1)


class TestVideosStream(unittest.TestCase):
    def setUp(self):
        self.transformer = MagicMock()
        self.transformer.transform.side_effect = lambda record, *_: record

        self.client = MagicMock()
        self.client.config = {
            "channel_ids": "channel_a",
            "start_date": "2023-01-01T00:00:00Z",
        }
        self.client.base_url = "https://data.test"

        self.catalog_entry = build_catalog_entry(Videos)

    def test_sync_hydrates_recent_videos_only(self):
        state = {
            "bookmarks": {
                Videos.tap_stream_id: "2023-01-01T00:00:00Z"
            }
        }

        def mocked_get_records(stream_self, isreport=False):
            if stream_self.path == "search":
                return iter([
                    {
                        "id": {"videoId": "vid_new"},
                        "snippet": {
                            "publishedAt": "2023-01-03T00:00:00Z",
                            "published_at": "2023-01-03T00:00:00Z",
                        },
                    },
                    {
                        "id": {"videoId": "vid_recent"},
                        "snippet": {
                            "publishedAt": "2023-01-02T00:00:00Z",
                            "published_at": "2023-01-02T00:00:00Z",
                        },
                    },
                    {
                        "id": {"videoId": "vid_old"},
                        "snippet": {
                            "publishedAt": "2022-12-31T00:00:00Z",
                            "published_at": "2022-12-31T00:00:00Z",
                        },
                    },
                ])
            if stream_self.path == "videos":
                ids = stream_self.params["id"].split(",")
                records = []
                for vid in ids:
                    published = "2023-01-03T00:00:00Z" if vid == "vid_new" else "2023-01-02T00:00:00Z"
                    records.append(
                        {
                            "id": vid,
                            "snippet": {
                                "publishedAt": published,
                                "published_at": published,
                            },
                        }
                    )
                return iter(records)
            return iter([])

        stream = Videos(self.client, self.catalog_entry)

        with patch.object(Videos, "get_records", new=mocked_get_records):
            with patch("tap_youtube_analytics.streams.abstracts.metrics.record_counter", side_effect=lambda *_: DummyCounter()):
                with patch("tap_youtube_analytics.streams.videos.write_record") as mock_write_record:
                    bookmark_updates = {}

                    def bookmark_side_effect(state_arg, stream_name, key, value):
                        bookmark_updates["value"] = value
                        bookmark_updates["stream"] = stream_name
                        bookmark_updates["key"] = key
                        state_arg.setdefault("bookmarks", {}).setdefault(stream_name, {})[key] = value
                        return state_arg

                    with patch("tap_youtube_analytics.streams.abstracts.write_bookmark", side_effect=bookmark_side_effect) as mock_write_bookmark:
                        result = stream.sync(state=state, transformer=self.transformer)

        # Two new videos should be emitted
        written_ids = [record_call.args[1]["id"] for record_call in mock_write_record.call_args_list]
        self.assertEqual(sorted(written_ids), ["vid_new", "vid_recent"])

        mock_write_bookmark.assert_called_once()
        parsed = parser.isoparse(bookmark_updates["value"])
        self.assertEqual(parsed, parser.isoparse("2023-01-03T00:00:00Z"))
        self.assertEqual(result, 2)


if __name__ == "__main__":
    unittest.main()
