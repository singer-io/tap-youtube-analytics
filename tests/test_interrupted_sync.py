from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class YoutubeAnalyticsInterruptedSyncTest(InterruptedSyncTest, YoutubeAnalyticsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude().union({"channels", "playlists"})
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "playlist_items",
            "bookmarks": {
                "playlist_items": {"published_at": "2025-04-22T00:00:00Z"},
                "videos": {"published_at": "2025-04-22T06:36:22Z"}
            }
        }

    def test_resuming_sync_records(self):
        """Verify for all streams that the recovery sync gets all the expected records.

        Sorts records by 'id' before comparison since YouTube API does not guarantee
        a stable record order between syncs.
        """
        from tap_tester import runner as _runner
        # Patch both record sets to be sorted so the base assertEqual passes
        for stream in (self.first_sync_records or {}):
            msgs = self.first_sync_records[stream].get('messages', [])
            msgs.sort(key=lambda r: r.get('data', {}).get('id', ''))
        for stream in (self.resuming_sync_records or {}):
            msgs = self.resuming_sync_records[stream].get('messages', [])
            msgs.sort(key=lambda r: r.get('data', {}).get('id', ''))
        super().test_resuming_sync_records()
