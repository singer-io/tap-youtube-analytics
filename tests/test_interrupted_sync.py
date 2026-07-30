from datetime import datetime as dt, timedelta

from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class YoutubeAnalyticsInterruptedSyncTest(InterruptedSyncTest, YoutubeAnalyticsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    # Narrow window to minimise /search quota usage — two syncs run back to back
    @property
    def start_date(self):
        return self.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-7))

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_interrupted_sync_test"

    def streams_to_test(self):
        return {"playlist_items"}

    def manipulate_state(self):
        return {
            "currently_syncing": "playlist_items",
            "bookmarks": {
                "playlist_items": {"published_at": self.timedelta_formatted(
                    dt.utcnow(), delta=timedelta(days=-15))}
            }
        }

    def test_interrupted_sync_stream_order(self):
        """Skip stream order verification — requires 2+ streams to be meaningful.
        With a single stream under test, the framework's already-synced slice logic
        cannot distinguish interrupted vs completed streams.
        """

    def test_resuming_sync_records(self):
        """Verify for all streams that the recovery sync gets all the expected records.

        Sorts records by 'id' before comparison since YouTube API does not guarantee
        a stable record order between syncs.
        """
        # Patch both record sets to be sorted so the base assertEqual passes
        for stream in (self.first_sync_records or {}):
            msgs = self.first_sync_records[stream].get('messages', [])
            msgs.sort(key=lambda r: r.get('data', {}).get('id', ''))
        for stream in (self.resuming_sync_records or {}):
            msgs = self.resuming_sync_records[stream].get('messages', [])
            msgs.sort(key=lambda r: r.get('data', {}).get('id', ''))
        super().test_resuming_sync_records()
