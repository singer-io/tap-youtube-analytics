
from base import youtube-analyticsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class youtube-analyticsInterruptedSyncTest(youtube-analyticsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    @staticmethod
    def name():
        return "tap_tester_youtube-analytics_interrupted_sync_test"

    def streams_to_test(self):
        return self.expected_stream_names()


    def manipulate_state(self):
        return {
            "currently_syncing": "prospects",
            "bookmarks": {
                "playlist_items": { "published_at" : "2020-01-01T00:00:00Z"},
                "videos": { "published_at" : "2020-01-01T00:00:00Z"},
        }
    }