from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class YoutubeAnalyticsInterruptedSyncTest(InterruptedSyncTest, YoutubeAnalyticsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

    max_diff = None

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude().union({"channels", "playlists"})
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "videos",
            "bookmarks": {
                "playlist_items": {"published_at": "2020-04-20T00:00:00Z"},
                "videos": {"published_at": "2020-04-20T00:00:00Z"}
            }
        }
