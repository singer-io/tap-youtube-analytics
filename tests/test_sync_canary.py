from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest


class YoutubeAnalyticsSyncCanaryTest(SyncCanaryTest, YoutubeAnalyticsBaseTest):
    """Standard Sync Canary Test.

    Verifies that each stream syncs without critical errors and replicates at
    least one record. Analytics streams with no test data are excluded and
    covered at the discovery level only.
    """

    NEWLY_DISCOVERED_STREAMS = {
        "playlist_basic",
        "channel_demographics",
        "channel_province",
        "channel_device_os",
        "playlist_device_os",
        "channel_playback_location",
        "playlist_playback_location",
        "playlist_province",
        "playlist_combined",
        "channel_traffic_source",
        "channel_subtitles",
        "playlist_traffic_source",
        "channel_combined"
    }

    @staticmethod
    def name():
        return "tt_youtube_analytics_sync"

    @classmethod
    def expected_stream_names(cls):
        return super().expected_stream_names().union(cls.NEWLY_DISCOVERED_STREAMS)

    def streams_to_test(self):
        """Only test streams known to have records in the test account."""
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
