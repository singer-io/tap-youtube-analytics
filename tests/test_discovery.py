"""Test tap discovery mode and metadata."""
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class YoutubeAnalyticsDiscoveryTest(DiscoveryTest, YoutubeAnalyticsBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""
    orphan_streams = {}

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
        return "tap_tester_youtube_analytics_discovery_test"

    @classmethod
    def expected_stream_names(cls):
        return super().expected_stream_names().union(cls.NEWLY_DISCOVERED_STREAMS)

    def streams_to_test(self):
        return self.expected_stream_names()
