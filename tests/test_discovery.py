"""Test tap discovery mode and metadata."""
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.discovery_test import DiscoveryTest


class YoutubeAnalyticsDiscoveryTest(DiscoveryTest, YoutubeAnalyticsBaseTest):
    """Test tap discovery mode and metadata conforms to standards."""
    orphan_streams = {}

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
