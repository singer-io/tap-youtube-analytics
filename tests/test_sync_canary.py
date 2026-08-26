from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest


class YoutubeAnalyticsSyncCanaryTest(SyncCanaryTest, YoutubeAnalyticsBaseTest):
    """Standard Sync Canary Test.

    Verifies that each stream syncs without critical errors and replicates at
    least one record. Analytics streams with no test data are excluded and
    covered at the discovery level only.
    """

    @staticmethod
    def name():
        return "tt_youtube_analytics_sync"

    def streams_to_test(self):
        """Only test streams known to have records in the test account."""
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
