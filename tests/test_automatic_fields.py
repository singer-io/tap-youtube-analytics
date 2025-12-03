"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class YoutubeAnalyticsAutomaticFields(MinimumSelectionTest, YoutubeAnalyticsBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_automatic_fields_test"

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
