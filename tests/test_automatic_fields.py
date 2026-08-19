"""Test that with no fields selected for a stream automatic fields are still
replicated."""
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest


class YoutubeAnalyticsAutomaticFields(MinimumSelectionTest, YoutubeAnalyticsBaseTest):
    """Test that with no fields selected for a stream automatic fields are
    still replicated."""

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
        return "tap_tester_youtube_analytics_automatic_fields_test"

    @classmethod
    def expected_stream_names(cls):
        return super().expected_stream_names().union(cls.NEWLY_DISCOVERED_STREAMS)

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
