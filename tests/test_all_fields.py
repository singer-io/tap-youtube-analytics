from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class YoutubeAnalyticsAllFields(AllFieldsTest, YoutubeAnalyticsBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""
    # We don't get these fields in API response
    MISSING_FIELDS = {
        "playlists": [
            'snippet',
            'status',
            'content_details',
            'player'
        ],
        "videos": [
            'status',
            'content_details',
            'statistics',
            'player'
        ]
    }

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_all_fields_test"

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
