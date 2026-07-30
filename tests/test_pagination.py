from datetime import datetime as dt, timedelta

from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.pagination_test import PaginationTest
from tap_tester.jira_client import JiraClient, CONFIGURATION_ENVIRONMENT


class YoutubeAnalyticsPaginationTest(PaginationTest, YoutubeAnalyticsBaseTest):
    """
    Test that tap-youtube-analytics properly handles paginated API responses.
    """

    @property
    def start_date(self):
        """Set start_date further back to ensure videos have accumulated over time."""
        return self.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-90))

    request_window_size = 30

    @staticmethod
    def name():
        return "tt_youtube_analytics_pagination"

    @staticmethod
    def streams_to_test():
        """Test pagination on the videos stream.
        
        Videos stream has the potential for many records and uses pagination.
        It's a good candidate for verifying pagination logic works correctly.
        """
        return {'videos'}

    @staticmethod
    def streams_to_selected_fields():
        """Select minimal fields to reduce API quota consumption during pagination testing."""
        return {
            "videos": {
                "id",
                "content_details",
                "published_at"
            }
        }

    def test_record_count_greater_than_page_limit(self):
        """Tests that the target received more records than the page limit for each stream.

        Skips if SAC-31807 is not yet done (YouTube API pagination bug is open).
        Skips if test account doesn't have enough records to test pagination.
        """
        jira = JiraClient(CONFIGURATION_ENVIRONMENT)
        status = jira.get_status_category("SAC-31807")
        if status != "done":
            self.skipTest("Skipping pagination test: SAC-31807 is not yet resolved (status: {})".format(status))

        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                page_limit = self.expected_page_size(stream)
                record_count = self.record_count_by_stream.get(stream, -1)

                if record_count <= page_limit:
                    self.skipTest(
                        f"Stream '{stream}' has {record_count} records, "
                        f"which is not greater than page limit {page_limit}. "
                        "Cannot test pagination with insufficient data."
                    )

                self.assertGreater(record_count, page_limit)
