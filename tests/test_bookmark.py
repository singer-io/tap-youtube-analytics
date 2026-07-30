from datetime import datetime as dt, timedelta
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class YoutubeAnalyticsBookmarkTest(BookmarkTest, YoutubeAnalyticsBaseTest):
    """Standard bookmark test for tap-youtube-analytics."""

    @property
    def start_date(self):
        return self.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-5))

    @staticmethod
    def streams_to_test():
        return {'videos', 'playlist_items'}

    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    initial_bookmarks = {}

    @staticmethod
    def name():
        return "tt_youtube_analytics_bookmark"

    def get_bookmark_value(self, state, stream):
        """Extract bookmark value — YouTube Analytics stores bookmarks as {replication_key: value}."""
        stream_id = self.get_stream_id(stream)
        stream_bookmark = state.get('bookmarks', {}).get(stream_id)
        if stream_bookmark:
            return next(iter(stream_bookmark.values()))
        return None

    @staticmethod
    def streams_to_selected_fields():
        return {
            "videos": {"id", "content_details", "published_at"},
            "playlist_items": {"id", "etag", "snippet"},
        }



