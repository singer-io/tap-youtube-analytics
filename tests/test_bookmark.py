from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class YoutubeAnalyticsBookmarkTest(BookmarkTest, YoutubeAnalyticsBaseTest):
    """Standard bookmark test for tap-youtube-analytics."""

    @staticmethod
    def streams_to_test():
        return {'videos', 'playlist_items'}

    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    # Set initial bookmarks to a recent date so sync 1 only fetches a small
    # window of records, avoiding rapid bursts of /search API calls from
    # scanning all videos back to 2019.
    initial_bookmarks = {
        "bookmarks": {
            "videos": {"published_at": "2025-01-01T00:00:00Z"},
            "playlist_items": {"published_at": "2025-01-01T00:00:00Z"}
        }
    }

    @staticmethod
    def name():
        return "tt_youtube_analytics_bookmark"

    def get_bookmark_value(self, state, stream):
        """Extract bookmark value — normalise to strip microseconds if present so
        both streams are consistent with bookmark_format '%Y-%m-%dT%H:%M:%SZ'.
        """
        stream_id = self.get_stream_id(stream)
        stream_bookmark = state.get('bookmarks', {}).get(stream_id)
        if stream_bookmark:
            value = next(iter(stream_bookmark.values()))
            if value and '.' in value:
                value = value.split('.')[0] + 'Z'
            return value
        return None

    @staticmethod
    def streams_to_selected_fields():
        return {
            "videos": {"id", "content_details", "published_at"},
            "playlist_items": {"id", "etag", "snippet"},
        }
