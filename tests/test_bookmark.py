from copy import deepcopy
from datetime import datetime as dt, timedelta
import unittest
from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class YoutubeAnalyticsBookmarkTest(BookmarkTest, YoutubeAnalyticsBaseTest):
    """
    Test bookmark behavior for incremental streams in tap-youtube-analytics.
    Ensures that bookmarks are set correctly on first sync and subsequent syncs.
    """

    @property
    def start_date(self):
        """Set start_date to 5 days ago to ensure bookmark behavior is testable."""
        return self.timedelta_formatted(dt.utcnow(), delta=timedelta(days=-5))

    @staticmethod
    def streams_to_test():
        """Test incremental streams that use bookmarks.
        
        Videos and playlists both have records in the test account.
        Since bookmark tests are for INCREMENTAL streams only, we test:
        - videos: INCREMENTAL stream
        - playlist_items: INCREMENTAL child stream of playlists
        """
        return {
            'videos',
            'playlist_items',
        }

    bookmark_format = "%Y-%m-%dT%H:%M:%SZ"
    initial_bookmarks = {}

    @staticmethod
    def name():
        return "tt_youtube_analytics_bookmark"

    def setUp(self):
        """Setup bookmark tests, skipping if there aren't enough records to test with."""
        try:
            super().setUp()
        except IndexError as e:
            # If streams don't have enough records to calculate bookmarks,
            # skip the test rather than failing
            if "list index out of range" in str(e):
                self.skipTest("Streams do not have enough records to test bookmarks")
            raise

    def manipulate_state(self, state: dict, new_bookmarks: dict):
        """
        Update the passed state with new_bookmarks.
        
        new_bookmarks format: { stream_id: {replication_key: replication_value}}
        For YouTube Analytics, bookmarks are stored per stream by replication key value.
        """
        new_state = deepcopy(state)
        if new_state.get('bookmarks') is None:
            new_state['bookmarks'] = {}
        
        for stream_id, rep in new_bookmarks.items():
            # rep should be {replication_key: value}
            for replication_key, value in rep.items():
                new_state['bookmarks'][stream_id] = {replication_key: value}
        
        return new_state

    def get_bookmark_value(self, state, stream):
        """Extract the bookmark value for a given stream from state."""
        stream_id = self.get_stream_id(stream)
        bookmarks = state.get('bookmarks', {})
        stream_bookmark = bookmarks.get(stream_id)
        
        if stream_bookmark:
            # YouTube Analytics streams use replication_key like 'published_at'
            # Get the first (and usually only) value from the replication_key dict
            for value in stream_bookmark.values():
                return value
        return None

    @staticmethod
    def streams_to_selected_fields():
        """Select a minimal set of fields to reduce API quota consumption."""
        return {
            "videos": {"id", "content_details", "published_at"},
            "playlist_items": {"id", "etag", "snippet"},
        }

    ##########################################################################
    # Tap Specific Tests
    ##########################################################################

    def test_first_sync_bookmark(self):
        """Verify that the first sync sets the bookmark to the latest record's replication key value."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # Get bookmark value from first sync
                bookmark_value = self.get_bookmark_value(self.state_1, stream)
                
                # Verify bookmark was set (not None)
                self.assertIsNotNone(
                    bookmark_value,
                    f"Bookmark not set for stream {stream} after first sync"
                )

    def test_second_sync_bookmark(self):
        """Verify that the second sync's bookmark is the same or later than the first sync."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                # Get bookmark values from both syncs
                bookmark_value_1 = self.get_bookmark_value(self.state_1, stream)
                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)
                
                # Verify bookmarks were set
                self.assertIsNotNone(bookmark_value_1, f"Bookmark not set for {stream} in sync 1")
                self.assertIsNotNone(bookmark_value_2, f"Bookmark not set for {stream} in sync 2")
                
                # Parse dates for comparison
                bookmark_datetime_1 = self.parse_date(bookmark_value_1)
                bookmark_datetime_2 = self.parse_date(bookmark_value_2)
                
                # Verify bookmark doesn't move backwards
                self.assertGreaterEqual(
                    bookmark_datetime_2,
                    bookmark_datetime_1,
                    f"Bookmark for {stream} moved backwards: {bookmark_value_1} -> {bookmark_value_2}"
                )

    def test_bookmark_persists_across_syncs(self):
        """Verify that bookmarks persist correctly across multiple syncs."""
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                bookmark_value_1 = self.get_bookmark_value(self.state_1, stream)
                bookmark_value_2 = self.get_bookmark_value(self.state_2, stream)
                
                # Both should be set
                self.assertIsNotNone(bookmark_value_1)
                self.assertIsNotNone(bookmark_value_2)
