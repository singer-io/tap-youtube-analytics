from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class YoutubeAnalyticsInterruptedSyncTest(InterruptedSyncTest, YoutubeAnalyticsBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""

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
        return "tap_tester_youtube_analytics_interrupted_sync_test"

    @classmethod
    def expected_stream_names(cls):
        return super().expected_stream_names().union(cls.NEWLY_DISCOVERED_STREAMS)

    def streams_to_test(self):
        # Exclude base streams plus additional streams that were added in recent commits
        streams_to_exclude = self.get_streams_to_exclude().union({
            "channels", 
            "playlists", 
            "videos",
            # Newly added streams to exclude from interrupted sync test
            *self.NEWLY_DISCOVERED_STREAMS
        })
        return self.expected_stream_names().difference(streams_to_exclude)

    def manipulate_state(self):
        return {
            "currently_syncing": "playlist_items",
            "bookmarks": {
                "playlist_items": {"published_at": "2025-04-22T00:00:00Z"}
            }
        }

    def test_interrupted_sync_stream_order(self):
        """
        Verify that the sync starts with the interrupted stream,
        then not yet synced, then completed.

        This tap can have no "already synced" streams in manipulated state;
        in that case skip the trailing-order assertion.
        """

        expected_interrupted_sync = self.manipulate_state()['currently_syncing']
        expected_yet_to_be_synced = self.streams_to_test().difference(
            self.manipulate_state()['bookmarks'].keys())
        expected_already_synced = set(self.manipulate_state()['bookmarks'].keys()).difference(
            {expected_interrupted_sync})

        self.assertEqual(self.resuming_sync_order[0], expected_interrupted_sync)

        actual_next_synced = set(self.resuming_sync_order[1:1 + len(expected_yet_to_be_synced)])
        self.assertSetEqual(actual_next_synced, expected_yet_to_be_synced)

        if expected_already_synced:
            actual_last_synced = set(self.resuming_sync_order[-len(expected_already_synced):])
            self.assertSetEqual(actual_last_synced, expected_already_synced)

