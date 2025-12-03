import os

from tap_tester.base_suite_tests.base_case import BaseCase


class YoutubeAnalyticsBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-youtube-analytics"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.youtube-analytics"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "channel_basic": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_province": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_playback_location": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_traffic_source": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_device_os": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_demographics": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_sharing_service": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_annotations": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_cards": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_end_screens": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_subtitles": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channel_combined": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_basic": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_province": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_playback_location": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_device_os": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_traffic_source": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_combined": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_basic": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_province": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playback_location": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_traffic_source": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_device_os": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_demographics": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_sharing_service": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_annotations": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_cards": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_end_screens": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_subtitles": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_combined": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_basic": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_province": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_playback_location": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_traffic_source": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_device_os": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_playlist_combined": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_ad_rates": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_estimated_revenue": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_estimated_revenue": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_basic": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_province": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_playback_location": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_traffic_source": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_device_os": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_demographics": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_sharing_service": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_annotations": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_cards": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_end_screens": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "content_owner_asset_combined": {
                cls.PRIMARY_KEYS: {"dimensions_hash_key", "date"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"create_time"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "channels": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlist_items": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"published_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "playlists": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "videos": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"published_at"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "client_id": "TAP_YOUTUBE_ANALYTICS_CLIENT_ID",
            "client_secret": "TAP_YOUTUBE_ANALYTICS_CLIENT_SECRET",
            "refresh_token": "TAP_YOUTUBE_ANALYTICS_REFRESH_TOKEN"
        }

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date,
            "user_agent": "Singer.io Youtube Analytics Tap",
            "channel_ids": os.getenv("TAP_YOUTUBE_ANALYTICS_CHANNEL_IDS", "")
        }

        return return_value

    def expected_parent_tap_stream(self, stream=None):
        """return a dictionary with key of table name and value of parent stream"""
        parent_stream = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream
        return parent_stream[stream]

    def get_streams_to_exclude(self):
        """Return a set of streams to exclude from testing."""
        # We don't have access to these streams
        return {
            'playlist_combined',
            'channel_playback_location',
            'content_owner_playlist_traffic_source',
            'content_owner_end_screens',
            'playlist_traffic_source',
            'content_owner_playlist_province',
            'content_owner_asset_traffic_source',
            'content_owner_asset_sharing_service',
            'content_owner_device_os',
            'content_owner_playlist_basic',
            'channel_province',
            'content_owner_asset_annotations',
            'content_owner_demographics',
            'channel_cards',
            'channel_traffic_source',
            'content_owner_playlist_combined',
            'content_owner_asset_cards',
            'content_owner_province',
            'playlist_basic',
            'channel_combined',
            'playlist_province',
            'channel_sharing_service',
            'content_owner_basic',
            'playlist_playback_location',
            'content_owner_sharing_service',
            'content_owner_estimated_revenue',
            'content_owner_asset_province',
            'content_owner_playlist_playback_location',
            'content_owner_asset_basic',
            'channel_demographics',
            'content_owner_ad_rates',
            'content_owner_asset_estimated_revenue',
            'content_owner_asset_end_screens',
            'content_owner_traffic_source',
            'content_owner_combined',
            'channel_end_screens',
            'channel_annotations',
            'channel_device_os',
            'content_owner_playlist_device_os',
            'playlist_device_os',
            'channel_subtitles',
            'channel_basic',
            'content_owner_playback_location',
            'content_owner_asset_playback_location',
            'content_owner_asset_device_os',
            'content_owner_annotations',
            'content_owner_asset_demographics',
            'content_owner_cards',
            'content_owner_asset_combined',
            'content_owner_subtitles'
        }
