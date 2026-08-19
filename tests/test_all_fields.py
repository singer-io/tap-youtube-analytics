from base import YoutubeAnalyticsBaseTest
from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest


class YoutubeAnalyticsAllFields(AllFieldsTest, YoutubeAnalyticsBaseTest):
    """Ensure running the tap with all streams and fields selected results in
    the replication of all fields."""

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

    KEYS_WITH_NO_DATA = {
        "channel_basic": [
            'subtitle_language',
            'card_type',
            'end_screen_element_clicks',
            'estimated_playback_based_cpm',
            'end_screen_element_type',
            'estimated_partner_transaction_revenue',
            'asset_id',
            'age_group',
            'ad_impressions',
            'claimed_status',
            'end_screen_element_impressions',
            'uploader_type',
            'estimated_partner_ad_sense_revenue',
            'estimated_partner_double_click_revenue',
            'estimated_monetized_playbacks',
            'province_code',
            'traffic_source_type',
            'playback_location_detail',
            'traffic_source_detail',
            'estimated_partner_ad_reserved_revenue',
            'estimated_partner_red_revenue',
            'device_type',
            'playlist_saves_added',
            'gender',
            'views_percentage',
            'playback_location_type',
            'playlist_starts',
            'estimated_cpm',
            'operating_system',
            'playlist_saves_removed',
            'ad_type',
            'estimated_partner_ad_revenue',
            'estimated_partner_ad_auction_revenue',
            'annotation_id',
            'end_screen_element_click_rate',
            'annotation_type',
            'card_id',
            'estimated_youtube_ad_revenue',
            'playlist_id',
            'end_screen_element_id',
            'estimated_partner_revenue',
            'audience_retention_percentage',
            'sharing_service'
        ]
    }

    @staticmethod
    def name():
        return "tap_tester_youtube_analytics_all_fields_test"

    @classmethod
    def expected_stream_names(cls):
        return super().expected_stream_names().union(cls.NEWLY_DISCOVERED_STREAMS)

    def streams_to_test(self):
        streams_to_exclude = self.get_streams_to_exclude()
        return self.expected_stream_names().difference(streams_to_exclude)
