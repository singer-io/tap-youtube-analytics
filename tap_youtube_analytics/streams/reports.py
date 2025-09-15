from tap_youtube_analytics.streams.abstracts import ReportStream


class ChannelBasicStream(ReportStream):
    """Channel Basic Stream"""
    tap_stream_id = "channel_basic"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_basic_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status", 
        "country_code"
    ]
    metrics = [
        "engaged_views", "views", "comments", "likes", "dislikes", "videos_added_to_playlists", 
        "videos_removed_from_playlists", "shares", "watch_time_minutes", 
        "average_view_duration_seconds", "average_view_duration_percentage", 
        "annotation_click_through_rate", "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", 
        "annotation_clicks", "annotation_closes", "card_click_rate", 
        "card_teaser_click_rate", "card_impressions", "card_teaser_impressions", 
        "card_clicks", "card_teaser_clicks", "subscribers_gained", "subscribers_lost", 
        "red_views", "red_watch_time_minutes"
    ]


class ChannelProvinceStream(ReportStream):
    tap_stream_id = "channel_province"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_province_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status", 
        "country_code", "province_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "annotation_click_through_rate", 
        "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", 
        "annotation_clicks", "annotation_closes", "card_click_rate", 
        "card_teaser_click_rate", "card_impressions", "card_teaser_impressions", 
        "card_clicks", "card_teaser_clicks", "red_views", "red_watch_time_minutes"
    ]


class ChannelPlaybackLocationStream(ReportStream):
    tap_stream_id = "channel_playback_location"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_playback_location_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status", 
        "country_code", "playback_location_type", "playback_location_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]


class ChannelTrafficSourceStream(ReportStream):
    tap_stream_id = "channel_traffic_source"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_traffic_source_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status", 
        "country_code", "traffic_source_type", "traffic_source_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ChannelDeviceOSStream(ReportStream):
    tap_stream_id = "channel_device_os"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_device_os_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ChannelDemographicsStream(ReportStream):
    tap_stream_id = "channel_demographics"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_demographics_a1"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "age_group", "gender"
    ]
    metrics = [
        "views_percentage"
    ]

class ChannelSharingServiceStream(ReportStream):
    tap_stream_id = "channel_sharing_service"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_sharing_service_a2"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "sharing_service"
    ]
    metrics = ["shares"]

class ChannelAnnotationsStream(ReportStream):
    tap_stream_id = "channel_annotations"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_annotations_a2"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "annotation_type", "annotation_id"
    ]
    metrics = [
        "annotation_click_through_rate", "annotation_close_rate",
        "annotation_impressions", "annotation_clickable_impressions",
        "annotation_closable_impressions", "annotation_clicks", "annotation_closes"
    ]

class ChannelCardsStream(ReportStream):
    tap_stream_id = "channel_cards"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_cards_a2"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "card_type", "card_id"
    ]
    metrics = [
        "card_click_rate", "card_teaser_click_rate", "card_impressions",
        "card_teaser_impressions", "card_clicks", "card_teaser_clicks"
    ]

class ChannelEndScreensStream(ReportStream):
    tap_stream_id = "channel_end_screens"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_end_screens_a2"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "end_screen_element_type", "end_screen_element_id"
    ]
    metrics = [
        "end_screen_element_clicks", "end_screen_element_impressions", "end_screen_element_click_rate"
    ]

class ChannelSubtitlesStream(ReportStream):
    tap_stream_id = "channel_subtitles"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_subtitles_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "subtitle_language", "subtitle_language_autotranslated"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ChannelCombinedStream(ReportStream):
    tap_stream_id = "channel_combined"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "channel_combined_a3"
    dimensions = [
        "date", "channel_id", "video_id", "live_or_on_demand", "subscribed_status",
        "country_code", "playback_location_type", "traffic_source_type", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class PlaylistBasicStream(ReportStream):
    tap_stream_id = "playlist_basic"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_basic_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class PlaylistProvinceStream(ReportStream):
    tap_stream_id = "playlist_province"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_province_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code", "province_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class PlaylistPlaybackLocationStream(ReportStream):
    tap_stream_id = "playlist_playback_location"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_playback_location_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code", "playback_location_type", "playback_location_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class PlaylistTrafficSourceStream(ReportStream):
    tap_stream_id = "playlist_traffic_source"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_traffic_source_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code", "traffic_source_type", "traffic_source_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class PlaylistDeviceOSStream(ReportStream):
    tap_stream_id = "playlist_device_os"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_device_os_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class PlaylistCombinedStream(ReportStream):
    tap_stream_id = "playlist_combined"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "playlist_combined_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand",
        "subscribed_status", "country_code", "playback_location_type", "traffic_source_type",
        "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds",
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerBasicStream(ReportStream):
    tap_stream_id = "content_owner_basic"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_basic_a4"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code"
    ]
    metrics = [
        "engaged_views", "views", "comments", "shares", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "annotation_click_through_rate", "annotation_close_rate", 
        "annotation_impressions", "annotation_clickable_impressions", "annotation_closable_impressions", 
        "annotation_clicks", "annotation_closes", "card_click_rate", "card_teaser_click_rate", 
        "card_impressions", "card_teaser_impressions", "card_clicks", "card_teaser_clicks", 
        "subscribers_gained", "subscribers_lost", "videos_added_to_playlists", "videos_removed_from_playlists", 
        "likes", "dislikes", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerProvinceStream(ReportStream):
    tap_stream_id = "content_owner_province"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_province_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", "subscribed_status", 
        "country_code", "province_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "annotation_click_through_rate", "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", "annotation_clicks", 
        "annotation_closes", "card_click_rate", "card_teaser_click_rate", "card_impressions", 
        "card_teaser_impressions", "card_clicks", "card_teaser_clicks", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerPlaybackLocationStream(ReportStream):
    tap_stream_id = "content_owner_playback_location"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playback_location_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "playback_location_type", "playback_location_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerTrafficSourceStream(ReportStream):
    tap_stream_id = "content_owner_traffic_source"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_traffic_source_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "traffic_source_type", "traffic_source_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerDeviceOSStream(ReportStream):
    tap_stream_id = "content_owner_device_os"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_device_os_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerDemographicsStream(ReportStream):
    tap_stream_id = "content_owner_demographics"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_demographics_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "age_group", "gender"
    ]
    metrics = ["views_percentage"]

class ContentOwnerSharingServiceStream(ReportStream):
    tap_stream_id = "content_owner_sharing_service"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_sharing_service_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "sharing_service"
    ]
    metrics = ["shares"]

class ContentOwnerAnnotationsStream(ReportStream):
    tap_stream_id = "content_owner_annotations"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_annotations_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "annotation_type", "annotation_id"
    ]
    metrics = [
        "annotation_click_through_rate", "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", "annotation_clicks", 
        "annotation_closes"
    ]

class ContentOwnerCardsStream(ReportStream):
    tap_stream_id = "content_owner_cards"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_cards_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "card_type", "card_id"
    ]
    metrics = [
        "card_click_rate", "card_teaser_click_rate", "card_impressions", "card_teaser_impressions", 
        "card_clicks", "card_teaser_clicks"
    ]

class ContentOwnerEndScreensStream(ReportStream):
    tap_stream_id = "content_owner_end_screens"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_end_screens_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "end_screen_element_type", "end_screen_element_id"
    ]
    metrics = [
        "end_screen_element_clicks", "end_screen_element_impressions", "end_screen_element_click_rate"
    ]

class ContentOwnerSubtitlesStream(ReportStream):
    tap_stream_id = "content_owner_subtitles"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_subtitles_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "subtitle_language", "subtitle_language_autotranslated"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerCombinedStream(ReportStream):
    tap_stream_id = "content_owner_combined"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_combined_a3"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", "live_or_on_demand", 
        "subscribed_status", "country_code", "playback_location_type", "traffic_source_type", 
        "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", "average_view_duration_percentage", 
        "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerPlaylistBasicStream(ReportStream):
    tap_stream_id = "content_owner_playlist_basic"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_basic_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerPlaylistProvinceStream(ReportStream):
    tap_stream_id = "content_owner_playlist_province"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_province_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code", "province_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerPlaylistPlaybackLocationStream(ReportStream):
    tap_stream_id = "content_owner_playlist_playback_location"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_playback_location_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code", "playback_location_type", 
        "playback_location_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerPlaylistTrafficSourceStream(ReportStream):
    tap_stream_id = "content_owner_playlist_traffic_source"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_traffic_source_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code", "traffic_source_type", "traffic_source_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerPlaylistDeviceOSStream(ReportStream):
    tap_stream_id = "content_owner_playlist_device_os"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_device_os_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerPlaylistCombinedStream(ReportStream):
    tap_stream_id = "content_owner_playlist_combined"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_playlist_combined_a2"
    dimensions = [
        "date", "channel_id", "playlist_id", "video_id", "live_or_on_demand", 
        "subscribed_status", "country_code", "playback_location_type", 
        "traffic_source_type", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "playlist_starts", "playlist_saves_added", "playlist_saves_removed"
    ]

class ContentOwnerAdRatesStream(ReportStream):
    tap_stream_id = "content_owner_ad_rates"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_ad_rates_a1"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", 
        "country_code", "ad_type"
    ]
    metrics = [
        "estimated_youtube_ad_revenue", "ad_impressions", "estimated_cpm"
    ]

class ContentOwnerEstimatedRevenueStream(ReportStream):
    tap_stream_id = "content_owner_estimated_revenue"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_estimated_revenue_a2"
    dimensions = [
        "date", "channel_id", "video_id", "claimed_status", "uploader_type", 
        "country_code"
    ]
    metrics = [
        "estimated_partner_revenue", "estimated_partner_ad_revenue", 
        "estimated_partner_ad_auction_revenue", "estimated_partner_ad_reserved_revenue", 
        "estimated_youtube_ad_revenue", "estimated_monetized_playbacks", 
        "estimated_playback_based_cpm", "ad_impressions", "estimated_cpm", 
        "estimated_partner_red_revenue", "estimated_partner_transaction_revenue"
    ]

class ContentOwnerAssetEstimatedRevenueStream(ReportStream):
    tap_stream_id = "content_owner_asset_estimated_revenue"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_estimated_revenue_a2"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", 
        "uploader_type", "country_code"
    ]
    metrics = [
        "estimated_partner_revenue", "estimated_partner_ad_revenue", 
        "estimated_partner_ad_auction_revenue", "estimated_partner_ad_reserved_revenue", 
        "estimated_partner_red_revenue", "estimated_partner_transaction_revenue"
    ]

class ContentOwnerAssetBasicStream(ReportStream):
    tap_stream_id = "content_owner_asset_basic"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_basic_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", 
        "uploader_type", "live_or_on_demand", "subscribed_status", "country_code"
    ]
    metrics = [
        "engaged_views", "views", "comments", "likes", "dislikes", "videos_added_to_playlists", 
        "videos_removed_from_playlists", "shares", "watch_time_minutes", 
        "average_view_duration_seconds", "average_view_duration_percentage", 
        "annotation_click_through_rate", "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", "annotation_clicks", 
        "annotation_closes", "card_click_rate", "card_teaser_click_rate", "card_impressions", 
        "card_teaser_impressions", "card_clicks", "card_teaser_clicks", "red_views", 
        "red_watch_time_minutes"
    ]

class ContentOwnerAssetProvinceStream(ReportStream):
    tap_stream_id = "content_owner_asset_province"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_province_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "province_code"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "annotation_click_through_rate", 
        "annotation_close_rate", "annotation_impressions", "annotation_clickable_impressions", 
        "annotation_closable_impressions", "annotation_clicks", "annotation_closes", 
        "card_click_rate", "card_teaser_click_rate", "card_impressions", "card_teaser_impressions", 
        "card_clicks", "card_teaser_clicks", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerAssetPlaybackLocationStream(ReportStream):
    tap_stream_id = "content_owner_asset_playback_location"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_playback_location_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "playback_location_type", 
        "playback_location_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerAssetTrafficSourceStream(ReportStream):
    tap_stream_id = "content_owner_asset_traffic_source"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_traffic_source_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "traffic_source_type", 
        "traffic_source_detail"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerAssetDeviceOSStream(ReportStream):
    tap_stream_id = "content_owner_asset_device_os"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_device_os_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]

class ContentOwnerAssetDemographicsStream(ReportStream):
    tap_stream_id = "content_owner_asset_demographics"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_demographics_a1"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "age_group", "gender"
    ]
    metrics = ["views_percentage"]

class ContentOwnerAssetSharingServiceStream(ReportStream):
    tap_stream_id = "content_owner_asset_sharing_service"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_sharing_service_a1"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "sharing_service"
    ]
    metrics = ["shares"]

class ContentOwnerAssetAnnotationsStream(ReportStream):
    tap_stream_id = "content_owner_asset_annotations"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_annotations_a1"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "annotation_type", "annotation_id"
    ]
    metrics = [
        "annotation_click_through_rate", "annotation_close_rate", "annotation_impressions", 
        "annotation_clickable_impressions", "annotation_closable_impressions", "annotation_clicks", 
        "annotation_closes"
    ]

class ContentOwnerAssetCardsStream(ReportStream):
    tap_stream_id = "content_owner_asset_cards"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_cards_a1"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "card_type", "card_id"
    ]
    metrics = [
        "card_click_rate", "card_teaser_click_rate", "card_impressions", "card_teaser_impressions", 
        "card_clicks", "card_teaser_clicks"
    ]

class ContentOwnerAssetEndScreensStream(ReportStream):
    tap_stream_id = "content_owner_asset_end_screens"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_end_screens_a1"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "end_screen_element_type", 
        "end_screen_element_id"
    ]
    metrics = [
        "end_screen_element_clicks", "end_screen_element_impressions", "end_screen_element_click_rate"
    ]

class ContentOwnerAssetCombinedStream(ReportStream):
    tap_stream_id = "content_owner_asset_combined"
    key_properties = ["dimensions_hash_key", "date"]
    replication_method = "INCREMENTAL"
    replication_keys = ["create_time"]
    path = "jobs"
    endpoint = "jobs"
    report_type = "content_owner_asset_combined_a3"
    dimensions = [
        "date", "channel_id", "video_id", "asset_id", "claimed_status", "uploader_type", 
        "live_or_on_demand", "subscribed_status", "country_code", "playback_location_type", 
        "traffic_source_type", "device_type", "operating_system"
    ]
    metrics = [
        "engaged_views", "views", "watch_time_minutes", "average_view_duration_seconds", 
        "average_view_duration_percentage", "red_views", "red_watch_time_minutes"
    ]