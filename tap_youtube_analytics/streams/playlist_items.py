from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_youtube_analytics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class PlaylistItems(IncrementalStream):
    tap_stream_id = "playlist_items"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["published_at"]
    params = {"id": "{parent_id}", "maxResults": 50, "part": "id,contentDetails,snippet,status"}
    path = "playlistItems"
