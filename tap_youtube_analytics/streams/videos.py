from typing import Dict, Any
from singer import get_bookmark, get_logger
from tap_youtube_analytics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Videos(IncrementalStream):
    tap_stream_id = "videos"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["published_at"]
    params = {"id": "{video_ids}", "maxResults": 50, "part": "id,contentDetails,player,snippet,statistics,status"}
