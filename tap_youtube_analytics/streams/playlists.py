from typing import Dict, Iterator, List
from singer import get_logger
from tap_youtube_analytics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Playlists(FullTableStream):
    tap_stream_id = "playlists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    params = {"id": "{parent_id}", "maxResults": 50, "part": "id,contentDetails,player,snippet,status"}
