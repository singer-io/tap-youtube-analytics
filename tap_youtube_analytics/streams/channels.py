from typing import Dict, Iterator, List
from singer import get_logger
from tap_youtube_analytics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Channels(FullTableStream):
    tap_stream_id = "channels"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    params = {"id": "{channel_ids}", "maxResults": 50, "part": "id,contentDetails,snippet,statistics,status"}
