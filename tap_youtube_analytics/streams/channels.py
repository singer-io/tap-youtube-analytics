from typing import Dict, Iterator, List
from singer import get_logger
from tap_youtube_analytics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Channels(FullTableStream):
    tap_stream_id = "channels"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    params = {"maxResults": 1, "part": "id,contentDetails,snippet,statistics,status"}
    data_key = "items"
    path = "channels"
    endpoint = "channels"

    def update_params(self) -> Dict:
        """Update the params for the request."""
        channel_ids = self.client.config["channel_ids"]
        self.params = {
            **self.__class__.params,
            "id": channel_ids,
        }
        return self.params
