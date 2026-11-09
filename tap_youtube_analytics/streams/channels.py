from typing import Dict, Iterator, List
from singer import get_logger
from tap_youtube_analytics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Channels(FullTableStream):
    tap_stream_id = "channels"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    params = {"maxResults": 50, "part": "id,contentDetails,snippet,statistics,status"}
    data_key = "items"
    path = "channels"
    endpoint = "channels"

    def transform_data_record(self, record):
        new_record = super().transform_data_record(record)
        new_record["channel_id"] = new_record.get("id")
        snippet = new_record.get("snippet") or {}
        new_record["name"] = snippet.get("title")
        return new_record

    def update_params(self) -> Dict:
        """Update the params for the request."""
        channel_ids = self.client.config["channel_ids"]
        id_csv = ",".join(cid.strip() for cid in channel_ids.split(",") if cid.strip())
        self.params = {
            **self.__class__.params,
            "id": id_csv,
        }
        return self.params
