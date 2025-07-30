from typing import Dict, Iterator, List
from singer import get_logger
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metrics,
    write_bookmark,
    write_record,
    write_schema,
    metadata
)
from tap_youtube_analytics.streams.abstracts import FullTableStream

LOGGER = get_logger()


class Playlists(FullTableStream):
    tap_stream_id = "playlists"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    params = {"maxResults": 1, "part": "id,contentDetails,player,snippet,status"}
    data_key = "items"
    path = "playlists"
    endpoint = "playlists"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        channel_ids = self.client.config["channel_ids"]
        channel_list = [cid.strip() for cid in channel_ids.split(",")]

        with metrics.record_counter(self.tap_stream_id) as counter:
            for channel_id in channel_list:
                self.params["channelId"] = channel_id
                for record in self.get_records():
                    if record is None:
                        continue
                    transformed_record = transformer.transform(
                        self.transform_data_record(record), self.schema, self.metadata
                    )
                    if self.is_selected:
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
