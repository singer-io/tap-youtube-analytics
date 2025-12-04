from typing import Any, Dict

from singer import Transformer, get_logger, metrics, utils, write_bookmark, write_record
from tap_youtube_analytics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class PlaylistItems(IncrementalStream):
    tap_stream_id = "playlist_items"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["published_at"]
    path = "playlistItems"
    endpoint = "playlist_items"

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Incrementally sync videos for all configured channel IDs."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        last_dttm = utils.strptime_to_utc(bookmark_date)
        current_max_bookmark_date = bookmark_date

        channel_ids = self.client.config["channel_ids"]
        channel_list = [cid.strip() for cid in channel_ids.split(",")]

        with metrics.record_counter(self.tap_stream_id) as counter:
            for channel_id in channel_list:
                playlist_params = {
                    "channelId": channel_id,
                    "maxResults": 50,
                    "part": "id,contentDetails,player,snippet,status"
                }

                self.path = "playlists"
                self.endpoint = "playlists"
                self.data_key = "items"
                self.params = playlist_params

                playlists = self.get_records()
                for playlist in playlists:
                    playlist_id = playlist.get("id")
                    self.params = {
                        "maxResults": 50,
                        "part": "id,contentDetails,snippet,status",
                    }
                    self.params["playlistId"] = playlist_id
                    self.path = "playlistItems"
                    self.endpoint = "playlist_items"
                    self.data_key = "items"

                    records = self.get_records()
                    exhausted_playlist = False
                    for record in records:
                        for key in self.key_properties:
                            if not record.get(key):
                                raise ValueError(f"Stream: {self.tap_stream_id}, Missing key: {key}")


                        transformed_record = transformer.transform(
                            self.transform_data_record(record),
                            self.schema,
                            self.metadata,
                        )
                        record_timestamp = transformed_record[self.replication_keys[0]]

                        record_dttm = utils.strptime_to_utc(record_timestamp)
                        if record_dttm >= last_dttm:
                            if self.is_selected():
                                write_record(self.tap_stream_id, transformed_record)
                                counter.increment()

                            current_max_bookmark_date = max(
                                current_max_bookmark_date, record_timestamp
                            )

                            for child in self.child_to_sync:
                                child.sync(state=state, transformer=transformer, parent_obj=record)
                        else:
                            exhausted_playlist = True
                            break

                    if exhausted_playlist:
                        continue


            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value
