from typing import Any, Dict, Iterable, List, Tuple

from singer import Transformer, get_logger, metrics, utils, write_bookmark, write_record
from tap_youtube_analytics.streams.abstracts import IncrementalStream

LOGGER = get_logger()


class Videos(IncrementalStream):
    tap_stream_id = "videos"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["published_at"]
    data_key = "items"
    path = "search"
    endpoint = "search_videos"

    def chunks(self, items: Iterable[str], cnt: int) -> Iterable[List[str]]:
        """Yield successive n-sized chunks from any iterable."""
        items_list = list(items)
        for i in range(0, len(items_list), cnt):
            yield items_list[i:i + cnt]

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

        total_written = 0

        with metrics.record_counter(self.tap_stream_id) as counter:
            for channel_id in channel_list:
                search_params = {
                    "part": "id,snippet",
                    "channelId": channel_id,
                    "order": "date",
                    "type": "video",
                    "maxResults": 50,
                    "publishedAfter": bookmark_date,
                }

                self.path = "search"
                self.endpoint = "search_videos"
                self.data_key = "items"
                self.params = search_params

                search_records = self.get_records()
                video_ids = set()

                for search_record in search_records:
                    video_id = search_record.get("id", {}).get("videoId")
                    if not video_id:
                        continue

                    published_at = search_record.get("snippet", {}).get("publishedAt")
                    if not published_at:
                        continue

                    try:
                        search_record_dttm = utils.strptime_to_utc(published_at)
                    except Exception:
                        continue
                    if search_record_dttm < last_dttm:
                        break

                    video_ids.add(video_id)
                    current_max_bookmark_date = max(
                        current_max_bookmark_date, published_at
                    )

                total_written, current_max_bookmark_date = self._fetch_and_emit_videos(
                    video_ids=video_ids,
                    last_dttm=last_dttm,
                    current_max_bookmark_date=current_max_bookmark_date,
                    transformer=transformer,
                    counter=counter,
                    total_written=total_written,
                    state=state,
                )


        state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
        return total_written

    def _fetch_and_emit_videos(
        self,
        video_ids: Iterable[str],
        last_dttm: Any,
        current_max_bookmark_date: str,
        transformer: Transformer,
        counter: metrics.Counter,
        total_written: int,
        state: Dict,
    ) -> Tuple[int, str]:
        """Fetch video details in chunks and emit records.

        Returns updated total_written and current_max_bookmark_date.
        """
        video_id_chunks = self.chunks(video_ids, 50)

        stop_fetching = False
        for video_id_chunk in video_id_chunks:
            videos_params = {
                "part": "id,snippet",
                "id": ",".join(video_id_chunk)
            }

            self.path = "videos"
            self.endpoint = "videos"
            self.data_key = "items"
            self.params = videos_params

            records = self.get_records()

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
                        total_written += 1

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)
                else:
                    stop_fetching = True
                    break

            if stop_fetching:
                break

        return total_written, current_max_bookmark_date
