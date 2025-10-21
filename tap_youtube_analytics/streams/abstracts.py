import hashlib
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import humps
from singer import (
    Transformer,
    get_bookmark,
    get_logger,
    metadata,
    metrics,
    utils,
    write_bookmark,
    write_record,
    write_schema,
)

from tap_youtube_analytics.exceptions import (
    YoutubeAnalyticsError,
    YoutubeAnalyticsForbiddenError,
)

LOGGER = get_logger()
ATTRIBUTION_DAYS = 7
DEFAULT_REPORT_PAGE_SIZE = 50


class BaseStream(ABC):
    """A Base Class providing structure and boilerplate for generic streams
    and required attributes for any kind of stream
    ~~~
    Provides:
     - Basic Attributes (stream_name,replication_method,key_properties)
     - Helper methods for catalog generation
     - `sync` and `get_records` method for performing sync
    """

    url_endpoint = ""
    path = ""
    next_page_key = "nextPageToken"
    headers = {"Accept": "application/json"}
    params = {}
    children = []
    parent = ""
    data_key = ""
    parent_bookmark_key = ""
    _dim_lookup_map = None  # Class-level cache for dimension lookup map

    def __init__(self, client=None, catalog=None) -> None:
        self.client = client
        self.catalog = catalog
        self.schema = catalog.schema.to_dict()
        self.metadata = metadata.to_map(catalog.metadata)
        self.child_to_sync = []
        self.params = {}

    @property
    @abstractmethod
    def tap_stream_id(self) -> str:
        """Unique identifier for the stream.

        This is allowed to be different from the name of the stream, in
        order to allow for sources that have duplicate stream names.
        """

    @property
    @abstractmethod
    def replication_method(self) -> str:
        """Defines the sync mode of a stream."""

    @property
    @abstractmethod
    def replication_keys(self) -> str:
        """Defines the replication key for incremental sync mode of a
        stream."""

    @property
    @abstractmethod
    def key_properties(self) -> List[str]:
        """List of key properties for stream."""

    def is_selected(self):
        return metadata.get(self.metadata, (), "selected")

    @abstractmethod
    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Performs a replication sync for the stream.
        ~~~
        Args:
         - state (dict): represents the state file for the tap.
         - transformer (object): A Object of the singer.transformer class.
         - parent_obj (dict): The parent object for the stream.

        Returns:
         - bool: The return value. True for success, False otherwise.

        Docs:
         - https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md
        """


    def get_records(self, isreport=False) -> List:
        """Interacts with api client interaction and pagination."""
        total_count = 0
        page = 1
        is_next_page = True
        page_token = ""
        consecutive_empty_pages = 0
        max_empty_pages = 3  # Prevent infinite loops

        while is_next_page:
            if page > 1:
                self.params["pageToken"] = page_token

            if isreport:
                url = self.client.reporting_url
                # YouTube Analytics API doesn't use data_key, data is in 'rows'
                data_key = "rows"
            else:
                url = self.client.base_url
                data_key = self.data_key

            response = {}
            response = self.client.get(
                url=url,
                path=self.path,
                params=self.params,
                endpoint=self.url_endpoint
            )
            if not response or response is None or response == {}:
                LOGGER.info("Data not found for endpoint: %s", self.url_endpoint)
                break

            if isreport:
                # YouTube Analytics API response structure
                results = response.get("rows", [])
                total_results = len(results)  # Set total_results for reports
                # Convert rows to dict format using columnHeaders
                column_headers = response.get("columnHeaders", [])
                if column_headers and results:
                    # Convert each row array to dict using column headers
                    converted_results = []
                    for row in results:
                        if len(row) == len(column_headers):
                            record = {}
                            for i, header in enumerate(column_headers):
                                record[header.get('name', f'col_{i}')] = row[i]
                            converted_results.append(record)
                    results = converted_results
            else:
                total_results = response.get("pageInfo", {}).get("totalResults", 0)
                results = response.get(data_key, [])

            results_count = len(results)
            from_count = total_count + 1
            total_count = total_count + results_count
            to_count = total_count

            LOGGER.info(f"Endpoint: {self.url_endpoint}, Page: {page}, Results: {from_count}-{to_count} of Total: {total_results}")

            if not results or results is None or results == []:
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= max_empty_pages:
                    LOGGER.warning(f"Breaking pagination after {max_empty_pages} consecutive empty pages")
                    break
            else:
                consecutive_empty_pages = 0  # Reset counter when we get data

            for result in results:
                if result is None:
                    continue
                yield result

            # YouTube Analytics API doesn't support pagination like YouTube Data API
            if isreport:
                is_next_page = False
            else:
                # Pagination: increment the offset by the limit (batch-size)
                page_token = response.get("nextPageToken")
                if page_token is None:
                    is_next_page = False
            page += 1

    def write_schema(self) -> None:
        """Write a schema message."""
        try:
            write_schema(self.tap_stream_id, self.schema, self.key_properties)
        except OSError as err:
            LOGGER.error(
                f"OS Error while writing schema for: {self.tap_stream_id}"
            )
            raise err

    def update_params(self, **kwargs) -> None:
        """Update params for the stream"""
        self.params.update(kwargs)

    def modify_object(self, record: Dict, parent_record: Dict = None) -> Dict:
        """Modify the record before writing to the stream"""
        return record

    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        """Get the URL endpoint for the stream"""
        return self.url_endpoint or f"{self.client.base_url}/{self.path}"

    # PyHumps: camelCase to snake_case
    # Reference: https://github.com/nficano/humps
    def transform_data_record(self, record):
        """"Transform data record to snake_case."""
        new_record = humps.decamelize(record.copy())

        # denest published_at
        published_at = new_record.get("snippet", {}).get("published_at")
        new_record["published_at"] = published_at

        return new_record
    

    def hash_data(self, data):
        """Create MD5 hash key for data element
        Prepare the project id hash"""
        hash_id = hashlib.md5()
        hash_id.update(repr(data).encode('utf-8'))

        return hash_id.hexdigest()
    
    def _load_dim_lookup_map(self):
        """Load and cache the dimension lookup map"""
        if BaseStream._dim_lookup_map is None:
            map_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dim_lookup_map.json')
            try:
                if os.path.exists(map_path):
                    with open(map_path) as file:
                        BaseStream._dim_lookup_map = json.load(file)
                        LOGGER.info("Loaded dimension lookup map from file")
                else:
                    LOGGER.warning(f"Dimension lookup map file not found at {map_path}, using empty map")
                    BaseStream._dim_lookup_map = {}
            except (IOError, json.JSONDecodeError) as e:
                LOGGER.error(f"Failed to load dimension lookup map: {e}")
                BaseStream._dim_lookup_map = {}
        return BaseStream._dim_lookup_map
        
    # dim_lookup_map.json: code to description mapping dictionary for each dimension
    # Created from Dimensions lookup tables here:
    #   https://developers.google.com/youtube/reporting/v1/reports/dimensions#Annotation_Dimensions
    # Google Sheet for creating/maintaining dim_lookup_map.json:
    #   https://docs.google.com/spreadsheets/d/1qR1kCiqwcvkZL4z9e0hxWa1kokesCSRv4LLyPmnnuUI/edit?usp=sharing
    def transform_report_record(self, record, dimensions, report):
        """Transform report records"""

        new_record = record.copy()
        dim_lookup_map = self._load_dim_lookup_map()

        dimension_values = {}

        for key, val in list(record.items()):
            # Add dimension key-val to dimension_values dictionary
            if key in dimensions:
                dimension_values[key] = val

            # Transform dim values from codes to names using dim_lookup_map
            if key in dim_lookup_map:
                # lookup new_val, with a default for existing val (if not found)
                new_val = dim_lookup_map[key].get(val, val)
                if val == new_val:
                    LOGGER.warning(f"dim_lookup_map value not found; key: {key}, value: {val}")
                new_record[key] = new_val
            else:
                new_record[key] = val

        # Add report fields to data
        new_record['report_id'] = report.get('id')
        new_record['report_type_id'] = report.get('reportTypeId')
        new_record['report_name'] = report.get('name')
        new_record['create_time'] = report.get('createTime')

        # Create unique md5 hash key for dimension_values
        dims_md5 = str(self.hash_data(json.dumps(dimension_values, sort_keys=True)))
        new_record['dimensions_hash_key'] = dims_md5

        return new_record

    def append_times_to_dates(self, record: Dict) -> None:
        """Append time portion to date-only fields to ensure proper datetime format.
        
        This method ensures that date fields have time components for consistent
        datetime handling across the pipeline.
        """
        date_fields = ['published_at', 'create_time', 'updated_at', 'scheduled_start_time', 'scheduled_end_time']
        
        for field in date_fields:
            if field in record and record[field]:
                try:
                    # Parse the date string
                    date_value = record[field]
                    if isinstance(date_value, str):
                        # If it's just a date (YYYY-MM-DD), append time
                        if len(date_value) == 10 and 'T' not in date_value:
                            record[field] = f"{date_value}T00:00:00Z"
                        # If it's already a datetime string, ensure it has timezone
                        elif 'T' in date_value and not date_value.endswith('Z') and '+' not in date_value:
                            record[field] = f"{date_value}Z"
                except (ValueError, TypeError) as e:
                    LOGGER.warning(f"Failed to process date field {field}: {e}")
                    continue
    

class IncrementalStream(BaseStream):
    """Base Class for Incremental Stream."""
    replication_method = "INCREMENTAL"
    forced_replication_method = "INCREMENTAL"

    def get_bookmark(self, state: dict, stream: str, key: Any = None) -> int:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        bookmark_key = key or (self.replication_keys[0] if self.replication_keys else None)
        self._migrate_legacy_bookmark(state, stream, bookmark_key)
        return get_bookmark(
            state,
            stream,
            bookmark_key,
            self.client.config["start_date"],
        )

    def write_bookmark(self, state: dict, stream: str, key: Any = None, value: Any = None) -> Dict:
        """A wrapper for singer.get_bookmark to deal with compatibility for
        bookmark values or start values."""
        if not (key or self.replication_keys):
            return state

        bookmark_key = key or self.replication_keys[0]
        self._migrate_legacy_bookmark(state, stream, bookmark_key)
        current_bookmark = get_bookmark(state, stream, bookmark_key, self.client.config["start_date"])
        value = max(current_bookmark, value)
        return write_bookmark(
            state, stream, bookmark_key, value
        )

    def _migrate_legacy_bookmark(self, state: dict, stream: str, key: Any) -> None:
        """Upgrade legacy flat bookmarks to the nested structure expected by Singer."""
        if not state or not isinstance(state, dict):
            return

        bookmarks = state.get("bookmarks")
        if not isinstance(bookmarks, dict) or stream not in bookmarks:
            return

        current = bookmarks.get(stream)
        if isinstance(current, str) and key:
            bookmarks[stream] = {key: current}
        elif current is None:
            bookmarks[stream] = {}


    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Implementation for `type: Incremental` stream."""
        bookmark_date = self.get_bookmark(state, self.tap_stream_id)
        current_max_bookmark_date = bookmark_date
        self.update_params(updated_since=bookmark_date)
        self.url_endpoint = self.get_url_endpoint(parent_obj)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                record = self.modify_object(record, parent_obj)
                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                self.append_times_to_dates(transformed_record)

                record_timestamp = transformed_record[self.replication_keys[0]]
                if record_timestamp >= bookmark_date:
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

                    current_max_bookmark_date = max(
                        current_max_bookmark_date, record_timestamp
                    )

                    for child in self.child_to_sync:
                        child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(state, self.tap_stream_id, value=current_max_bookmark_date)
            return counter.value


class FullTableStream(BaseStream):
    """Base Class for Full Table Stream."""

    replication_method = "FULL_TABLE"
    forced_replication_method = "FULL_TABLE"
    valid_replication_keys = None
    replication_keys = None

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        """Abstract implementation for `type: Fulltable` stream."""
        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_params()
        with metrics.record_counter(self.tap_stream_id) as counter:
            for record in self.get_records():
                transformed_record = transformer.transform(
                    self.transform_data_record(record), self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            return counter.value
        
class ReportStream(IncrementalStream):
    def get_url_endpoint(self, parent_obj: Dict = None) -> str:
        return self.client.reporting_url

    @staticmethod
    def _normalize_datetime(value: str) -> str:
        """Ensure datetime strings include a time component and timezone."""
        if not value:
            return value
        if len(value) == 10 and "T" not in value:
            value = f"{value}T00:00:00Z"
        if "T" in value and not value.endswith("Z") and "+" not in value:
            value = f"{value}Z"
        return value

    def update_params(self, **kwargs) -> None:
        """Update params for YouTube Reporting API requests"""
        super().update_params(**kwargs)

        params: Dict[str, Any] = {"pageSize": DEFAULT_REPORT_PAGE_SIZE}

        if hasattr(self, "report_type"):
            params["reportTypeId"] = self.report_type

        updated_since = kwargs.get("updated_since")
        if updated_since:
            params["createdAfter"] = self._normalize_datetime(updated_since)

        config_start = self.client.config.get("start_date")
        if config_start:
            params["startTimeAtOrAfter"] = self._normalize_datetime(config_start)

        end_time = kwargs.get("end_time")
        if end_time:
            params["startTimeBefore"] = self._normalize_datetime(end_time)

        self.params.update({k: v for k, v in params.items() if v})

    def get_records(self, isreport=False) -> List:
        """Get records from YouTube Reporting API jobs workflow"""
        if not isreport:
            # Use parent implementation for non-report streams
            return super().get_records(isreport)
            
        # YouTube Reporting API workflow
        # Step 1: List existing jobs for this report type
        jobs_url = f"{self.client.reporting_url}/jobs"
        jobs_params = {
            "includeSystemManaged": "true",
            "pageSize": DEFAULT_REPORT_PAGE_SIZE,
        }

        try:
            target_job = None
            page_token = None
            while True:
                if page_token:
                    jobs_params['pageToken'] = page_token

                jobs_response = self.client.get(
                    url=jobs_url,
                    params=jobs_params,
                    endpoint=f"{self.client.reporting_url}/jobs"
                )

                if not jobs_response:
                    LOGGER.info(
                        "No jobs found for report type: %s",
                        getattr(self, 'report_type', 'unknown')
                    )
                    break

                jobs = jobs_response.get('jobs', [])

                for job in jobs:
                    if job.get('reportTypeId') == getattr(self, 'report_type', None):
                        target_job = job
                        break

                if target_job or 'nextPageToken' not in jobs_response:
                    break

                page_token = jobs_response.get('nextPageToken')

            if not target_job and hasattr(self, 'report_type'):
                report_type = getattr(self, 'report_type', None)
                LOGGER.info(
                    "No existing job for report type %s. Creating new job.",
                    report_type,
                )
                create_payload = {
                    'name': self.tap_stream_id,
                    'reportTypeId': report_type,
                }
                target_job = self.client.post(
                    url=self.client.reporting_url,
                    path='jobs',
                    data=create_payload,
                    endpoint='job_create'
                ) or {}

            if not target_job:
                LOGGER.info(
                    "Unable to find or create reporting job for stream %s", self.tap_stream_id
                )
                return

            job_id = target_job.get('id')
            if not job_id:
                LOGGER.error("Job found but no job ID available")
                return
                
            # Step 2: Get reports for this job
            reports_url = f"{self.client.reporting_url}/jobs/{job_id}/reports"
            reports_params = {"pageSize": DEFAULT_REPORT_PAGE_SIZE}

            if 'createdAfter' in self.params:
                reports_params['createdAfter'] = self.params['createdAfter']
            if 'startTimeAtOrAfter' in self.params:
                reports_params['startTimeAtOrAfter'] = self.params['startTimeAtOrAfter']
            if 'startTimeBefore' in self.params:
                reports_params['startTimeBefore'] = self.params['startTimeBefore']

            page_token = None
            while True:
                if page_token:
                    reports_params['pageToken'] = page_token

                reports_response = self.client.get(
                    url=reports_url,
                    params=reports_params,
                    endpoint=f"{self.client.reporting_url}/jobs/{job_id}/reports"
                )

                if not reports_response:
                    LOGGER.info(f"No reports found for job: {job_id}")
                    break

                reports = reports_response.get('reports', [])
                LOGGER.info(f"Found {len(reports)} reports for job {job_id}")

                for report in reports:
                    download_url = report.get('downloadUrl')
                    if not download_url:
                        LOGGER.warning(f"Report {report.get('id')} has no download URL")
                        continue

                    LOGGER.info(f"Downloading report {report.get('id')} from {download_url}")

                    try:
                        row_count = 0
                        for record in self.client.get_report(url=download_url, endpoint=download_url):
                            row_count += 1
                            yield (record, report)

                        if row_count == 0:
                            LOGGER.info(f"Report {report.get('id')} returned empty data")
                        else:
                            LOGGER.info(f"Processed {row_count} rows from report {report.get('id')}")

                    except Exception as e:
                        LOGGER.error(f"Error downloading/parsing report {report.get('id')}: {e}")
                        continue
                page_token = reports_response.get('nextPageToken')
                if not page_token:
                    break

        except YoutubeAnalyticsForbiddenError as err:
            LOGGER.error(
                "YouTube Reporting API workflow failed with permission error: %s",
                err,
            )
            raise
        except YoutubeAnalyticsError as err:
            LOGGER.error(
                "YouTube Reporting API workflow failed: %s",
                err,
            )
            raise
        except Exception as err:
            LOGGER.error(
                "Unexpected error in YouTube Reporting API workflow: %s",
                err,
            )
            raise

    def sync(
        self,
        state: Dict,
        transformer: Transformer,
        parent_obj: Dict = None,
    ) -> Dict:
        bookmark_value = self._normalize_datetime(self.get_bookmark(state, self.tap_stream_id))

        try:
            bookmark_dttm = utils.strptime_to_utc(bookmark_value)
        except Exception as err:  # pragma: no cover - defensive
            LOGGER.warning(
                "Failed to parse bookmark %s for stream %s: %s",
                bookmark_value,
                self.tap_stream_id,
                err,
            )
            bookmark_dttm = utils.now() - timedelta(days=ATTRIBUTION_DAYS)

        attribution_cutoff = utils.now() - timedelta(days=ATTRIBUTION_DAYS)
        if attribution_cutoff < bookmark_dttm:
            effective_start_dttm = attribution_cutoff
            effective_start = utils.strftime(effective_start_dttm)
        else:
            effective_start_dttm = bookmark_dttm
            effective_start = bookmark_value

        current_max_dttm = bookmark_dttm

        self.url_endpoint = self.get_url_endpoint(parent_obj)
        self.update_params(updated_since=effective_start)

        with metrics.record_counter(self.tap_stream_id) as counter:
            for item in self.get_records(isreport=True):
                if not item:
                    continue

                # Support either (row, report) tuples or just row dicts
                if isinstance(item, tuple) and len(item) == 2:
                    record, report = item
                else:
                    record, report = item, {}

                dims = getattr(self, "dimensions", [])

                transformed_record = transformer.transform(
                    self.transform_report_record(record, dims, report),
                    self.schema,
                    self.metadata
                )

                record_time_raw = transformed_record.get(self.replication_keys[0])
                record_dttm = None
                if record_time_raw:
                    try:
                        normalized_time = self._normalize_datetime(record_time_raw)
                        record_dttm = utils.strptime_to_utc(normalized_time)
                    except Exception as err:
                        LOGGER.warning(
                            "Failed to parse record timestamp %s for stream %s: %s",
                            record_time_raw,
                            self.tap_stream_id,
                            err,
                        )

                if record_dttm and record_dttm < effective_start_dttm:
                    continue

                if record_dttm and record_dttm > current_max_dttm:
                    current_max_dttm = record_dttm

                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

                for child in self.child_to_sync:
                    child.sync(state=state, transformer=transformer, parent_obj=record)

            state = self.write_bookmark(
                state,
                self.tap_stream_id,
                value=utils.strftime(current_max_dttm),
            )
            return counter.value
