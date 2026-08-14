import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_youtube_analytics.exceptions import (
    YoutubeAnalyticsUnauthorizedError,
    YoutubeAnalyticsForbiddenError,
    YoutubeAnalyticsNoAccessibleStreamsError,
)
from tap_youtube_analytics.schema import get_schemas

LOGGER = singer.get_logger()

# Streams served by the YouTube Data API v3 (base_url: googleapis.com/youtube/v3)
DATA_API_STREAMS = {"channels", "playlists", "playlist_items", "videos"}

# All remaining streams use the YouTube Reporting API (reporting_url: youtubereporting.googleapis.com/v1)
# They are identified at runtime as any stream NOT in DATA_API_STREAMS.

# Auth error types common to both APIs
_AUTH_ERROR_TYPES = (YoutubeAnalyticsUnauthorizedError, YoutubeAnalyticsForbiddenError)


def check_stream_access(stream_name, probe_fn, auth_error_types):
    """
    Probe a stream endpoint and return True if accessible, False on auth error.

    :param stream_name: Used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe.
    :param auth_error_types: Exception type(s) indicating 401/403 — returns False.
    """
    try:
        probe_fn()
        LOGGER.info("Stream '%s' is accessible.", stream_name)
        return True
    except auth_error_types:
        LOGGER.warning(
            "Stream '%s' is not accessible with the provided credentials.",
            stream_name,
        )
        return False


def _check_data_api_access(client) -> bool:
    """Probe Data API access using a minimal channels request."""
    def _probe():
        client.get(
            path="channels",
            params={"part": "id", "mine": "true", "maxResults": 1},
            endpoint="channels",
        )

    return check_stream_access(
        "data_api",
        probe_fn=_probe,
        auth_error_types=_AUTH_ERROR_TYPES,
    )


def _check_reporting_api_access(client) -> bool:
    """
    Probes the YouTube Reporting API by listing jobs with pageSize=1.
    Returns True if accessible, False on 401/403.
    """
    def _probe():
        client.get(
            url=client.reporting_url,
            path="jobs",
            params={"pageSize": 1},
            endpoint="reporting_jobs",
        )

    return check_stream_access(
        "reporting_api",
        probe_fn=_probe,
        auth_error_types=_AUTH_ERROR_TYPES,
    )


def _prune_inaccessible_children(schemas: dict, field_metadata: dict) -> None:
    """Drop child streams whose parent stream is no longer in `schemas`."""
    for stream_name in list(schemas.keys()):
        mdata_map = metadata.to_map(field_metadata.get(stream_name, []))
        parent_stream = mdata_map.get((), {}).get("parent-tap-stream-id")
        if parent_stream and parent_stream not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                parent_stream,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict) -> None:
    """Apply API access checks and prune inaccessible streams in place."""
    data_api_accessible = _check_data_api_access(client)
    reporting_api_accessible = _check_reporting_api_access(client)

    inaccessible_streams = [
        stream_name
        for stream_name in list(schemas.keys())
        if (stream_name in DATA_API_STREAMS and not data_api_accessible)
        or (stream_name not in DATA_API_STREAMS and not reporting_api_accessible)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if not schemas:
        raise YoutubeAnalyticsNoAccessibleStreamsError(
            "HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams. Please re-check configuration."
        )
    if inaccessible_streams:
        LOGGER.warning(
            "Unauthorized streams excluded from catalog: %s",
            ", ".join(sorted(inaccessible_streams)),
        )


def discover(client) -> Catalog:
    """Run discovery, filter inaccessible streams, and return the catalog."""
    schemas, field_metadata = get_schemas()
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error(f"stream_name: {stream_name}")
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
