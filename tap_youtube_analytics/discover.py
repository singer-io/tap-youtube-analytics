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


def check_stream_access(stream_name, probe_fn, auth_error_types, fallback_accessible=False):
    """
    Probe a stream endpoint and return True if accessible, False on auth error.

    :param stream_name: Used in log messages.
    :param probe_fn: Zero-argument callable that performs the API probe.
    :param auth_error_types: Exception type(s) indicating 401/403 — returns False.
    :param fallback_accessible: If True, non-auth errors (e.g. 400 from minimal probe
                                params) are treated as auth-OK and return True.
                                If False (default), they are re-raised.
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
    except Exception:  # pylint: disable=broad-except
        if fallback_accessible:
            LOGGER.info("Stream '%s' endpoint reachable (auth OK).", stream_name)
            return True
        raise


def _check_data_api_access(client) -> bool:
    """
    Probes the YouTube Data API v3 by requesting the authenticated user's channel
    list with minimal parameters. Returns True if accessible, False on 401/403.

    Uses fallback_accessible=True because the YouTube Data API may return 400 for
    some OAuth scopes that don't include 'mine=true' access (e.g. service accounts),
    but a 400 still confirms the token is valid and the API is reachable.
    """
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
        fallback_accessible=True,
    )


def _check_reporting_api_access(client) -> bool:
    """
    Probes the YouTube Reporting API by listing jobs with maxResults=1.
    Returns True if accessible, False on 401/403.
    """
    def _probe():
        client.get(
            url=client.reporting_url,
            path="jobs",
            params={"maxResults": 1},
            endpoint="reporting_jobs",
        )

    return check_stream_access(
        "reporting_api",
        probe_fn=_probe,
        auth_error_types=_AUTH_ERROR_TYPES,
        fallback_accessible=False,
    )


def discover(client) -> Catalog:
    """Run the discovery mode, probe API access, and return the catalog.

    Two API families are probed:
      - YouTube Data API v3  → covers channels, playlists, playlist_items, videos
      - YouTube Reporting API → covers all report streams

    Streams belonging to an inaccessible API family are excluded from the catalog.
    Raises YoutubeAnalyticsNoAccessibleStreamsError if no streams pass the access check.
    """
    data_api_accessible = _check_data_api_access(client)
    reporting_api_accessible = _check_reporting_api_access(client)

    if not data_api_accessible:
        LOGGER.warning(
            "YouTube Data API is not accessible. Streams %s will be excluded from the catalog.",
            sorted(DATA_API_STREAMS),
        )
    if not reporting_api_accessible:
        LOGGER.warning(
            "YouTube Reporting API is not accessible. All report streams will be excluded from the catalog."
        )

    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        is_data_api_stream = stream_name in DATA_API_STREAMS

        if is_data_api_stream and not data_api_accessible:
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
            continue

        if not is_data_api_stream and not reporting_api_accessible:
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )
            continue

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

    if not catalog.streams:
        raise YoutubeAnalyticsNoAccessibleStreamsError(
            "No stream endpoints are accessible with the provided credentials. "
            "Verify that the OAuth token has the required YouTube API scopes."
        )

    return catalog
