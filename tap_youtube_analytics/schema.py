import os
import json
import singer
from typing import Dict, Tuple
from singer import metadata
from tap_youtube_analytics.streams import STREAMS

LOGGER = singer.get_logger()


def get_abs_path(path: str) -> str:
    """Get the absolute path for the schema files."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema_references() -> Dict:
    """Load the shared schema files from the schema/shared folder and return references."""
    shared_schema_path = get_abs_path("schemas/shared")

    shared_file_names = []
    if os.path.exists(shared_schema_path):
        shared_file_names = [
            f
            for f in os.listdir(shared_schema_path)
            if os.path.isfile(os.path.join(shared_schema_path, f))
        ]

    refs = {}
    for shared_schema_file in shared_file_names:
        with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
            refs["shared/" + shared_schema_file] = json.load(data_file)

    return refs


def _load_schema_for_stream(stream_name: str) -> Dict:
    """
    Try to load a schema specific to the stream.
    If it doesn't exist, fall back to schemas/reports.json.
    """
    candidate = get_abs_path(f"schemas/{stream_name}.json")
    fallback = get_abs_path("schemas/reports.json")

    if os.path.exists(candidate):
        path = candidate
    elif os.path.exists(fallback):
        LOGGER.info(
            "Schema for stream '%s' not found at %s. Falling back to %s.",
            stream_name, candidate, fallback
        )
        path = fallback
    else:
        # Be explicit so we don't hide real packaging issues
        raise FileNotFoundError(
            f"No schema file found for stream '{stream_name}'. "
            f"Tried: {candidate} and fallback: {fallback}"
        )

    with open(path) as f:
        return json.load(f)


def get_schemas() -> Tuple[Dict, Dict]:
    """
    Load the schema references, prepare metadata for each stream,
    and return (schemas, field_metadata) for the catalog.
    """
    schemas: Dict[str, Dict] = {}
    field_metadata: Dict[str, Dict] = {}

    refs = load_schema_references()

    for stream_name, stream_obj in STREAMS.items():
        # Load per-stream schema or fallback to reports.json
        raw_schema = _load_schema_for_stream(stream_name)

        # Resolve $ref entries against shared refs
        schema = singer.resolve_schema_references(raw_schema, refs)

        # Save the resolved schema
        schemas[stream_name] = schema

        # Build Singer metadata
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=getattr(stream_obj, "key_properties", []),
            valid_replication_keys=(getattr(stream_obj, "replication_keys", []) or []),
            replication_method=getattr(stream_obj, "replication_method", None),
        )
        m_map = metadata.to_map(mdata)

        # Mark replication keys as automatic
        automatic_keys = getattr(stream_obj, "replication_keys", []) or []
        for field_name in schema.get("properties", {}).keys():
            if field_name in automatic_keys:
                m_map = metadata.write(
                    m_map, ("properties", field_name), "inclusion", "automatic"
                )

        field_metadata[stream_name] = metadata.to_list(m_map)

    return schemas, field_metadata
