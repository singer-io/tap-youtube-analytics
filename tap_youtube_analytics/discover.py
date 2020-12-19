from singer.catalog import Catalog, CatalogEntry, Schema
from tap_youtube_analytics.schema import get_schemas
from tap_youtube_analytics.streams import STREAMS

def discover(client):
    schemas, field_metadata = get_schemas(client)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]
        key_properties = STREAMS.get(stream_name, {}).get( 
            'key_properties', ['dimensions_hash_key', 'date'])

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog
