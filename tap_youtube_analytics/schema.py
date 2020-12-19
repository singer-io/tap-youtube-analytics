import os
import json
import singer
from singer import metadata
from tap_youtube_analytics.streams import STREAMS, REPORTS

LOGGER = singer.get_logger()

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas(client):
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )

        field_metadata[stream_name] = mdata

    # Limit report endpoints to those available to the account
    endpoint = 'report_types'
    report_type_data = client.get(
        url='https://youtubereporting.googleapis.com/v1',
        path='reportTypes',
        endpoint=endpoint
    )
    report_types = report_type_data.get('reportTypes', [])

    for report_type in report_types:
        # report_name = report id minus the version (last 3 chars)
        report_name = report_type.get('id')[:-3]
        report_metadata = REPORTS.get(report_name, {})

        schema_path = get_abs_path('schemas/reports.json')
        with open(schema_path) as file:
            schema = json.load(file)

        # dimensions, metrics, keys lists
        dimensions = report_metadata.get('dimensions', [])
        metrics = report_metadata.get('metrics', [])
        key_properties = ['dimensions_hash_key']
        report_fields = ['report_id', 'report_type_id', 'report_name', 'create_time']
        combined_list = [*dimensions, *metrics, *key_properties, *report_fields]

        # remove keys not in combined_list
        remove = [key for key in schema['properties'] if key not in combined_list]
        for key in remove:
            del schema['properties'][key]

        schemas[report_name] = schema
        mdata = metadata.new()

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=key_properties,
            valid_replication_keys=['create_time'],
            replication_method='INCREMENTAL'
        )

        # Set dimensions and create_time (bookmark) as automatic inclusion
        mdata_map = metadata.to_map(mdata)
        for dimension in dimensions:
            mdata_map[('properties', dimension)]['inclusion'] = 'automatic'
        mdata_map[('properties', 'create_time')]['inclusion'] = 'automatic'
        mdata = metadata.to_list(mdata_map)

        field_metadata[report_name] = mdata

    return schemas, field_metadata
