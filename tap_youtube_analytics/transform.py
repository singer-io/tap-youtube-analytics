import hashlib
import json
import singer
import humps

from tap_youtube_analytics.schema import get_abs_path

LOGGER = singer.get_logger()


# PyHumps: camelCase to snake_case
# Reference: https://github.com/nficano/humps
def transform_data_record(record):
    new_record = humps.decamelize(record.copy())

    # denest published_at
    published_at = new_record.get('snippet', {}).get('published_at')
    new_record['published_at'] = published_at

    return new_record


# Create MD5 hash key for data element
def hash_data(data):
    # Prepare the project id hash
    hash_id = hashlib.md5()
    hash_id.update(repr(data).encode('utf-8'))

    return hash_id.hexdigest()


# dim_lookup_map.json: code to description mapping dictionary for each dimension
# Created from Dimensions lookup tables here:
#   https://developers.google.com/youtube/reporting/v1/reports/dimensions#Annotation_Dimensions
# Google Sheet for creating/maintaining dim_lookup_map.json:
#   https://docs.google.com/spreadsheets/d/1qR1kCiqwcvkZL4z9e0hxWa1kokesCSRv4LLyPmnnuUI/edit?usp=sharing
def transform_report_record(record, dimensions, report):

    new_record = record.copy()

    map_path = get_abs_path('dim_lookup_map.json')
    with open(map_path) as file:
        dim_lookup_map = json.load(file)

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
                LOGGER.warning('dim_lookup_map value not found; key: {}, value: {}'.format(key, val))
            new_record[key] = new_val
        else:
            new_record[key] = val

    # Add report fields to data
    new_record['report_id'] = report.get('id')
    new_record['report_type_id'] = report.get('reportTypeId')
    new_record['report_name'] = report.get('name')
    new_record['create_time'] = report.get('createTime')

    # Create unique md5 hash key for dimension_values
    dims_md5 = str(hash_data(json.dumps(dimension_values, sort_keys=True)))
    new_record['dimensions_hash_key'] = dims_md5

    return new_record
