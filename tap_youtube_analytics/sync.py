from datetime import timedelta
import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from tap_youtube_analytics.transform import transform_data_record, transform_report_record
from tap_youtube_analytics.streams import STREAMS, REPORTS
from tap_youtube_analytics.client import get_paginated_data

LOGGER = singer.get_logger()

REPORTING_URL = 'https://youtubereporting.googleapis.com/v1'
DATA_URL = 'https://www.googleapis.com/youtube/v3'

# YouTube provides daily estimated metrics at a 2-3 day lag
# Attribution window set to 7 days to ensure last 7 days are re-synced daily
ATTRIBUTION_DAYS = 7
DATE_WINDOW_SIZE = 30

def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
    except OSError as err:
        LOGGER.error('OS Error writing schema for: %s', stream_name)
        raise err


def write_record(stream_name, record, time_extracted):
    try:
        singer.messages.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.error('OS Error writing record for: %s', stream_name)
        LOGGER.error('Stream: %s, record: %s', stream_name, record)
        raise err
    except TypeError as err:
        LOGGER.error('Type Error writing record for: %s', stream_name)
        LOGGER.error('Stream: %s, record: %s', stream_name, record)
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: %s, value: %s', stream, value)
    singer.write_state(state)


#pylint: disable=protected-access
def transform_datetime(this_dttm):
    with Transformer() as transformer:
        new_dttm = transformer._transform_datetime(this_dttm)
    return new_dttm


# Chunk large array to smaller arrays
# lst: List
# cnt: Chunk Size Count
def chunks(lst, cnt):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), cnt):
        yield lst[i:i + cnt]


def sync_channels(client,
                  catalog,
                  channel_ids,
                  endpoint_config):

    stream_name = 'channels'
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    id_fields = endpoint_config.get('key_properties', 'id')
    params = endpoint_config.get('params', {})
    params['id'] = channel_ids

    records = get_paginated_data(
        client=client,
        url=DATA_URL,
        path=stream_name,
        endpoint=stream_name,
        params=params,
        data_key='items'
    )
    time_extracted = utils.now()

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            for key in id_fields:
                if not record.get(key):
                    raise ValueError('Stream: {}, Missing key: {}'.format(stream_name, key))

            with Transformer() as transformer:
                try:
                    transformed_record = transformer.transform(
                        transform_data_record(record),
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error('Transformer Error: %s', err)
                    LOGGER.error('Stream: %s, record: %s', stream_name, record)
                    raise err

                write_record(stream_name, transformed_record, time_extracted=time_extracted)
                counter.increment()

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return counter.value


def sync_playlists(client,
                   catalog,
                   channel_ids,
                   endpoint_config):

    stream_name = 'playlists'
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    id_fields = endpoint_config.get('key_properties', 'id')
    params = endpoint_config.get('params', {})
    channel_list = channel_ids.split(',')

    with metrics.record_counter(stream_name) as counter:
        # Loop each channel_id from config
        for channel_id in channel_list:
            params['channelId'] = channel_id
            records = get_paginated_data(
                client=client,
                url=DATA_URL,
                path=stream_name,
                endpoint=stream_name,
                params=params,
                data_key='items'
            )
            time_extracted = utils.now()

            for record in records:
                for key in id_fields:
                    if not record.get(key):
                        raise ValueError('Stream: {}, Missing key: {}'.format(stream_name, key))

                with Transformer() as transformer:
                    try:
                        transformed_record = transformer.transform(
                            transform_data_record(record),
                            schema,
                            stream_metadata)
                    except Exception as err:
                        LOGGER.error('Transformer Error: %s', err)
                        LOGGER.error('Stream: %s, record: %s', stream_name, record)
                        raise err

                    write_record(stream_name, transformed_record, time_extracted=time_extracted)
                    counter.increment()

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return counter.value


def sync_playlist_items(client,
                        catalog,
                        state,
                        start_date,
                        channel_ids,
                        endpoint_config):

    stream_name = 'playlist_items'
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    id_fields = endpoint_config.get('key_properties', 'id')
    bookmark_field = next(iter(endpoint_config.get('replication_keys', [])), None)
    params = endpoint_config.get('params', {})
    playlist_params = STREAMS.get('playlists', {}).get('params', {})
    channel_list = channel_ids.split(',')

    # Initialize bookmarking
    last_datetime = get_bookmark(state, stream_name, start_date)
    last_dttm = strptime_to_utc(last_datetime)
    max_bookmark_value = last_datetime

    with metrics.record_counter(stream_name) as counter:
        # Loop each channel_id from config
        for channel_id in channel_list:
            playlist_params['channelId'] = channel_id
            playlists = get_paginated_data(
                client=client,
                url=DATA_URL,
                path='playlists',
                endpoint='playlists',
                params=playlist_params,
                data_key='items'
            )

            # Loop playlists
            for playlist in playlists:
                playlist_id = playlist.get('id')
                params['playlistId'] = playlist_id
                records = get_paginated_data(
                    client=client,
                    url=DATA_URL,
                    path='playlistItems',
                    endpoint=stream_name,
                    params=params,
                    data_key='items'
                )
                time_extracted = utils.now()

                for record in records:
                    if record is None:
                        continue
                    for key in id_fields:
                        if not record.get(key):
                            raise ValueError('Stream: {}, Missing key: {}'.format(stream_name, key))

                    with Transformer() as transformer:
                        try:
                            transformed_record = transformer.transform(
                                transform_data_record(record),
                                schema,
                                stream_metadata)
                        except Exception as err:
                            LOGGER.error('Transformer Error: %s', err)
                            LOGGER.error('Stream: %s, record: %s', stream_name, record)
                            raise err

                        # Bookmarking
                        bookmark_date = transformed_record.get(bookmark_field)
                        bookmark_dttm = strptime_to_utc(bookmark_date)
                        max_bookmark_dttm = strptime_to_utc(max_bookmark_value)
                        if bookmark_dttm > max_bookmark_dttm:
                            max_bookmark_value = strftime(bookmark_dttm)

                        # Only sync records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, \
                                time_extracted=time_extracted)
                            counter.increment()

        # Youtube API does not allow page/batch sorting for playlist_items
        write_bookmark(state, stream_name, max_bookmark_value)

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return counter.value


def sync_videos(client,
                catalog,
                state,
                start_date,
                channel_ids,
                endpoint_config):

    stream_name = 'videos'
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    id_fields = endpoint_config.get('key_properties', 'id')
    params = endpoint_config.get('params', {})
    channel_list = channel_ids.split(',')

    # Initialize bookmarking
    last_datetime = get_bookmark(state, stream_name, start_date)
    last_dttm = strptime_to_utc(last_datetime)
    max_bookmark_value = last_datetime

    search_params = {
        'part': 'id,snippet',
        'channelId': '{channel_id}',
        'order': 'date', # Descending date order
        'type': 'video',
        'maxResults': 50
    }

    with metrics.record_counter(stream_name) as counter:
        # Loop each channel_id from config
        for channel_id in channel_list:
            video_ids = []
            search_params['channelId'] = channel_id
            search_records = get_paginated_data(
                client=client,
                url=DATA_URL,
                path='search',
                endpoint='search_videos',
                params=search_params,
                data_key='items'
            )

            i = 0
            for search_record in search_records:
                if search_record is None:
                    continue
                video_id = search_record.get('id', {}).get('videoId')
                video_ids.append(video_id)

                # Bookmarking
                bookmark_date = search_record.get('snippet', {}).get('publishedAt')
                bookmark_dttm = strptime_to_utc(bookmark_date)
                if i == 0:
                    max_bookmark_value = bookmark_date
                # Stop looping when bookmark is before last datetime
                if bookmark_dttm < last_dttm:
                    break
                i = i + 1

            # Break into chunks of 50 video_ids
            video_id_chunks = chunks(video_ids, 50)

            # Loop through chunks
            for video_id_chunk in video_id_chunks:
                video_ids_str = ','.join(video_id_chunk)
                params['id'] = video_ids_str
                records = get_paginated_data(
                    client=client,
                    url=DATA_URL,
                    path='videos',
                    endpoint='videos',
                    params=params,
                    data_key='items'
                )
                time_extracted = utils.now()

                for record in records:
                    for key in id_fields:
                        if not record.get(key):
                            raise ValueError('Stream: {}, Missing key: {}'.format(stream_name, key))

                    with Transformer() as transformer:
                        try:
                            transformed_record = transformer.transform(
                                transform_data_record(record),
                                schema,
                                stream_metadata)
                        except Exception as err:
                            LOGGER.error('Transformer Error: %s', err)
                            LOGGER.error('Stream: %s, record: %s', stream_name, record)
                            raise err

                        write_record(stream_name, transformed_record, time_extracted=time_extracted)
                        counter.increment()

        # Write bookmark after all records synced due to sort descending (most recent first)
        write_bookmark(state, stream_name, max_bookmark_value)

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return counter.value


#pylint: disable=too-many-statements
def sync_report(client,
                catalog,
                state,
                start_date,
                stream_name,
                endpoint_config):

    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)

    report_type = endpoint_config.get('report_type')
    dimensions = endpoint_config.get('dimensions', [])

    # Initialize bookmarking
    # There is a 2-3 day lag (sometimes up to 6-7 days lag) in YouTube results reconcilliation
    now_dttm = utils.now()
    attribution_start_dttm = now_dttm - timedelta(days=ATTRIBUTION_DAYS)
    last_datetime = get_bookmark(state, stream_name, start_date)
    last_dttm = strptime_to_utc(last_datetime)

    if attribution_start_dttm < last_dttm:
        last_dttm = attribution_start_dttm
        last_datetime = strftime(last_dttm)

    max_bookmark_value = last_datetime

    with metrics.record_counter(stream_name) as counter:
        job_id = None
        job_params = {
            'includeSystemManaged': 'true',
            'pageSize': 50
        }

        jobs = get_paginated_data(
            client=client,
            url=REPORTING_URL,
            path='jobs',
            endpoint='jobs',
            params=job_params,
            data_key='jobs'
        )

        # Check if job exists for stream
        job_exists = False
        for job in jobs:
            job_report_id = job.get('reportTypeId')
            if job_report_id == report_type:
                job_exists = True
                job_id = job.get('id')
                break

        # Create job for stream if not job_exists
        if not job_exists:
            body = {
                'name': stream_name,
                'reportTypeId': report_type
            }
            new_job = {}
            new_job = client.post(
                url=REPORTING_URL,
                path='jobs',
                data=body,
                endpoint='job_create'
            )
            job_name = new_job.get('name')
            job_id = new_job.get('id')

        # Get reports for job_id created after bookmark last_datetime
        report_params = {
            'createdAfter': last_datetime,
            'startTimeAtOrAfter': start_date,
            'pageSize': 50
        }
        reports = get_paginated_data(
            client=client,
            url=REPORTING_URL,
            path='jobs/{}/reports'.format(job_id),
            endpoint='reports',
            params=report_params,
            data_key='reports'
        )

        for report in reports:
            if report:
                download_url = report.get('downloadUrl')

                # Get report csv records to json
                records = client.get_report(
                    url=download_url,
                    endpoint='report_download')
                time_extracted = utils.now()

                for record in records:
                    for key in dimensions:
                        if record.get(key) is None:
                            err = 'Stream: {}, Missing key: {}, Dimensions: {}, Record: {}'.format(
                                stream_name, key, dimensions, record)
                            raise ValueError(err)

                    with Transformer() as transformer:
                        try:
                            transformed_record = transformer.transform(
                                transform_report_record(record, dimensions, report),
                                schema,
                                stream_metadata)
                        except Exception as err:
                            LOGGER.error('Transformer Error: %s', err)
                            LOGGER.error('Stream: %s, record: %s', stream_name, record)
                            raise err

                        # Bookmarking
                        bookmark_date = transformed_record.get('create_time')
                        bookmark_dttm = strptime_to_utc(bookmark_date)
                        max_bookmark_dttm = strptime_to_utc(max_bookmark_value)
                        if bookmark_dttm > max_bookmark_dttm:
                            max_bookmark_value = strftime(bookmark_dttm)

                        # Only sync records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            write_record(stream_name, transformed_record, time_extracted=time_extracted)
                            counter.increment()

        # Write bookmark after all records synced due to sort descending (most recent first)
        write_bookmark(state, stream_name, max_bookmark_value)

        LOGGER.info('Stream: {}, Processed {} records'.format(stream_name, counter.value))
        return counter.value


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, catalog, state):
    start_date = config.get('start_date')
    channel_ids = config.get('channel_ids').replace(" ", "")

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams or selected_streams == []:
        return

    # Loop through selected_streams
    for stream_name in selected_streams:
        LOGGER.info('STARTED Syncing: {}'.format(stream_name))
        update_currently_syncing(state, stream_name)
        write_schema(catalog, stream_name)

        endpoint_config = {}
        total_records = 0

        if stream_name in STREAMS:
            endpoint_config = STREAMS[stream_name]

            if stream_name == 'channels':
                total_records = sync_channels(client=client,
                                              catalog=catalog,
                                              channel_ids=channel_ids,
                                              endpoint_config=endpoint_config)

            elif stream_name == 'playlists':
                total_records = sync_playlists(client=client,
                                               catalog=catalog,
                                               channel_ids=channel_ids,
                                               endpoint_config=endpoint_config)

            elif stream_name == 'playlist_items':
                total_records = sync_playlist_items(client=client,
                                                    catalog=catalog,
                                                    state=state,
                                                    start_date=start_date,
                                                    channel_ids=channel_ids,
                                                    endpoint_config=endpoint_config)

            elif stream_name == 'videos':
                total_records = sync_videos(client=client,
                                            catalog=catalog,
                                            state=state,
                                            start_date=start_date,
                                            channel_ids=channel_ids,
                                            endpoint_config=endpoint_config)

        elif stream_name in REPORTS:
            endpoint_config = REPORTS[stream_name]
            total_records = sync_report(client=client,
                                        catalog=catalog,
                                        state=state,
                                        start_date=start_date,
                                        stream_name=stream_name,
                                        endpoint_config=endpoint_config)

        LOGGER.info('FINISHED Syncing Stream: {}'.format(stream_name))
        LOGGER.info('  Records Synced for Stream: {}'.format(total_records))
        update_currently_syncing(state, None)
        # End stream loop
