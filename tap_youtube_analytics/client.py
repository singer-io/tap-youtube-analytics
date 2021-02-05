import codecs
import csv
import json
from datetime import datetime, timedelta

import backoff
import requests
import singer
from singer import metrics, utils

BASE_URL = 'https://www.googleapis.com/youtube/v3'
GOOGLE_TOKEN_URI = 'https://oauth2.googleapis.com/token'
LOGGER = singer.get_logger()


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class GoogleError(Exception):
    pass


class GoogleBadRequestError(GoogleError):
    pass


class GoogleUnauthorizedError(GoogleError):
    pass


class GooglePaymentRequiredError(GoogleError):
    pass


class GoogleNotFoundError(GoogleError):
    pass


class GoogleMethodNotAllowedError(GoogleError):
    pass


class GoogleConflictError(GoogleError):
    pass


class GoogleGoneError(GoogleError):
    pass


class GooglePreconditionFailedError(GoogleError):
    pass


class GoogleRequestEntityTooLargeError(GoogleError):
    pass


class GoogleRequestedRangeNotSatisfiableError(GoogleError):
    pass


class GoogleExpectationFailedError(GoogleError):
    pass


class GoogleForbiddenError(GoogleError):
    pass


class GoogleUnprocessableEntityError(GoogleError):
    pass


class GooglePreconditionRequiredError(GoogleError):
    pass


class GoogleInternalServiceError(GoogleError):
    pass


# Error Codes: https://developers.google.com/webmaster-tools/search-console-api-original/v3/errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: GoogleBadRequestError,
    401: GoogleUnauthorizedError,
    402: GooglePaymentRequiredError,
    403: GoogleForbiddenError,
    404: GoogleNotFoundError,
    405: GoogleMethodNotAllowedError,
    409: GoogleConflictError,
    410: GoogleGoneError,
    412: GooglePreconditionFailedError,
    413: GoogleRequestEntityTooLargeError,
    416: GoogleRequestedRangeNotSatisfiableError,
    417: GoogleExpectationFailedError,
    422: GoogleUnprocessableEntityError,
    428: GooglePreconditionRequiredError,
    500: GoogleInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, GoogleError)

def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Google has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('error', {}).get('code')
                ex = get_exception_for_error_code(error_code)
                raise ex(message)
            else:
                raise GoogleError(error)
        except (ValueError, TypeError):
            raise GoogleError(error)

# Pagination loop for API calls to yield records
def get_paginated_data(client, url, path, endpoint, params, data_key='items'):
    total_count = 0
    page = 1
    is_next_page = True
    page_token = ''

    while is_next_page:
        if page > 1:
            params['pageToken'] = page_token

        # Squash params to query-string params for URL
        querystring = None
        if params.items():
            querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
        LOGGER.info('Endpoint: {}, URL: {}/{}{}'.format(
            endpoint,
            url,
            path,
            '?{}'.format(querystring) if querystring else ''))

        data = {}
        data = client.get(
            url=url,
            path=path,
            params=params,
            endpoint=endpoint
        )

        if not data or data is None or data == {}:
            LOGGER.info('xxx NO DATA xxx')

        total_results = data.get('pageInfo', {}).get('totalResults')
        results = data.get(data_key, [])
        results_count = len(results)
        from_count = total_count + 1
        total_count = total_count + results_count
        to_count = total_count

        LOGGER.info('Endpoint: {}, Page: {}, Results: {}-{} of Total: {}'.format(
            endpoint,
            page,
            from_count,
            to_count,
            total_results))

        if not results or results is None or results == []:
            yield None

        for result in data.get(data_key, []):
            yield result

        # Pagination: increment the offset by the limit (batch-size)
        page_token = data.get('nextPageToken')
        if page_token is None:
            is_next_page = False
        page = page + 1

class GoogleClient: # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 client_id,
                 client_secret,
                 refresh_token,
                 user_agent=None):
        self.__client_id = client_id
        self.__client_secret = client_secret
        self.__refresh_token = refresh_token
        self.__user_agent = user_agent
        self.__access_token = None
        self.__expires = None
        self.__session = requests.Session()
        self.base_url = None


    def __enter__(self):
        self.get_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=5,
                          factor=2)
    def get_access_token(self):
        # The refresh_token never expires and may be used many times to generate each access_token
        # Since the refresh_token does not expire, it is not included in get access_token response
        if self.__access_token is not None and self.__expires > datetime.utcnow():
            return

        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent

        response = self.__session.post(
            url=GOOGLE_TOKEN_URI,
            headers=headers,
            data={
                'grant_type': 'refresh_token',
                'client_id': self.__client_id,
                'client_secret': self.__client_secret,
                'refresh_token': self.__refresh_token,
            })

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data['access_token']
        self.__expires = datetime.utcnow() + timedelta(seconds=data['expires_in'])
        LOGGER.info('Authorized, token expires = {}'.format(self.__expires))


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    # Rate Limit:
    #  https://developers.google.com/webmaster-tools/search-console-api-original/v3/limits
    @utils.ratelimit(1200, 60)
    def request(self, method, path=None, url=None, **kwargs):

        self.get_access_token()

        if url:
            self.base_url = url
        else:
            self.base_url = BASE_URL

        if url and path:
            url = '{}/{}'.format(url, path)

        if not url and path:
            url = '{}/{}'.format(self.base_url, path)

        # endpoint = stream_name (from sync.py API call)
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        if kwargs.get('data'):
            kwargs['data'] = json.dumps(kwargs['data'])

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        #Use retry functionality in backoff to wait and retry if
        #response code equals 429 because rate limit has been exceeded
        if response.status_code == 429:
            raise Server429Error()

        if response.status_code != 200:
            raise_for_error(response)

        response_json = response.json()
        return response_json

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    def get_report(self, url, **kwargs):

        self.get_access_token()

        # endpoint = stream_name (from sync.py API call)
        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['Authorization'] = 'Bearer {}'.format(self.__access_token)

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        with metrics.http_request_timer(endpoint) as timer:
            try:
                with self.__session.request('GET', url, stream=True, **kwargs) as response:
                    timer.tags[metrics.Tag.http_status_code] = response.status_code

                    if response.status_code >= 500:
                        raise Server5xxError()

                    #Use retry functionality in backoff to wait and retry if
                    #response code equals 429 because rate limit has been exceeded
                    if response.status_code == 429:
                        raise Server429Error()

                    if response.status_code != 200:
                        raise_for_error(response)

                    # Stream CSV results for report_download
                    reader = csv.DictReader(codecs.iterdecode(response.iter_lines(), encoding='utf-8'), delimiter=',')

                    for page in reader:
                        yield page
            except Exception as e:
                LOGGER.info(e)

    def get(self, path=None, url=None, **kwargs):
        return self.request('GET', path=path, url=url, **kwargs)

    def post(self, path=None, url=None, **kwargs):
        return self.request('POST', path=path, url=url, **kwargs)
