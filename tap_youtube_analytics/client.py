import codecs
import csv
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Dict, Mapping, Optional, Tuple, Iterator

import backoff
import requests
from requests import session
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from singer import get_logger, metrics

from tap_youtube_analytics.exceptions import ERROR_CODE_EXCEPTION_MAPPING, YoutubeAnalyticsError, YoutubeAnalyticsBackoffError, YoutubeAnalyticsRateLimitError

LOGGER = get_logger()
REQUEST_TIMEOUT = 300

def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            message = "HTTP-error-code: {}, Error: {}".format(
                response.status_code,
                response_json.get("message", ERROR_CODE_EXCEPTION_MAPPING.get(
                    response.status_code, {}).get("message", "Unknown Error")))
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(
            response.status_code, {}).get("raise_exception", YoutubeAnalyticsError)
        raise exc(message, response) from None

class Client:
    """A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self.__access_token = None
        self.__expires = None
        self._session = session()
        self.base_url = "https://www.googleapis.com/youtube/v3"
        self.google_token_uri = "https://oauth2.googleapis.com/token"
        self.reporting_url = "https://youtubereporting.googleapis.com/v1"


        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    @backoff.on_exception(backoff.expo,
                        YoutubeAnalyticsBackoffError,
                        max_tries=5,
                        factor=2)
    def check_api_credentials(self) -> None:
        if self.__access_token is not None and self.__expires > datetime.now(timezone.utc):
            return

        headers = {}
        if self.config["user_agent"]:
            headers["User-Agent"] = self.config["user_agent"]

        response = self._session.post(
            url=self.google_token_uri,
            headers=headers,
            data={
                "grant_type": "refresh_token",
                "client_id": self.config["client_id"],
                "client_secret": self.config["client_secret"],
                "refresh_token": self.config["refresh_token"],
            })

        if response.status_code >= 500:
            raise YoutubeAnalyticsBackoffError()

        if response.status_code != 200:
            raise_for_error(response)

        data = response.json()
        self.__access_token = data["access_token"]
        # Make this timezone-aware as well
        # Use timezone-aware datetime to ensure correct comparison and avoid bugs from mixing naive and aware datetimes.
        self.__expires = datetime.now(timezone.utc) + timedelta(seconds=data["expires_in"])
        LOGGER.info(f"Authorized, token expires = {self.__expires}")

    def get(self, path=None, url=None, **kwargs):
        """Calls the make_request method with a prefixed method type `GET`"""
        return self.__make_request("GET", path=path, url=url, **kwargs)

    def post(self, path=None, url=None, **kwargs):
        """Calls the make_request method with a prefixed method type `POST`"""
        return self.__make_request("POST", path=path, url=url, **kwargs)

    def get_raw(self, url=None, **kwargs):
        """Get raw response without JSON parsing for CSV downloads"""
        return self.__make_request_raw("GET", url=url, **kwargs)

    def get_report(self, url: str, **kwargs) -> Iterator[Dict[str, Any]]:
        """Download and stream CSV report rows as dictionaries."""
        self.check_api_credentials()

        endpoint = kwargs.pop("endpoint", None)

        headers = kwargs.setdefault("headers", {})
        headers["Authorization"] = f"Bearer {self.__access_token}"
        if self.config.get("user_agent"):
            headers["User-Agent"] = self.config["user_agent"]

        kwargs.setdefault("stream", True)

        def _row_iterator() -> Iterator[Dict[str, Any]]:
            with metrics.http_request_timer(endpoint) as timer:
                with self._session.request(
                    "GET",
                    url,
                    timeout=self.request_timeout,
                    **kwargs,
                ) as response:
                    timer.tags[metrics.Tag.http_status_code] = response.status_code

                    if response.status_code >= 500:
                        raise YoutubeAnalyticsBackoffError()

                    if response.status_code == 429:
                        raise YoutubeAnalyticsRateLimitError()

                    if response.status_code != 200:
                        raise_for_error(response)

                    reader = csv.DictReader(
                        codecs.iterdecode(response.iter_lines(), encoding="utf-8"),
                        delimiter=",",
                    )

                    for row in reader:
                        if row:
                            yield row

        return _row_iterator()

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            YoutubeAnalyticsBackoffError
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request_raw(self, method: str, url=None, **kwargs) -> Optional[str]:
        """Performs HTTP Operations for raw data (like CSV)"""
        self.check_api_credentials()

        if "endpoint" in kwargs:
            endpoint = kwargs["endpoint"]
            del kwargs["endpoint"]
        else:
            endpoint = None

        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.__access_token}"

        if self.config["user_agent"]:
            kwargs["headers"]["User-Agent"] = self.config["user_agent"]

        with metrics.http_request_timer(endpoint) as timer:
            response = self._session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise YoutubeAnalyticsBackoffError()

        if response.status_code == 429:
            raise YoutubeAnalyticsRateLimitError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.text

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            YoutubeAnalyticsBackoffError
        ),
        max_tries=5,
        factor=2,
    )
    def __make_request(self, method: str, path=None, url=None, **kwargs) -> Optional[Mapping[Any, Any]]:
        """Performs HTTP Operations
        Args:
            method (str): represents the state file for the tap.
            endpoint (str): url of the resource that needs to be fetched
            params (dict): A mapping for url params eg: ?name=Avery&age=3
            headers (dict): A mapping for the headers that need to be sent
            body (dict): only applicable to post request, body of the request

        Returns:
            Dict,List,None: Returns a `Json Parsed` HTTP Response or None if exception
        """
        self.check_api_credentials()

        if url and path:
            url = f"{url}/{path}"

        if not url and path:
            url = f"{self.base_url}/{path}"

        if "endpoint" in kwargs:
            endpoint = kwargs["endpoint"]
            del kwargs["endpoint"]
        else:
            endpoint = None

        if "headers" not in kwargs:
            kwargs["headers"] = {}
        kwargs["headers"]["Authorization"] = f"Bearer {self.__access_token}"

        if self.config["user_agent"]:
            kwargs["headers"]["User-Agent"] = self.config["user_agent"]

        if method == "POST":
            kwargs["headers"]["Content-Type"] = "application/json"

        if kwargs.get("data"):
            kwargs["data"] = json.dumps(kwargs["data"])

        with metrics.http_request_timer(endpoint) as timer:
            response = self._session.request(method, url, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise YoutubeAnalyticsBackoffError()

        #Use retry functionality in backoff to wait and retry if
        #response code equals 429 because rate limit has been exceeded
        if response.status_code == 429:
            raise YoutubeAnalyticsRateLimitError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()
