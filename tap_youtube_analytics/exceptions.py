class YoutubeAnalyticsError(Exception):
    """Class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class YoutubeAnalyticsBackoffError(YoutubeAnalyticsError):
    """Class representing backoff error handling."""
    pass

class YoutubeAnalyticsBadRequestError(YoutubeAnalyticsError):
    """Class representing 400 status code."""
    pass

class YoutubeAnalyticsUnauthorizedError(YoutubeAnalyticsError):
    """Class representing 401 status code."""
    pass


class YoutubeAnalyticsForbiddenError(YoutubeAnalyticsError):
    """Class representing 403 status code."""
    pass

class YoutubeAnalyticsNotFoundError(YoutubeAnalyticsError):
    """Class representing 404 status code."""
    pass

class YoutubeAnalyticsConflictError(YoutubeAnalyticsError):
    """Class representing 406 status code."""
    pass

class YoutubeAnalyticsUnprocessableEntityError(YoutubeAnalyticsBackoffError):
    """Class representing 409 status code."""
    pass

class YoutubeAnalyticsRateLimitError(YoutubeAnalyticsBackoffError):
    """Class representing 429 status code."""
    pass

class YoutubeAnalyticsInternalServerError(YoutubeAnalyticsBackoffError):
    """Class representing 500 status code."""
    pass

class YoutubeAnalyticsNotImplementedError(YoutubeAnalyticsBackoffError):
    """Class representing 501 status code."""
    pass

class YoutubeAnalyticsBadGatewayError(YoutubeAnalyticsBackoffError):
    """Class representing 502 status code."""
    pass

class YoutubeAnalyticsServiceUnavailableError(YoutubeAnalyticsBackoffError):
    """Class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": YoutubeAnalyticsBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": YoutubeAnalyticsUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": YoutubeAnalyticsForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": YoutubeAnalyticsNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": YoutubeAnalyticsConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": YoutubeAnalyticsUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": YoutubeAnalyticsRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": YoutubeAnalyticsInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": YoutubeAnalyticsNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": YoutubeAnalyticsBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": YoutubeAnalyticsServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
