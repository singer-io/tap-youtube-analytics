import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
from tap_youtube_analytics.client import Client
from tap_youtube_analytics.exceptions import YoutubeAnalyticsError, YoutubeAnalyticsRateLimitError, YoutubeAnalyticsBackoffError
import requests

class TestClient(unittest.TestCase):
    def setUp(self):
        self.config = {
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
            "user_agent": "test_user_agent",
            "request_timeout": 300
        }
        # Initialize client with mocked authentication to avoid real API calls
        with patch.object(Client, 'check_api_credentials'):
            self.client = Client(self.config)

    @patch("requests.Session.post")
    def test_check_api_credentials_success(self, mock_post):
        """Test successful API credential check"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_access_token",
            "expires_in": 3600
        }
        mock_post.return_value = mock_response

        self.client.check_api_credentials()
        self.assertEqual(self.client._Client__access_token, "test_access_token")
        # Compare with timezone-naive datetime since client uses utcnow()
        current_time = datetime.utcnow()
        self.assertTrue(self.client._Client__expires > current_time)

    @patch("requests.Session.post")
    def test_check_api_credentials_failure(self, mock_post):
        """Test API credential check failure"""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_post.return_value = mock_response

        with self.assertRaises(YoutubeAnalyticsError):
            self.client.check_api_credentials()

    @patch.object(Client, 'check_api_credentials')
    @patch("requests.Session.request")
    def test_make_request_success(self, mock_request, mock_check):
        """Test successful HTTP request"""
        # Set up valid token to avoid authentication calls
        self.client._Client__access_token = "valid_token"
        self.client._Client__expires = datetime.now(timezone.utc) + timedelta(hours=1)
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "test_data"}
        mock_request.return_value = mock_response

        result = self.client.get(path="test_path")
        self.assertEqual(result, {"data": "test_data"})

    @patch('backoff.expo', return_value=0)  # Disable backoff delays
    @patch.object(Client, 'check_api_credentials')
    @patch("requests.Session.request")
    def test_make_request_rate_limit_error(self, mock_request, mock_check, mock_backoff):
        """Test rate limit error handling"""
        # Set up valid token to avoid authentication calls
        self.client._Client__access_token = "valid_token"
        self.client._Client__expires = datetime.now(timezone.utc) + timedelta(hours=1)
        
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.json.return_value = {"error": "Rate limit exceeded"}
        mock_request.return_value = mock_response

        with self.assertRaises(YoutubeAnalyticsRateLimitError):
            self.client.get(path="test_path")

    @patch('backoff.expo', return_value=0)  # Disable backoff delays
    @patch.object(Client, 'check_api_credentials')
    @patch("requests.Session.request")
    def test_make_request_server_error(self, mock_request, mock_check, mock_backoff):
        """Test server error handling"""
        # Set up valid token to avoid authentication calls
        self.client._Client__access_token = "valid_token"
        self.client._Client__expires = datetime.now(timezone.utc) + timedelta(hours=1)
        
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {"error": "Internal server error"}
        mock_request.return_value = mock_response

        with self.assertRaises(YoutubeAnalyticsBackoffError):
            self.client.get(path="test_path")
