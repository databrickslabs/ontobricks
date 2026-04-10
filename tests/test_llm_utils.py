"""Tests for LLM utility functions."""
import pytest
from unittest.mock import patch, MagicMock
import requests
from agents.llm_utils import (
    call_llm_with_retry,
    _get_retry_delay,
    _RATE_LIMIT_RETRIES,
    _RATE_LIMIT_BASE_DELAY,
    _RATE_LIMIT_MAX_DELAY,
)


def _make_resp(status_code, content=b'ok', headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    resp.headers = headers or {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(response=resp)
    else:
        resp.raise_for_status = MagicMock()
    return resp


class TestGetRetryDelay:
    def test_exponential_backoff(self):
        assert _get_retry_delay(1) == _RATE_LIMIT_BASE_DELAY
        assert _get_retry_delay(2) == _RATE_LIMIT_BASE_DELAY * 2
        assert _get_retry_delay(3) == _RATE_LIMIT_BASE_DELAY * 4

    def test_capped_at_max(self):
        assert _get_retry_delay(20) == _RATE_LIMIT_MAX_DELAY

    def test_retry_after_header_honoured(self):
        resp = _make_resp(429, headers={"Retry-After": "15"})
        assert _get_retry_delay(1, resp) == 15.0

    def test_retry_after_header_capped(self):
        resp = _make_resp(429, headers={"Retry-After": "999"})
        assert _get_retry_delay(1, resp) == _RATE_LIMIT_MAX_DELAY

    def test_retry_after_header_invalid_ignored(self):
        resp = _make_resp(429, headers={"Retry-After": "not-a-number"})
        assert _get_retry_delay(1, resp) == _RATE_LIMIT_BASE_DELAY


class TestCallLlmWithRetry:
    @patch('agents.llm_utils.requests.post')
    def test_success_first_attempt(self, mock_post):
        mock_post.return_value = _make_resp(200, b'{"result": "ok"}')

        resp = call_llm_with_retry('http://llm/api', {'Authorization': 'Bearer x'}, {'prompt': 'hi'}, timeout=10)
        assert resp.status_code == 200
        mock_post.assert_called_once()

    @patch('agents.llm_utils.time.sleep')
    @patch('agents.llm_utils.requests.post')
    def test_retry_on_429(self, mock_post, mock_sleep):
        mock_post.side_effect = [
            _make_resp(429, b'rate limited'),
            _make_resp(200, b'ok'),
        ]

        resp = call_llm_with_retry('http://llm/api', {}, {}, timeout=10)
        assert resp.status_code == 200
        assert mock_post.call_count == 2
        mock_sleep.assert_called_once()

    @patch('agents.llm_utils.time.sleep')
    @patch('agents.llm_utils.requests.post')
    def test_retry_on_503(self, mock_post, mock_sleep):
        mock_post.side_effect = [
            _make_resp(503, b'overloaded'),
            _make_resp(200, b'ok'),
        ]

        resp = call_llm_with_retry('http://llm/api', {}, {}, timeout=10)
        assert resp.status_code == 200

    @patch('agents.llm_utils.time.sleep')
    @patch('agents.llm_utils.requests.post')
    def test_timeout_retry(self, mock_post, mock_sleep):
        mock_post.side_effect = [
            requests.exceptions.ReadTimeout(),
            _make_resp(200, b'ok'),
        ]
        resp = call_llm_with_retry('http://llm/api', {}, {}, timeout=5)
        assert resp.status_code == 200

    @patch('agents.llm_utils.time.sleep')
    @patch('agents.llm_utils.requests.post')
    def test_all_retries_exhausted(self, mock_post, mock_sleep):
        exc = requests.exceptions.ReadTimeout()
        mock_post.side_effect = [exc] * _RATE_LIMIT_RETRIES
        with pytest.raises(requests.exceptions.ReadTimeout):
            call_llm_with_retry('http://llm/api', {}, {}, timeout=5)
        assert mock_post.call_count == _RATE_LIMIT_RETRIES

    @patch('agents.llm_utils.time.sleep')
    @patch('agents.llm_utils.requests.post')
    def test_retry_after_header_used(self, mock_post, mock_sleep):
        """When endpoint returns Retry-After, that delay is used."""
        mock_post.side_effect = [
            _make_resp(429, b'rate limited', headers={"Retry-After": "7"}),
            _make_resp(200, b'ok'),
        ]
        resp = call_llm_with_retry('http://llm/api', {}, {}, timeout=10)
        assert resp.status_code == 200
        mock_sleep.assert_called_once_with(7.0)
