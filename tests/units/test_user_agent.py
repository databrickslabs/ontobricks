"""Verify that every outbound HTTP call site sets User-Agent: OntoBricks/<version>.

Coverage map
------------
Layer                               Module / function under test
──────────────────────────────────  ──────────────────────────────────────────────────
Databricks auth (PAT)               DatabricksAuth.get_auth_headers  – PAT branch
Databricks auth (OAuth)             DatabricksAuth.get_auth_headers  – OAuth branch
Databricks auth (no creds)          DatabricksAuth.get_auth_headers  – fallback branch
Volume file service                 VolumeFileService._headers
Synced table manager (fallback)     SyncedTableManager._call_api  – raw-requests path
Mapping documents                   Mapping.fetch_documents_for_agent (requests.get)
Health accelerated-sync probe       _check_lakebase_accelerated_sync (requests.get)
Agent document tools                agents.tools.documents._headers
LLM utility                         agents.llm_utils.call_llm_with_retry (requests.post)
DTwin chat httpx client             agent_dtwin_chat.tools._client
Cohort httpx client                 agent_cohort.tools._client
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch, call

import httpx
import requests

from shared.config.constants import HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _ok_response(json_body: dict | None = None) -> MagicMock:
    """Minimal requests.Response mock that returns HTTP 200."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = 200
    resp.ok = True
    resp.content = b"{}"
    resp.text = "{}"
    resp.json.return_value = json_body or {}
    resp.raise_for_status = MagicMock()
    return resp


def _headers_kwarg(mock_call) -> dict:
    """Extract the ``headers`` kwarg from the first call of a mock."""
    _, kwargs = mock_call.call_args
    return kwargs.get("headers") or {}


# ──────────────────────────────────────────────────────────────────────────────
# DatabricksAuth.get_auth_headers
# ──────────────────────────────────────────────────────────────────────────────

class TestDatabricksAuthHeaders:
    def _clear(self, monkeypatch):
        for key in (
            "DATABRICKS_APP_PORT",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
            "DATABRICKS_TOKEN",
            "DATABRICKS_HOST",
        ):
            monkeypatch.delenv(key, raising=False)

    def test_pat_mode_sets_user_agent(self, monkeypatch):
        self._clear(monkeypatch)
        from back.core.databricks import DatabricksAuth
        auth = DatabricksAuth(host="https://ws.example.com", token="pat-token")
        assert auth.get_auth_headers().get("User-Agent") == HTTP_USER_AGENT

    def test_oauth_mode_sets_user_agent(self, monkeypatch):
        self._clear(monkeypatch)
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        from back.core.databricks import DatabricksAuth
        with patch.object(DatabricksAuth, "get_oauth_token", return_value="tok"):
            auth = DatabricksAuth(host="https://ws.example.com", token="")
            assert auth.get_auth_headers().get("User-Agent") == HTTP_USER_AGENT

    def test_no_creds_still_sets_user_agent(self, monkeypatch):
        self._clear(monkeypatch)
        from back.core.databricks import DatabricksAuth
        auth = DatabricksAuth(host="https://ws.example.com", token="")
        assert auth.get_auth_headers().get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# VolumeFileService._headers
# ──────────────────────────────────────────────────────────────────────────────

class TestVolumeFileServiceHeaders:
    def test_user_agent_present(self, monkeypatch):
        from back.core.databricks.unity_catalog.VolumeFileService import VolumeFileService
        from back.core.databricks.DatabricksAuth import DatabricksAuth

        auth = MagicMock(spec=DatabricksAuth)
        auth.get_bearer_token.return_value = "tok"
        auth.host = "https://ws.example.com"
        auth.has_valid_auth.return_value = True

        svc = VolumeFileService.__new__(VolumeFileService)
        svc._auth = auth
        svc._session = requests.Session()

        assert svc._headers().get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# SyncedTableManager._call_api — raw-requests fallback path
# ──────────────────────────────────────────────────────────────────────────────

class TestSyncedTableManagerHeaders:
    def test_user_agent_in_fallback_request(self, monkeypatch):
        """When WorkspaceClient.api_client is absent, _RestDatabaseAPI falls back
        to raw requests — those must include the User-Agent header."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.example.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")

        from back.core.graphdb.lakebase.SyncedTableManager import _RestDatabaseAPI

        mock_wc = MagicMock()
        # Force the raw-requests path by removing api_client
        del mock_wc.api_client

        api = _RestDatabaseAPI(mock_wc)

        with patch("requests.request", return_value=_ok_response()) as mock_req:
            api._do("GET", "/api/2.0/database/synced_tables")

        assert _headers_kwarg(mock_req).get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# Mapping.fetch_documents_for_agent
# ──────────────────────────────────────────────────────────────────────────────

class TestMappingDocumentHeaders:
    def test_user_agent_in_documents_request(self, monkeypatch):
        from back.objects.mapping.Mapping import Mapping

        domain = MagicMock()
        with patch(
            "back.core.helpers.effective_uc_version_path",
            return_value="/Volumes/cat/sch/vol/v1",
        ), patch("requests.get", return_value=_ok_response({"contents": []})) as mock_get:
            Mapping.fetch_documents_for_agent(domain, "https://ws.example.com", "tok")

        assert _headers_kwarg(mock_get).get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# health._check_lakebase_accelerated_sync
# ──────────────────────────────────────────────────────────────────────────────

class TestHealthAcceleratedSyncHeaders:
    def test_user_agent_in_probe_request(self, monkeypatch):
        """Set env vars so DatabricksAuth passes auth guards, then intercept
        the outbound requests.get and assert User-Agent is present."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://ws.example.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-tok")
        # Ensure app-mode OAuth path is not triggered
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)

        from shared.fastapi.health import _check_lakebase_accelerated_sync

        with patch("requests.get", return_value=_ok_response({"synced_tables": []})) as mock_get:
            _check_lakebase_accelerated_sync()

        assert _headers_kwarg(mock_get).get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# agents/tools/documents._headers
# ──────────────────────────────────────────────────────────────────────────────

class TestDocumentToolHeaders:
    def test_user_agent_present(self):
        from agents.tools.documents import _headers
        from agents.tools.context import ToolContext

        ctx = ToolContext(host="https://ws.example.com", token="tok")
        assert _headers(ctx).get("User-Agent") == HTTP_USER_AGENT


# ──────────────────────────────────────────────────────────────────────────────
# agents/llm_utils.call_llm_with_retry
# ──────────────────────────────────────────────────────────────────────────────

class TestLLMUtilsHeaders:
    def test_user_agent_added_to_post(self):
        from agents.llm_utils import call_llm_with_retry

        caller_headers = {"Authorization": "Bearer tok", "Content-Type": "application/json"}

        with patch("requests.post", return_value=_ok_response()) as mock_post:
            call_llm_with_retry("https://llm.example.com", caller_headers, {})

        sent_headers = _headers_kwarg(mock_post)
        assert sent_headers.get("User-Agent") == HTTP_USER_AGENT

    def test_caller_headers_not_mutated(self):
        """UA must be injected without altering the caller's dict."""
        from agents.llm_utils import call_llm_with_retry

        caller_headers = {"Authorization": "Bearer tok"}
        original = dict(caller_headers)

        with patch("requests.post", return_value=_ok_response()):
            call_llm_with_retry("https://llm.example.com", caller_headers, {})

        assert caller_headers == original


# ──────────────────────────────────────────────────────────────────────────────
# agent_dtwin_chat / agent_cohort — httpx.Client factories
# ──────────────────────────────────────────────────────────────────────────────

class TestAgentHttpxClientHeaders:
    def _ctx(self):
        from agents.tools.context import ToolContext
        return ToolContext(
            host="https://ws.example.com",
            token="tok",
            dtwin_base_url="http://loopback.invalid",
        )

    def test_dtwin_chat_client_user_agent(self):
        from agents.agent_dtwin_chat.tools import _client
        client = _client(self._ctx())
        assert client.headers.get("user-agent") == HTTP_USER_AGENT

    def test_cohort_client_user_agent(self):
        from agents.agent_cohort.tools import _client
        client = _client(self._ctx())
        assert client.headers.get("user-agent") == HTTP_USER_AGENT

    def test_dtwin_chat_caller_headers_not_overwritten(self):
        """Session headers forwarded via ctx must survive alongside UA."""
        from agents.agent_dtwin_chat.tools import _client
        from agents.tools.context import ToolContext

        ctx = ToolContext(
            host="https://ws.example.com",
            token="tok",
            dtwin_base_url="http://loopback.invalid",
            dtwin_session_headers={"X-Forwarded-User": "alice"},
        )
        client = _client(ctx)
        assert client.headers.get("user-agent") == HTTP_USER_AGENT
        assert client.headers.get("x-forwarded-user") == "alice"
