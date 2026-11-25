"""Tests for Knowledge Pages normalize helper and KnowledgePageService."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.KnowledgePageService import KnowledgePageService
from back.core.errors import InfrastructureError
from back.core.databricks.knowledge_pages import normalize_knowledge_pages


@pytest.fixture(autouse=True)
def _reset_domains_cache():
    KnowledgePageService.clear_domains_cache()
    yield
    KnowledgePageService.clear_domains_cache()


@pytest.fixture
def clean_databricks_env(monkeypatch):
    for key in (
        "DATABRICKS_APP_PORT",
        "DATABRICKS_TOKEN",
        "DATABRICKS_HOST",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
    ):
        monkeypatch.delenv(key, raising=False)


class TestNormalizeKnowledgePages:
    def test_empty_and_invalid(self):
        assert normalize_knowledge_pages(None) == []
        assert normalize_knowledge_pages({}) == []
        assert normalize_knowledge_pages("x") == []
        assert normalize_knowledge_pages([{"name": "bad"}]) == []

    def test_dedupe_and_shape(self):
        raw = [
            {
                "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
                "domain": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
                "display_name": "On-Time Delivery",
                "state": "published",
                "description": "Share of shipments",
                "synonyms": ["OTD", "OTD", ""],
                "update_time": "2026-10-27T05:17:02.529Z",
                "body": "should not be kept by normalize",
            },
            {
                "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
                "display_name": "Duplicate",
            },
        ]
        out = normalize_knowledge_pages(raw)
        assert len(out) == 1
        assert out[0]["name"].startswith("pages/")
        assert out[0]["state"] == "PUBLISHED"
        assert out[0]["synonyms"] == ["OTD"]
        assert "body" not in out[0]


class TestKnowledgePageService:
    @patch("requests.get")
    def test_list_pages_encodes_filter_and_normalizes(self, mock_get, clean_databricks_env):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {
            "pages": [
                {
                    "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
                    "domain": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
                    "display_name": "On-Time Delivery",
                    "state": "PUBLISHED",
                    "update_time": "2026-10-27T05:17:02.529Z",
                }
            ],
            "next_page_token": "tok2",
        }
        mock_get.return_value = m

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = KnowledgePageService(auth)
        result = svc.list_pages(
            page_size=25,
            filter_str='domain = "domains/3ec7f320-1464-4630-a606-409e07d2e778" AND state = "PUBLISHED"',
        )
        assert len(result["pages"]) == 1
        assert result["next_page_token"] == "tok2"
        assert mock_get.call_args.kwargs["params"]["page_size"] == 25
        assert "filter" in mock_get.call_args.kwargs["params"]
        assert mock_get.call_args.args[0].endswith("/api/2.0/knowledge/pages")

    @patch("requests.get")
    def test_get_page_path(self, mock_get, clean_databricks_env):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {
            "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
            "domain": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
            "display_name": "On-Time Delivery",
            "state": "PUBLISHED",
            "description": "desc",
            "synonyms": ["OTD"],
            "body": "## Definition\nShare of shipments",
            "create_time": "2026-09-01T00:00:00Z",
            "update_time": "2026-10-27T05:17:02.529Z",
            "owner_ids": ["123"],
        }
        mock_get.return_value = m

        auth = DatabricksAuth(host="https://h.com", token="tok")
        page = KnowledgePageService(auth).get_page(
            "pages/004f614e-c6bc-4c93-b12d-f95c7569e173"
        )
        assert page["display_name"] == "On-Time Delivery"
        assert page["body"].startswith("## Definition")
        assert mock_get.call_args.args[0].endswith(
            "/api/2.0/knowledge/pages/004f614e-c6bc-4c93-b12d-f95c7569e173"
        )

    def test_get_page_rejects_bad_name(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        with pytest.raises(ValueError):
            KnowledgePageService(auth).get_page("not-a-page")

    @patch("requests.get")
    def test_list_pages_403_names_missing_permission(self, mock_get, clean_databricks_env):
        response = MagicMock(status_code=403)
        response.json.return_value = {"message": "User does not have access"}
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(
            "403 Client Error: Forbidden", response=response
        )
        auth = DatabricksAuth(host="https://h.com", token="tok")

        with pytest.raises(InfrastructureError) as exc_info:
            KnowledgePageService(auth).list_pages()

        message = exc_info.value.message
        assert message.startswith("Permission denied")
        assert "(HTTP 403)" in message
        assert "User does not have access" in message

    @patch("requests.get")
    def test_list_pages_403_domain_admin_tells_to_pick_a_domain(
        self, mock_get, clean_databricks_env
    ):
        response = MagicMock(status_code=403)
        response.json.return_value = {
            "message": (
                "Domain Admin permissions are required to list pages. Without a "
                "domain filter, account-wide domain administration is required."
            )
        }
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(
            "403 Client Error: Forbidden", response=response
        )
        auth = DatabricksAuth(host="https://h.com", token="tok")

        with pytest.raises(InfrastructureError) as exc_info:
            KnowledgePageService(auth).list_pages()

        assert exc_info.value.message.endswith(
            "Select a Domain in the Domains filter to list its pages."
        )

    @patch("requests.get")
    def test_list_pages_403_other_reason_has_no_domain_hint(
        self, mock_get, clean_databricks_env
    ):
        response = MagicMock(status_code=403)
        response.json.return_value = {"message": "User does not have access"}
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError(
            "403 Client Error: Forbidden", response=response
        )
        auth = DatabricksAuth(host="https://h.com", token="tok")

        with pytest.raises(InfrastructureError) as exc_info:
            KnowledgePageService(auth).list_pages()

        assert "Domains filter" not in exc_info.value.message

    @patch("requests.get")
    def test_list_pages_connection_error_is_explicit(self, mock_get, clean_databricks_env):
        mock_get.side_effect = requests.ConnectionError("no route to host")
        auth = DatabricksAuth(host="https://h.com", token="tok")

        with pytest.raises(InfrastructureError) as exc_info:
            KnowledgePageService(auth).list_pages()

        assert exc_info.value.message.startswith("Could not reach Databricks")
        assert "no route to host" in exc_info.value.detail

    @patch("requests.get")
    def test_list_domains(self, mock_get, clean_databricks_env):
        m = MagicMock()
        m.raise_for_status = MagicMock()
        m.json.return_value = {
            "domains": [
                {
                    "domain_id": "3ec7f320-1464-4630-a606-409e07d2e778",
                    "name": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
                    "tag_key": "Logistics",
                    "subtitle": "Shipments",
                }
            ]
        }
        mock_get.return_value = m
        auth = DatabricksAuth(host="https://h.com", token="tok")
        domains = KnowledgePageService(auth).list_domains()
        assert len(domains) == 1
        # Prefer governed tag_key (domain name) over subtitle description.
        assert domains[0]["label"] == "Logistics"
        assert domains[0]["name"].startswith("domains/")


def _domains_response(tag_key="Logistics"):
    m = MagicMock()
    m.raise_for_status = MagicMock()
    m.json.return_value = {
        "domains": [
            {
                "domain_id": "3ec7f320-1464-4630-a606-409e07d2e778",
                "name": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
                "tag_key": tag_key,
            }
        ]
    }
    return m


class TestListDomainsCache:
    @patch("requests.get")
    def test_second_call_is_served_from_cache(self, mock_get, clean_databricks_env):
        mock_get.return_value = _domains_response()
        auth = DatabricksAuth(host="https://h.com", token="tok")
        KnowledgePageService(auth).list_domains()
        KnowledgePageService(auth).list_domains()
        assert mock_get.call_count == 1

    @patch("requests.get")
    def test_cached_result_is_isolated_from_caller_mutation(self, mock_get, clean_databricks_env):
        mock_get.return_value = _domains_response()
        auth = DatabricksAuth(host="https://h.com", token="tok")
        KnowledgePageService(auth).list_domains()[0]["label"] = "mutated"
        assert KnowledgePageService(auth).list_domains()[0]["label"] == "Logistics"

    @patch("requests.get")
    def test_other_identity_does_not_share_cache(self, mock_get, clean_databricks_env):
        mock_get.return_value = _domains_response()
        KnowledgePageService(DatabricksAuth(host="https://h.com", token="a")).list_domains()
        KnowledgePageService(DatabricksAuth(host="https://h.com", token="b")).list_domains()
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_failed_fetch_is_not_cached(self, mock_get, clean_databricks_env):
        mock_get.side_effect = [requests.ConnectionError("down"), _domains_response()]
        auth = DatabricksAuth(host="https://h.com", token="tok")
        with pytest.raises(requests.ConnectionError):
            KnowledgePageService(auth).list_domains()
        assert len(KnowledgePageService(auth).list_domains()) == 1

    @patch("requests.get")
    def test_expired_entry_is_refetched(self, mock_get, clean_databricks_env, monkeypatch):
        import sys

        kps = sys.modules[KnowledgePageService.__module__]

        mock_get.return_value = _domains_response()
        auth = DatabricksAuth(host="https://h.com", token="tok")
        KnowledgePageService(auth).list_domains()
        monkeypatch.setattr(kps, "_DOMAINS_CACHE_TTL_S", 0)
        KnowledgePageService(auth).list_domains()
        assert mock_get.call_count == 2
