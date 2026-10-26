"""Tests for executable Unity AI Gateway model-service discovery."""

import threading
from unittest.mock import MagicMock, patch

from back.core.databricks.ModelServiceCatalog import ModelServiceCatalog


def _response(payload, status=200):
    response = MagicMock()
    response.status_code = status
    response.json.return_value = payload
    if status >= 400:
        response.raise_for_status.side_effect = RuntimeError(f"HTTP {status}")
    return response


def _client():
    client = MagicMock()
    client.host = "https://workspace"
    client.get_auth_headers.return_value = {"Authorization": "Bearer token"}
    client.get_current_user_email.return_value = "me@example.com"
    client.get_catalogs.return_value = ["main"]
    client.get_schemas.return_value = ["ai"]
    return client


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_uses_global_paginated_listing_without_schema_enumeration(mock_get):
    client = _client()
    client.get_catalogs.side_effect = AssertionError("must not enumerate catalogs")
    mock_get.return_value = _response({"model_services": []})

    assert ModelServiceCatalog(client).list_executable() == []

    assert "model-services" in mock_get.call_args.args[0]
    assert "parent" not in mock_get.call_args.kwargs["params"]


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_effective_permission_checks_overlap(mock_get):
    client = _client()
    services = [
        {
            "name": f"model-services/main.ai.service_{index}",
            "owner": "other@example.com",
        }
        for index in range(4)
    ]
    mock_get.return_value = _response({"model_services": services})
    lock = threading.Lock()
    overlap = threading.Event()
    active = 0
    max_active = 0

    def check_execute(_name, _principal):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
            if active >= 2:
                overlap.set()
        overlap.wait(timeout=0.2)
        with lock:
            active -= 1
        return True

    catalog = ModelServiceCatalog(client)
    with patch.object(catalog, "_has_execute", side_effect=check_execute):
        result = catalog.list_executable()

    assert len(result) == 4
    assert max_active >= 2


@patch("back.core.databricks.ModelServiceCatalog.time.sleep")
@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_effective_permission_check_retries_rate_limit(mock_get, mock_sleep):
    client = _client()
    limited = _response({}, status=429)
    limited.headers = {"Retry-After": "0"}
    mock_get.side_effect = [
        limited,
        _response(
            {
                "privilege_assignments": [
                    {"privileges": [{"privilege": "EXECUTE"}]}
                ]
            }
        ),
    ]

    assert ModelServiceCatalog(client)._has_execute("main.ai.model", "me@example.com")
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(0.0)


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_lists_all_pages_and_strips_resource_prefix(mock_get):
    client = _client()
    mock_get.side_effect = [
        _response(
            {
                "model_services": [
                    {
                        "name": "model-services/main.ai.first",
                        "owner": "other@example.com",
                    }
                ],
                "next_page_token": "next",
            }
        ),
        _response(
            {
                "privilege_assignments": [
                    {
                        "principal": "me@example.com",
                        "privileges": [{"privilege": "EXECUTE"}],
                    }
                ]
            }
        ),
        _response(
            {
                "model_services": [
                    {
                        "name": "model-services/main.ai.second",
                        "owner": "me@example.com",
                        "comment": "routed",
                    }
                ]
            }
        ),
    ]

    result = ModelServiceCatalog(client).list_executable()

    assert result == [
        {"name": "main.ai.first", "kind": "ai_gateway", "comment": ""},
        {"name": "main.ai.second", "kind": "ai_gateway", "comment": "routed"},
    ]
    assert mock_get.call_args_list[2].kwargs["params"]["page_token"] == "next"


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_excludes_read_metadata_only_service(mock_get):
    client = _client()
    mock_get.side_effect = [
        _response(
            {
                "model_services": [
                    {
                        "name": "model-services/main.ai.visible",
                        "owner": "other@example.com",
                    }
                ]
            }
        ),
        _response(
            {
                "privilege_assignments": [
                    {
                        "principal": "me@example.com",
                        "privileges": [{"privilege": "READ_METADATA"}],
                    }
                ]
            }
        ),
    ]

    assert ModelServiceCatalog(client).list_executable() == []


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_all_privileges_implies_execute(mock_get):
    client = _client()
    mock_get.return_value = _response(
        {
            "privilege_assignments": [
                {
                    "principal": "me@example.com",
                    "privileges": [
                        {
                            "privilege": "ALL_PRIVILEGES",
                            "inherited_from_type": "CATALOG",
                        }
                    ],
                }
            ]
        }
    )

    assert ModelServiceCatalog(client)._has_execute("main.ai.gateway", "me@example.com")


@patch("back.core.databricks.ModelServiceCatalog.requests.get")
def test_forbidden_global_listing_returns_empty(mock_get):
    client = _client()
    mock_get.return_value = _response({}, status=403)

    assert ModelServiceCatalog(client).list_executable() == []
