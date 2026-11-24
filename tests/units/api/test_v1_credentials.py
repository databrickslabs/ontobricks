"""Public /api/v1 must not use caller-supplied Databricks credentials in Apps mode.

Marketplace SOP (Phase 1): the app MUST NOT ask for or store user PAT tokens.
In Databricks Apps mode every /api/v1 route authenticates with the app's
service principal; the ``X-Databricks-*`` headers and ``databricks_*`` body
fields remain a local-dev convenience only.
"""

import importlib
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit

_v1 = importlib.import_module("api.routers.v1")

_SERVER = ("https://server.example", "sp-token")


def _creds(app_mode, body=None, host=None, token=None):
    with patch.object(_v1, "is_databricks_app", return_value=app_mode), patch.object(
        _v1, "get_databricks_host_and_token", return_value=_SERVER
    ):
        return _v1.get_credentials(body, host, token)


def test_header_token_is_ignored_in_app_mode():
    assert _creds(True, host="https://caller.example", token="dapi-caller") == _SERVER


def test_body_token_is_ignored_in_app_mode():
    body = _v1.CredentialsModel(databricks_host="https://caller.example", databricks_token="dapi-body")
    assert _creds(True, body=body) == _SERVER


def test_caller_credentials_still_used_in_local_mode():
    assert _creds(False, host="https://dev.example", token="dapi-dev") == ("https://dev.example", "dapi-dev")


def test_openapi_does_not_ask_for_a_pat_in_app_mode():
    description = _v1.CredentialsModel.model_fields["databricks_token"].description
    assert "ignored" in description.lower()
