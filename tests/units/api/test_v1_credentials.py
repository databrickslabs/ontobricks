"""Public /api/v1 must not use caller-supplied Databricks credentials in Apps mode.

Marketplace SOP (Phase 1): the app MUST NOT ask for or store user PAT tokens.
In Databricks Apps mode every /api/v1 route authenticates with the app's
service principal, but only for a signed-in user with access to the app (the
``/api/`` prefix skips the role middleware); the ``X-Databricks-*`` headers and
``databricks_*`` body fields remain a local-dev convenience only.
"""

import importlib
from unittest.mock import patch

import pytest

from back.core.databricks.request_identity import (
    RequestIdentity,
    reset_request_identity,
    set_request_identity,
)
from back.core.errors import AuthorizationError
from back.objects.registry import ROLE_ADMIN, ROLE_APP_USER, ROLE_NONE

pytestmark = pytest.mark.unit

_v1 = importlib.import_module("api.routers.v1")

_SERVER = ("https://server.example", "sp-token")


def _creds(
    app_mode, body=None, host=None, token=None, *, email="u@x.com", role=ROLE_APP_USER
):
    """Call ``get_credentials`` as *email* whose app role resolves to *role*."""
    reset = set_request_identity(RequestIdentity(email=email, user_token="user-tok"))
    try:
        with (
            patch.object(_v1, "is_databricks_app", return_value=app_mode),
            patch.object(_v1, "get_databricks_host_and_token", return_value=_SERVER),
            patch.object(
                _v1.permission_service, "get_user_role", return_value=role
            ) as get_role,
        ):
            result = _v1.get_credentials(body, host, token)
            return result, get_role
    finally:
        reset_request_identity(reset)


def test_header_token_is_ignored_in_app_mode():
    creds, _ = _creds(True, host="https://caller.example", token="dapi-caller")
    assert creds == _SERVER


def test_body_token_is_ignored_in_app_mode():
    body = _v1.CredentialsModel(
        databricks_host="https://caller.example", databricks_token="dapi-body"
    )
    creds, _ = _creds(True, body=body)
    assert creds == _SERVER


def test_caller_credentials_still_used_in_local_mode():
    creds, get_role = _creds(False, host="https://dev.example", token="dapi-dev")
    assert creds == ("https://dev.example", "dapi-dev")
    get_role.assert_not_called()


@pytest.mark.parametrize("role", [ROLE_APP_USER, ROLE_ADMIN])
def test_app_users_get_the_service_principal(role):
    creds, get_role = _creds(True, role=role)
    assert creds == _SERVER
    assert get_role.call_args.kwargs["user_token"] == "user-tok"


def test_user_without_app_access_is_refused_in_app_mode():
    with pytest.raises(AuthorizationError):
        _creds(True, role=ROLE_NONE)


def test_anonymous_caller_is_refused_in_app_mode():
    with pytest.raises(AuthorizationError):
        _creds(True, email="")


def test_openapi_does_not_ask_for_a_pat_in_app_mode():
    description = _v1.CredentialsModel.model_fields["databricks_token"].description
    assert "ignored" in description.lower()
