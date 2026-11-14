"""Create Domain persistence is restricted to app administrators."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from api.routers.internal import _permissions
from back.core.errors import AuthorizationError
from back.objects.registry import ROLE_ADMIN


def _request(role: str) -> MagicMock:
    request = MagicMock(spec=Request)
    request.state = SimpleNamespace(
        user_role=role,
        user_email="user@example.com",
    )
    return request


def test_admin_can_create_domain():
    _permissions.assert_admin_can_create_domain(
        _request(ROLE_ADMIN),
        SimpleNamespace(domain_folder=""),
    )


def test_editor_can_save_existing_domain():
    _permissions.assert_admin_can_create_domain(
        _request("editor"),
        SimpleNamespace(domain_folder="sales"),
    )


@pytest.mark.parametrize("role", ["editor", "builder", "viewer"])
def test_non_admin_cannot_create_domain(role):
    with pytest.raises(
        AuthorizationError,
        match="Only administrators can create a context",
    ):
        _permissions.assert_admin_can_create_domain(
            _request(role),
            SimpleNamespace(domain_folder=""),
        )


def test_save_to_uc_route_enforces_create_permission():
    from api.routers.internal import domain as domain_routes

    assert (
        "assert_admin_can_create_domain"
        in domain_routes.save_domain_to_uc.__code__.co_names
    )
