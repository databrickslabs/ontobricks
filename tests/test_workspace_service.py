"""Tests for back.core.databricks.WorkspaceService."""
import pytest
from unittest.mock import MagicMock, patch

import requests

from back.core.databricks.DatabricksAuth import DatabricksAuth
from back.core.databricks.WorkspaceService import WorkspaceService


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


class TestGetCurrentUserEmail:
    @patch("requests.get")
    def test_success(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(
            status_code=200,
        )
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = {"userName": "me@co.com"}

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        assert svc.get_current_user_email() == "me@co.com"
        mock_get.assert_called_once()
        assert "/scim/v2/Me" in mock_get.call_args[0][0]

    @patch("requests.get")
    def test_failure(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(status_code=500)
        mock_get.return_value.raise_for_status.side_effect = (
            requests.HTTPError()
        )

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        assert svc.get_current_user_email() == ""

    def test_no_auth(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="")
        svc = WorkspaceService(auth)
        assert svc.get_current_user_email() == ""


class TestListUsers:
    @patch("requests.get")
    def test_success_with_pagination(self, mock_get, clean_databricks_env):
        def _resp(payload):
            m = MagicMock(status_code=200)
            m.raise_for_status = MagicMock()
            m.json.return_value = payload
            return m

        mock_get.side_effect = [
            _resp(
                {
                    "Resources": [
                        {
                            "userName": "a@x.com",
                            "displayName": "A",
                            "active": True,
                        }
                    ],
                    "totalResults": 150,
                    "itemsPerPage": 100,
                }
            ),
            _resp(
                {
                    "Resources": [
                        {"userName": "b@x.com", "displayName": "B"}
                    ],
                    "totalResults": 150,
                    "itemsPerPage": 100,
                }
            ),
        ]

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        users = svc.list_users()
        assert len(users) == 2
        assert users[0]["email"] == "a@x.com"
        assert users[1]["email"] == "b@x.com"
        assert mock_get.call_count == 2

    @patch("requests.get")
    def test_failure(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(status_code=503)
        mock_get.return_value.raise_for_status.side_effect = (
            requests.HTTPError()
        )

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        assert svc.list_users() == []


class TestListGroups:
    @patch("requests.get")
    def test_success(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = {
            "Resources": [
                {"displayName": "g1", "id": "101"},
                {"displayName": "", "id": "skip"},
            ]
        }

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        groups = svc.list_groups()
        assert len(groups) == 1
        assert groups[0]["display_name"] == "g1"
        assert groups[0]["id"] == "101"

    @patch("requests.get")
    def test_failure(self, mock_get, clean_databricks_env):
        mock_get.return_value.raise_for_status.side_effect = (
            requests.HTTPError()
        )

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        assert svc.list_groups() == []


class TestGetAppPermissions:
    @patch("requests.get")
    def test_success(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = {
            "access_control_list": [
                {
                    "user_name": "u@x.com",
                    "all_permissions": [
                        {
                            "permission_level": "CAN_MANAGE",
                            "inherited": False,
                        }
                    ],
                }
            ]
        }

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        perms = svc.get_app_permissions("my-app")
        assert len(perms) == 1
        assert perms[0]["principal"] == "u@x.com"
        assert perms[0]["permission_level"] == "CAN_MANAGE"

    def test_no_app_name(self, clean_databricks_env):
        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        assert svc.get_app_permissions("") == []


class TestListAppPrincipals:
    @patch("requests.get")
    def test_success_users_and_groups(self, mock_get, clean_databricks_env):
        mock_get.return_value = MagicMock(status_code=200)
        mock_get.return_value.raise_for_status = MagicMock()
        mock_get.return_value.json.return_value = {
            "access_control_list": [
                {
                    "user_name": "alice@x.com",
                    "all_permissions": [
                        {
                            "permission_level": "CAN_MANAGE",
                            "inherited": False,
                        }
                    ],
                },
                {
                    "group_name": "data-team",
                    "all_permissions": [
                        {"permission_level": "CAN_VIEW", "inherited": False}
                    ],
                },
            ]
        }

        auth = DatabricksAuth(host="https://h.com", token="tok")
        svc = WorkspaceService(auth)
        out = svc.list_app_principals("app1")
        assert len(out["users"]) == 1
        assert out["users"][0]["email"] == "alice@x.com"
        assert out["users"][0]["permission_level"] == "CAN_MANAGE"
        assert len(out["groups"]) == 1
        assert out["groups"][0]["display_name"] == "data-team"
        assert out["groups"][0]["permission_level"] == "CAN_VIEW"
