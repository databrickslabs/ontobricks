"""Unit tests for ``SettingsService._lakebase_branch_info`` /
``_lakebase_active_branch``.

OntoBricks targets **Lakebase Autoscaling** exclusively. These
tests pin the project-resource lookup that surfaces the active
branch (the one hosting the bound ``PGDATABASE``) plus the
project's autoscaling CU min/max range to the admin UI.

The full ``_lakebase_instance_metadata`` path is exercised in the
admin UI smoke test; here we keep the unit scope tight and stub
the SDK ``WorkspaceClient.api_client.do`` calls.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from back.objects.domain.SettingsService import SettingsService


def _w_with_responses(responses):
    """Build a fake WorkspaceClient whose ``api_client.do(method, path)``
    returns the value mapped by *path* — or raises if mapped to an
    Exception instance.
    """
    w = MagicMock()

    def _do(method, path, *args, **kwargs):
        if path not in responses:
            raise AssertionError(f"unexpected api call: {method} {path}")
        value = responses[path]
        if isinstance(value, Exception):
            raise value
        return value

    w.api_client.do.side_effect = _do
    return w


class TestLakebaseBranchInfo:
    def test_extracts_default_branch_and_cu_range(self):
        w = _w_with_responses(
            {
                "/api/2.0/postgres/projects/ontobricks-app": {
                    "name": "projects/ontobricks-app",
                    "status": {
                        "default_branch": "projects/ontobricks-app/branches/production",
                        "default_endpoint_settings": {
                            "autoscaling_limit_min_cu": 4,
                            "autoscaling_limit_max_cu": 8,
                        },
                    },
                },
                # No bound database → never hits the branch listing API.
            }
        )
        info = SettingsService._lakebase_branch_info(w, "ontobricks-app", "")
        assert info["branch"] == "production"
        assert info["branch_resource"] == "projects/ontobricks-app/branches/production"
        assert info["autoscaling_min_cu"] == 4
        assert info["autoscaling_max_cu"] == 8

    def test_finds_branch_hosting_bound_database(self):
        w = _w_with_responses(
            {
                "/api/2.0/postgres/projects/ontobricks-app": {
                    "status": {
                        "default_branch": "projects/ontobricks-app/branches/main",
                        "default_endpoint_settings": {
                            "autoscaling_limit_min_cu": 1,
                            "autoscaling_limit_max_cu": 2,
                        },
                    }
                },
                "/api/2.0/postgres/projects/ontobricks-app/branches": {
                    "branches": [
                        {"name": "projects/ontobricks-app/branches/main"},
                        {"name": "projects/ontobricks-app/branches/production"},
                    ]
                },
                "/api/2.0/postgres/projects/ontobricks-app/branches/main/databases": {
                    "databases": [
                        {"status": {"postgres_database": "databricks_postgres"}},
                    ]
                },
                "/api/2.0/postgres/projects/ontobricks-app/branches/production/databases": {
                    "databases": [
                        {"status": {"postgres_database": "databricks_postgres"}},
                        {"status": {"postgres_database": "ontobricks_registry"}},
                    ]
                },
            }
        )
        info = SettingsService._lakebase_branch_info(
            w, "ontobricks-app", "ontobricks_registry"
        )
        assert info["branch"] == "production"
        assert (
            info["branch_resource"]
            == "projects/ontobricks-app/branches/production"
        )

    def test_falls_back_to_default_branch_when_db_unmatched(self):
        w = _w_with_responses(
            {
                "/api/2.0/postgres/projects/ontobricks-app": {
                    "status": {
                        "default_branch": "projects/ontobricks-app/branches/main",
                        "default_endpoint_settings": {},
                    }
                },
                "/api/2.0/postgres/projects/ontobricks-app/branches": {
                    "branches": [
                        {"name": "projects/ontobricks-app/branches/main"}
                    ]
                },
                "/api/2.0/postgres/projects/ontobricks-app/branches/main/databases": {
                    "databases": []
                },
            }
        )
        info = SettingsService._lakebase_branch_info(
            w, "ontobricks-app", "missing-db"
        )
        assert info["branch"] == "main"

    def test_project_endpoint_failure_returns_empty(self):
        # OntoBricks no longer carries a Provisioned fallback. When
        # the Autoscaling project lookup fails we surface an empty
        # dict and let the caller keep the default payload values
        # (``branch=""``, ``branch_resource=""``, ``autoscaling_*=None``).
        w = _w_with_responses(
            {
                "/api/2.0/postgres/projects/legacy-instance": Exception(
                    "404 not found"
                )
            }
        )
        info = SettingsService._lakebase_branch_info(
            w, "legacy-instance", "anything"
        )
        assert info == {}

    def test_empty_instance_name_returns_empty_dict(self):
        info = SettingsService._lakebase_branch_info(MagicMock(), "", "any")
        assert info == {}


class TestLakebaseActiveBranch:
    def test_returns_default_when_bound_database_is_empty(self):
        # Empty bound_database short-circuits to the default branch
        # path without hitting the API. Avoids needless calls when the
        # platform hasn't injected PGDATABASE yet.
        active = SettingsService._lakebase_active_branch(
            MagicMock(), "ontobricks-app", "", "projects/x/branches/main"
        )
        assert active == "projects/x/branches/main"

    def test_branch_listing_failure_falls_back_to_default(self):
        w = _w_with_responses(
            {
                "/api/2.0/postgres/projects/ontobricks-app/branches": Exception(
                    "api down"
                )
            }
        )
        active = SettingsService._lakebase_active_branch(
            w,
            "ontobricks-app",
            "ontobricks_registry",
            "projects/ontobricks-app/branches/main",
        )
        assert active == "projects/ontobricks-app/branches/main"
