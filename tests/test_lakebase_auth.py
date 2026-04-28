"""Unit tests for ``back.core.databricks.LakebaseAuth``.

OntoBricks targets Lakebase **Autoscaling exclusively**. These tests
pin that contract:

* ``instance_name`` resolves the project_id via the Postgres API
  endpoint walk only — the legacy Database Instance API
  (``list_database_instances`` / ``generate_database_credential``)
  must never be called.
* ``password()`` mints via ``POST /api/2.0/postgres/credentials``
  scoped to the endpoint discovered above. Legacy mint must never
  fire.
* ``PGAPPNAME`` must never be used as the project_id — the Apps
  runtime sets it to the app's name, which has nothing to do with
  the Lakebase project and used to break credential minting.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from back.core.databricks.LakebaseAuth import LakebaseAuth
from back.core.errors import ValidationError


@pytest.fixture
def autoscaling_pg_env(monkeypatch):
    """Populate the ``PG*`` env vars to mimic an Apps runtime bound to
    a Lakebase Autoscaling project."""
    monkeypatch.setenv(
        "PGHOST", "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
    )
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "databricks_postgres")
    monkeypatch.setenv("PGUSER", "00000000-1111-2222-3333-444444444444")
    monkeypatch.delenv("DATABASE_INSTANCE_NAME", raising=False)
    monkeypatch.delenv("PGAPPNAME", raising=False)


def _autoscaling_api_client(
    host: str = "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com",
    *,
    project_id: str = "ontobricks-test",
    token: str = "POSTGRES-API-TOKEN",
    read_only_host: str = (
        "ep-damp-art-d1l8gclo-ro.database.us-west-2.cloud.databricks.com"
    ),
):
    """Build a ``MagicMock`` api_client that returns a single
    Autoscaling project whose primary endpoint host matches.

    Both GET (project/branch/endpoint walk) and POST (credential
    mint) responses are wired so a single instance can drive both
    :attr:`instance_name` and :meth:`password` paths.
    """
    project_path = f"projects/{project_id}"
    branch_path = f"{project_path}/branches/production"
    endpoint_path = f"{branch_path}/endpoints/primary"
    get_responses = {
        "GET /api/2.0/postgres/projects": {
            "projects": [{"name": project_path}]
        },
        f"GET /api/2.0/postgres/{project_path}/branches": {
            "branches": [{"name": branch_path}]
        },
        f"GET /api/2.0/postgres/{branch_path}/endpoints": {
            "endpoints": [
                {
                    "name": endpoint_path,
                    "status": {
                        "hosts": {
                            "host": host,
                            "read_only_host": read_only_host,
                        }
                    },
                }
            ]
        },
    }
    calls: list = []

    def do(method, path, *args, **kwargs):
        calls.append((method, path, kwargs.get("body")))
        if method == "GET":
            return get_responses.get(f"GET {path}")
        if method == "POST" and path == "/api/2.0/postgres/credentials":
            assert kwargs.get("body") == {"endpoint": endpoint_path}
            return {"token": token, "expire_time": "2099-01-01T00:00:00Z"}
        raise AssertionError(f"unexpected call {method} {path}")

    api = MagicMock()
    api.do.side_effect = do
    api.calls = calls
    return api


class TestInstanceNameResolution:
    """``instance_name`` must walk the Postgres API only — legacy
    ``list_database_instances`` is never called.
    """

    def test_resolves_project_id_via_postgres_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.side_effect = AssertionError(
            "Legacy Database Instance API must not be called"
        )
        fake_w.api_client = _autoscaling_api_client()
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-test"

    def test_matches_read_only_host(self, monkeypatch, autoscaling_pg_env):
        # PGHOST happens to point at the read-only endpoint — the
        # walk must still match and resolve the project_id.
        monkeypatch.setenv(
            "PGHOST",
            "ep-damp-art-d1l8gclo-ro.database.us-west-2.cloud.databricks.com",
        )
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client = _autoscaling_api_client()
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-test"

    def test_resolution_is_cached(self, monkeypatch, autoscaling_pg_env):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client = _autoscaling_api_client()
        auth._w = fake_w

        first = auth.instance_name
        # Subsequent reads must hit the cache — flip the API client
        # to "explode on call" to prove no further requests happen.
        fake_w.api_client.do.side_effect = AssertionError(
            "Cached project_id must not re-walk the API"
        )
        assert auth.instance_name == first

    def test_no_match_raises_with_helpful_message(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client = _autoscaling_api_client(
            "ep-other-project.database.us-west-2.cloud.databricks.com",
            project_id="other-project",
        )
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        msg = str(excinfo.value)
        assert "No Lakebase Autoscaling endpoint matched" in msg
        assert "ep-damp-art-d1l8gclo" in msg

    def test_postgres_api_failure_propagates_as_validation_error(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client.do.side_effect = RuntimeError("postgres api down")
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        assert "Could not resolve Lakebase Autoscaling project" in str(
            excinfo.value
        )

    def test_pgappname_is_never_used_as_project_id(
        self, monkeypatch, autoscaling_pg_env
    ):
        """Regression: ``PGAPPNAME`` is the app's name, not the
        Lakebase project. The probe must miss the (different) host
        and raise — never silently fall back to ``PGAPPNAME``.
        """
        monkeypatch.setenv("PGAPPNAME", "ontobricks-dev")
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client.do.return_value = {"projects": []}
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        msg = str(excinfo.value)
        assert "No Lakebase Autoscaling endpoint matched" in msg
        # The error names PGHOST, never the app's name.
        assert "ontobricks-dev" not in msg.split("PGHOST=")[-1]


class TestPasswordMinting:
    """``password()`` must mint via the Postgres API only — legacy
    ``generate_database_credential`` must never fire.
    """

    def test_mints_via_postgres_api_with_resolved_endpoint(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.side_effect = AssertionError(
            "Legacy Database Instance API must not be called"
        )
        fake_w.database.generate_database_credential.side_effect = (
            AssertionError("Legacy mint must not run on Autoscaling-only")
        )
        fake_w.api_client = _autoscaling_api_client()
        auth._w = fake_w

        assert auth.password() == "POSTGRES-API-TOKEN"
        # Sanity: subsequent call hits the in-memory cache, not the SDK.
        fake_w.api_client.do.side_effect = AssertionError(
            "Cached token must not re-mint"
        )
        assert auth.password() == "POSTGRES-API-TOKEN"

    def test_invalidate_forces_remint(self, monkeypatch, autoscaling_pg_env):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client = _autoscaling_api_client(token="FIRST-TOKEN")
        auth._w = fake_w
        assert auth.password() == "FIRST-TOKEN"

        # Re-wire the api client to return a different token.
        fake_w.api_client = _autoscaling_api_client(token="SECOND-TOKEN")
        auth.invalidate()
        # Project resolution is cached on ``_endpoint_resource`` already,
        # so only the POST mint runs again — the rebuilt api_client
        # above must be called.
        assert auth.password() == "SECOND-TOKEN"

    def test_empty_token_response_raises(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        api = _autoscaling_api_client()

        original_do = api.do.side_effect

        def do(method, path, *args, **kwargs):
            if method == "POST" and path == "/api/2.0/postgres/credentials":
                return {"token": ""}
            return original_do(method, path, *args, **kwargs)

        api.do.side_effect = do
        fake_w.api_client = api
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.password()
        assert "JWT was empty" in str(excinfo.value)

    def test_no_endpoint_match_raises_at_password(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.api_client = _autoscaling_api_client(
            "ep-other.database.us-west-2.cloud.databricks.com",
            project_id="other",
        )
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.password()
        assert "No Lakebase Autoscaling endpoint matched" in str(excinfo.value)
