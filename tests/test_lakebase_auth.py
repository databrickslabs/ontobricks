"""Unit tests for ``back.core.databricks.LakebaseAuth.instance_name``.

These pin the resolution order: explicit ``DATABASE_INSTANCE_NAME``
env var first, then a workspace SDK lookup matching ``PGHOST`` against
the database instance's DNS records. ``PGAPPNAME`` must **never** be
consulted — Databricks Apps sets it to the app name (e.g.
``ontobricks-dev``), which has nothing to do with the Lakebase
instance and used to break ``generate_database_credential`` with
``Database instance 'ontobricks-dev' not found``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from back.core.databricks.LakebaseAuth import LakebaseAuth
from back.core.errors import ValidationError


@pytest.fixture
def pg_env(monkeypatch):
    """Populate the ``PG*`` env vars to mimic an Apps runtime."""
    monkeypatch.setenv("PGHOST", "instance-abc.database.cloud.databricks.com")
    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
    monkeypatch.setenv("PGUSER", "00000000-1111-2222-3333-444444444444")
    monkeypatch.delenv("DATABASE_INSTANCE_NAME", raising=False)
    monkeypatch.delenv("PGAPPNAME", raising=False)


class TestInstanceNameResolution:
    def test_explicit_database_instance_name_env_wins(self, monkeypatch, pg_env):
        monkeypatch.setenv("DATABASE_INSTANCE_NAME", "ontobricks-app")
        # Even if PGAPPNAME is also set (Apps runtime injects the app
        # name there), the explicit override must take precedence.
        monkeypatch.setenv("PGAPPNAME", "ontobricks-dev")
        auth = LakebaseAuth()
        assert auth.instance_name == "ontobricks-app"

    def test_pgappname_is_never_used_as_instance_name(self, monkeypatch, pg_env):
        """Regression for the deployed-app failure where
        ``generate_database_credential`` was scoped to ``ontobricks-dev``
        (the app name) instead of the actual Lakebase instance.
        """
        monkeypatch.setenv("PGAPPNAME", "ontobricks-dev")
        auth = LakebaseAuth()
        # No explicit override + no workspace client → must NOT fall
        # back to PGAPPNAME. Force both the Database Instance API and
        # the Postgres API fallback to return no match, then assert on
        # the resulting "no match" error rather than a silent
        # PGAPPNAME read.
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        fake_w.api_client.do.return_value = {"projects": []}
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        assert "No Lakebase instance matched" in str(excinfo.value)
        assert "ontobricks-dev" not in str(excinfo.value).split("PGHOST=")[-1]

    def test_sdk_lookup_matches_pghost_to_read_write_dns(self, monkeypatch, pg_env):
        auth = LakebaseAuth()
        inst1 = MagicMock(
            name="other-instance",
            read_write_dns="other.database.cloud.databricks.com",
            read_only_dns="other-ro.database.cloud.databricks.com",
        )
        inst1.name = "other-instance"
        inst2 = MagicMock(
            read_write_dns="instance-abc.database.cloud.databricks.com",
            read_only_dns="instance-abc-ro.database.cloud.databricks.com",
        )
        inst2.name = "ontobricks-app"
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = [inst1, inst2]
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-app"

    def test_resolution_is_cached(self, monkeypatch, pg_env):
        monkeypatch.setenv("DATABASE_INSTANCE_NAME", "ontobricks-app")
        auth = LakebaseAuth()
        first = auth.instance_name
        # Mutating the env after the first read must NOT change the
        # cached value — otherwise tokens minted with the old name
        # could mismatch a later lookup.
        monkeypatch.setenv("DATABASE_INSTANCE_NAME", "something-else")
        assert auth.instance_name == first

    def test_sdk_failure_raises_validation_error(self, monkeypatch, pg_env):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.side_effect = RuntimeError("api down")
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        assert "Could not resolve Lakebase instance name" in str(excinfo.value)


class TestPostgresApiFallback:
    """Pin the Lakebase Autoscaling fallback when ``PGHOST`` is a
    regional ``ep-<id>.database.<region>.cloud.databricks.com``
    hostname not exposed via the legacy Database Instance API.

    Regression for the deployed-app failure where the dev sandbox
    bound to the Autoscaling project ``ontobricks-app`` raised
    ``Failed to mint Lakebase JWT for instance '?': No Lakebase
    instance matched PGHOST=ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com``.
    """

    @pytest.fixture
    def autoscaling_pg_env(self, monkeypatch):
        monkeypatch.setenv(
            "PGHOST", "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
        )
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "ontobricks_registry")
        monkeypatch.setenv("PGUSER", "00000000-1111-2222-3333-444444444444")
        monkeypatch.delenv("DATABASE_INSTANCE_NAME", raising=False)
        monkeypatch.delenv("PGAPPNAME", raising=False)

    @staticmethod
    def _api_client(host: str, project_id: str = "ontobricks-app"):
        """Build a ``MagicMock`` api_client that returns a single
        Autoscaling project whose primary endpoint host matches.
        """
        project_path = f"projects/{project_id}"
        branch_path = f"{project_path}/branches/production"
        responses = {
            "GET /api/2.0/postgres/projects": {
                "projects": [{"name": project_path}]
            },
            f"GET /api/2.0/postgres/{project_path}/branches": {
                "branches": [{"name": branch_path}]
            },
            f"GET /api/2.0/postgres/{branch_path}/endpoints": {
                "endpoints": [
                    {
                        "name": f"{branch_path}/endpoints/primary",
                        "status": {
                            "hosts": {
                                "host": host,
                                "read_only_host": (
                                    "ep-damp-art-d1l8gclo-ro.database."
                                    "us-west-2.cloud.databricks.com"
                                ),
                            }
                        },
                    }
                ]
            },
        }

        def do(method, path, *args, **kwargs):
            return responses.get(f"{method} {path}")

        api = MagicMock()
        api.do.side_effect = do
        return api

    def test_falls_back_to_postgres_api_on_database_api_miss(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        fake_w.api_client = self._api_client(
            "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
        )
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-app"

    def test_postgres_api_matches_read_only_host(
        self, monkeypatch, autoscaling_pg_env
    ):
        # PGHOST happens to point at the read-only endpoint — the
        # fallback must still resolve the project_id so that
        # ``generate_database_credential`` can scope the JWT.
        monkeypatch.setenv(
            "PGHOST",
            "ep-damp-art-d1l8gclo-ro.database.us-west-2.cloud.databricks.com",
        )
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        fake_w.api_client = self._api_client(
            "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
        )
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-app"

    def test_database_api_match_short_circuits_postgres_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        # Legacy API hit must win without paying the Postgres-API
        # walk — verifies the fallback is *only* used when needed.
        auth = LakebaseAuth()
        inst = MagicMock(
            read_write_dns=(
                "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
            ),
            read_only_dns="",
        )
        inst.name = "ontobricks-app"
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = [inst]
        fake_w.api_client.do.side_effect = AssertionError(
            "Postgres API must not be called when Database Instance API matches"
        )
        auth._w = fake_w
        assert auth.instance_name == "ontobricks-app"

    def test_no_match_anywhere_raises_with_helpful_message(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        # Different project's endpoint — must not match.
        fake_w.api_client = self._api_client(
            "ep-other-project.database.us-west-2.cloud.databricks.com",
            project_id="other-project",
        )
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        msg = str(excinfo.value)
        assert "No Lakebase instance matched" in msg
        assert "ep-damp-art-d1l8gclo" in msg

    def test_postgres_api_failure_propagates_as_validation_error(
        self, monkeypatch, autoscaling_pg_env
    ):
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        fake_w.api_client.do.side_effect = RuntimeError("postgres api down")
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.instance_name
        assert "Could not resolve Lakebase instance name" in str(excinfo.value)


class TestPasswordMintingPath:
    """Pin which API mints the JWT, depending on how the project was
    discovered. Regression for the deployed-app failure where
    ``ontobricks-test`` (Autoscaling-only) resolved correctly via the
    Postgres API but the JWT mint then fell back to
    ``w.database.generate_database_credential(instance_names=...)``,
    which raised ``Database instance 'ontobricks-test' not found``.
    """

    @pytest.fixture
    def autoscaling_pg_env(self, monkeypatch):
        monkeypatch.setenv(
            "PGHOST",
            "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com",
        )
        monkeypatch.setenv("PGPORT", "5432")
        monkeypatch.setenv("PGDATABASE", "databricks_postgres")
        monkeypatch.setenv("PGUSER", "00000000-1111-2222-3333-444444444444")
        monkeypatch.delenv("DATABASE_INSTANCE_NAME", raising=False)
        monkeypatch.delenv("PGAPPNAME", raising=False)

    @staticmethod
    def _autoscaling_api_client(
        host: str = "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com",
        project_id: str = "ontobricks-test",
        token: str = "POSTGRES-API-TOKEN",
    ):
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
                        "status": {"hosts": {"host": host}},
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
                # Pin the body shape we send so a regression on the
                # request schema gets caught loudly.
                assert kwargs.get("body") == {"endpoint": endpoint_path}
                return {"token": token, "expire_time": "2099-01-01T00:00:00Z"}
            raise AssertionError(f"unexpected call {method} {path}")

        api = MagicMock()
        api.do.side_effect = do
        api.calls = calls
        return api

    def test_postgres_api_resolution_mints_via_postgres_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        """Autoscaling-only project: must mint via
        ``POST /api/2.0/postgres/credentials`` with the resolved
        endpoint resource. Legacy ``database.generate_database_credential``
        must NEVER be called — it would 404 on this project.
        """
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
        fake_w.database.generate_database_credential.side_effect = (
            AssertionError(
                "Legacy mint must not run for an Autoscaling-only project"
            )
        )
        fake_w.api_client = self._autoscaling_api_client()
        auth._w = fake_w
        assert auth.password() == "POSTGRES-API-TOKEN"
        # Sanity: subsequent call hits the cache, not the SDK.
        fake_w.api_client.do.side_effect = AssertionError(
            "Cached token must not re-mint"
        )
        assert auth.password() == "POSTGRES-API-TOKEN"

    def test_legacy_resolution_still_mints_via_legacy_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        """Provisioned-style instance visible via Database Instance API:
        the legacy path is preserved (no Postgres-API call).
        """
        auth = LakebaseAuth()
        inst = MagicMock(
            read_write_dns=(
                "ep-damp-art-d1l8gclo.database.us-west-2.cloud.databricks.com"
            ),
            read_only_dns="",
        )
        inst.name = "ontobricks-app"
        cred = MagicMock(token="LEGACY-TOKEN")
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = [inst]
        fake_w.database.generate_database_credential.return_value = cred
        fake_w.api_client.do.side_effect = AssertionError(
            "Postgres API must not be called when legacy mint succeeds"
        )
        auth._w = fake_w
        assert auth.password() == "LEGACY-TOKEN"
        fake_w.database.generate_database_credential.assert_called_once()
        kwargs = fake_w.database.generate_database_credential.call_args.kwargs
        assert kwargs["instance_names"] == ["ontobricks-app"]

    def test_legacy_not_found_falls_back_to_postgres_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        """Explicit ``DATABASE_INSTANCE_NAME`` override pointing at an
        Autoscaling-only project: legacy mint 404s, fallback walks
        the Postgres API and retries the mint there.
        """
        monkeypatch.setenv("DATABASE_INSTANCE_NAME", "ontobricks-test")
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.generate_database_credential.side_effect = RuntimeError(
            "Database instance 'ontobricks-test' not found."
        )
        fake_w.api_client = self._autoscaling_api_client(
            project_id="ontobricks-test"
        )
        auth._w = fake_w
        assert auth.password() == "POSTGRES-API-TOKEN"

    def test_legacy_other_error_is_not_retried_via_postgres_api(
        self, monkeypatch, autoscaling_pg_env
    ):
        """Random transient legacy error (e.g. 500, network) must
        bubble up as a ``ValidationError`` rather than silently
        spawning a Postgres-API walk that masks the real failure.
        """
        monkeypatch.setenv("DATABASE_INSTANCE_NAME", "ontobricks-app")
        auth = LakebaseAuth()
        fake_w = MagicMock()
        fake_w.database.generate_database_credential.side_effect = RuntimeError(
            "boom: 500 Internal Server Error"
        )
        fake_w.api_client.do.side_effect = AssertionError(
            "Non-not-found legacy errors must NOT trigger Postgres API "
            "fallback"
        )
        auth._w = fake_w
        with pytest.raises(ValidationError) as excinfo:
            _ = auth.password()
        assert "ontobricks-app" in str(excinfo.value)
        assert "500 Internal Server Error" in str(excinfo.value)
