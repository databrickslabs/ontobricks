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
        # back to PGAPPNAME. Force the SDK lookup path to fail loudly
        # so the test asserts on its error rather than a silent
        # PGAPPNAME read.
        fake_w = MagicMock()
        fake_w.database.list_database_instances.return_value = []
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
