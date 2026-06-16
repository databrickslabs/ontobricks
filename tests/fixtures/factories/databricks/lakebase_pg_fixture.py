"""Ephemeral Postgres for Lakebase tests (`db` marker).

Thin wrapper around `testcontainers.postgres.PostgresContainer`. Importing this
module is cheap; the container is only created when the fixture is requested.

Usage:

    from tests.fixtures.factories.databricks.lakebase_pg_fixture import lakebase_pg

    @pytest.mark.db
    def test_registry_in_lakebase(lakebase_pg):
        conn = lakebase_pg.connection()
        ...

If `testcontainers` is not installed, the fixture skips the test rather than
erroring — keeps `db`-marked tests from gating PRs in environments without
Docker.
"""

from __future__ import annotations

import os
import pytest


@pytest.fixture(scope="session")
def lakebase_pg(request):
    """Ephemeral Postgres container; session-scoped (reused across `db`-marked tests).

    Yields an object with `.connection_url()` returning a psycopg-compatible DSN.
    Skips the test if `testcontainers` is missing or Docker is not running.
    """
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:
        pytest.skip("testcontainers not installed — install dev-deps for db-marker tests")

    if os.environ.get("ONTOBRICKS_SKIP_TESTCONTAINERS") == "1":
        pytest.skip("ONTOBRICKS_SKIP_TESTCONTAINERS=1 set; skipping db-marker test")

    container = PostgresContainer(image="postgres:16-alpine").with_env("POSTGRES_DB", "ontobricks_test")
    try:
        container.start()
    except Exception as exc:  # pragma: no cover — Docker missing is a CI/local issue
        pytest.skip(f"could not start Postgres container ({exc!r}); install Docker or set ONTOBRICKS_SKIP_TESTCONTAINERS=1")

    class _Handle:
        def connection_url(self) -> str:
            return container.get_connection_url()

        def host(self) -> str:
            return container.get_container_host_ip()

        def port(self) -> int:
            return int(container.get_exposed_port(container.port))

    request.addfinalizer(container.stop)
    yield _Handle()
