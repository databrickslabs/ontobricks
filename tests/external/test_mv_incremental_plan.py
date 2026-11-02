"""The ``_data`` materialized view plans an incremental refresh on a real warehouse.

``EXPLAIN CREATE MATERIALIZED VIEW`` (``MaterializedViewBuilder.explain_sql``)
of the ``_data`` definition a build would create, compiled from fixture
mappings, must name an incremental technique and no ``FULL_RECOMPUTE``.

Opt-in (``external``): skipped unless ``ONTOBRICKS_EXTERNAL_WAREHOUSE=1`` and
``ONTOBRICKS_EXTERNAL_SCHEMA`` (``catalog.schema`` the caller may create tables
in) are set, with one of two credential paths:

* a logged-in Databricks CLI profile: ``DATABRICKS_CONFIG_PROFILE`` and
  ``ONTOBRICKS_EXTERNAL_WAREHOUSE_ID``;
* host and token, as the nightly ``external-smoke`` job provides them:
  ``DATABRICKS_HOST``, ``DATABRICKS_TOKEN`` and ``DATABRICKS_SQL_WAREHOUSE_ID``.

::

    ONTOBRICKS_EXTERNAL_WAREHOUSE=1 ONTOBRICKS_EXTERNAL_SCHEMA=<cat.schema> \\
    DATABRICKS_CONFIG_PROFILE=<profile> ONTOBRICKS_EXTERNAL_WAREHOUSE_ID=<id> \\
    uv run --frozen pytest -q -m external tests/external/test_mv_incremental_plan.py

The test creates (if absent) the schema, a source table and a Delta table with
the slot columns standing in for the ``_slots`` streaming table; nothing is
materialized (``EXPLAIN`` only).
"""

from __future__ import annotations

import os

import pytest


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _profile_credentials() -> bool:
    return bool(_env("DATABRICKS_CONFIG_PROFILE") and _env("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID"))


def _token_credentials() -> bool:
    return bool(
        _env("DATABRICKS_HOST") and _env("DATABRICKS_TOKEN") and _env("DATABRICKS_SQL_WAREHOUSE_ID")
    )


pytestmark = [
    pytest.mark.external,
    pytest.mark.skipif(
        not (
            _env("ONTOBRICKS_EXTERNAL_WAREHOUSE") == "1"
            and _env("ONTOBRICKS_EXTERNAL_SCHEMA")
            and (_profile_credentials() or _token_credentials())
        ),
        reason=(
            "external: set ONTOBRICKS_EXTERNAL_WAREHOUSE=1, ONTOBRICKS_EXTERNAL_SCHEMA and "
            "either DATABRICKS_CONFIG_PROFILE + ONTOBRICKS_EXTERNAL_WAREHOUSE_ID or "
            "DATABRICKS_HOST + DATABRICKS_TOKEN + DATABRICKS_SQL_WAREHOUSE_ID"
        ),
    ),
]

_PROPERTIES = (
    "TBLPROPERTIES (delta.enableRowTracking = true, delta.enableDeletionVectors = true, "
    "delta.enableChangeDataFeed = true)"
)
_RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


@pytest.fixture(scope="module")
def warehouse():
    """A SQL client on the external warehouse, from a CLI profile or host + token."""
    from back.core.databricks import DatabricksClient
    from tests.fixtures.databricks_auth import DatabricksAuth

    if _profile_credentials():
        profile = _env("DATABRICKS_CONFIG_PROFILE")
        minted = DatabricksAuth.mint_host_and_token(profile)
        if minted is None:
            pytest.skip(f"Could not mint a token from the Databricks CLI profile {profile!r}")
        host, token = minted
        warehouse_id = _env("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID")
    else:
        host, token = _env("DATABRICKS_HOST"), _env("DATABRICKS_TOKEN")
        warehouse_id = _env("DATABRICKS_SQL_WAREHOUSE_ID")
    return DatabricksClient(host=host, token=token, warehouse_id=warehouse_id)


@pytest.fixture(scope="module")
def tables(warehouse):
    """``(source, slots, data)`` FQNs in the external schema; the first two exist."""
    from back.core.graphdb.delta import materialize

    schema = _env("ONTOBRICKS_EXTERNAL_SCHEMA")
    source, slots, data = (f"{schema}.ob_it_{leaf}" for leaf in ("customers", "slots", "data"))
    materialize.run_sql(warehouse, f"CREATE SCHEMA IF NOT EXISTS {schema}")
    materialize.run_sql(
        warehouse,
        f"CREATE TABLE IF NOT EXISTS {source} (id STRING, name STRING) {_PROPERTIES}",
    )
    materialize.run_sql(
        warehouse,
        f"CREATE TABLE IF NOT EXISTS {slots} (map_id STRING, source_key STRING, "
        f"predicate STRING, object STRING, subject STRING) {_PROPERTIES}",
    )
    return source, slots, data


def _mappings(source: str) -> tuple:
    entity = {
        "table": source,
        "sql_query": None,
        "uri_template": "http://ex.org/Customer/{id}",
        "id_column": "id",
        "label_column": "name",
        "class_uri": "http://ex.org/Customer",
        "predicates": {_RDFS_LABEL: {"type": "column", "column": "name"}},
    }
    batch = {
        **entity,
        "table": None,
        "sql_query": f"SELECT id, name FROM {source}",
        "uri_template": "http://ex.org/Account/{id}",
        "class_uri": "http://ex.org/Account",
    }
    return {entity["class_uri"]: entity, batch["class_uri"]: batch}, []


def test_the_data_view_plans_an_incremental_refresh(warehouse, tables):
    from back.core.graphdb.delta import MaterializedViewBuilder
    from back.core.helpers import WarehouseStatements
    from back.core.streaming import SlotProjectionCompiler
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    source, slots, data = tables
    entities, relationships = _mappings(source)
    spec = SlotProjectionCompiler.compile(
        entities, relationships, base_uri="http://ex.org/", slots_fqn=slots
    )
    definition = TwinIncrementalBuild.data_definition_for(
        spec, entities, relationships, "incremental"
    )
    assert definition["slots_fqn"] == slots and definition["batch_branch_sql"]

    explain = MaterializedViewBuilder.explain_sql(
        MaterializedViewBuilder.data_mv_sql(data, **definition)
    )
    rows = WarehouseStatements.query(warehouse, explain, f"explain {data}")

    assert MaterializedViewBuilder.plan_is_incremental(rows), rows
