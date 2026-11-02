"""The ``_data`` materialized view plans an incremental refresh on a real warehouse.

``EXPLAIN CREATE MATERIALIZED VIEW`` (``MaterializedViewBuilder.explain_sql``)
of the ``_data`` definition a build would create, compiled from fixture
mappings, must name an incremental technique and no ``FULL_RECOMPUTE``.

Opt-in (``external``): skipped unless ``ONTOBRICKS_EXTERNAL_WAREHOUSE=1``,
``DATABRICKS_CONFIG_PROFILE`` (a logged-in Databricks CLI profile),
``ONTOBRICKS_EXTERNAL_WAREHOUSE_ID`` (a SQL warehouse) and
``ONTOBRICKS_EXTERNAL_SCHEMA`` (``catalog.schema`` the caller may create
tables in) are set::

    ONTOBRICKS_EXTERNAL_WAREHOUSE=1 DATABRICKS_CONFIG_PROFILE=<profile> \\
    ONTOBRICKS_EXTERNAL_WAREHOUSE_ID=<id> ONTOBRICKS_EXTERNAL_SCHEMA=<cat.schema> \\
    uv run --frozen pytest -q -m external tests/external/test_mv_incremental_plan.py

The test creates (if absent) a source table and a Delta table with the slot
columns standing in for the ``_slots`` streaming table; nothing is
materialized (``EXPLAIN`` only).
"""

from __future__ import annotations

import os

import pytest

pytestmark = [
    pytest.mark.external,
    pytest.mark.skipif(
        not (
            os.environ.get("ONTOBRICKS_EXTERNAL_WAREHOUSE") == "1"
            and os.environ.get("DATABRICKS_CONFIG_PROFILE")
            and os.environ.get("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID")
            and os.environ.get("ONTOBRICKS_EXTERNAL_SCHEMA")
        ),
        reason=(
            "external: set ONTOBRICKS_EXTERNAL_WAREHOUSE=1, DATABRICKS_CONFIG_PROFILE, "
            "ONTOBRICKS_EXTERNAL_WAREHOUSE_ID and ONTOBRICKS_EXTERNAL_SCHEMA"
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
    from back.core.databricks import DatabricksClient
    from tests.fixtures.databricks_auth import DatabricksAuth

    profile = os.environ["DATABRICKS_CONFIG_PROFILE"]
    minted = DatabricksAuth.mint_host_and_token(profile)
    if minted is None:
        pytest.skip(f"Could not mint a token from the Databricks CLI profile {profile!r}")
    host, token = minted
    return DatabricksClient(
        host=host, token=token, warehouse_id=os.environ["ONTOBRICKS_EXTERNAL_WAREHOUSE_ID"]
    )


@pytest.fixture(scope="module")
def tables(warehouse):
    """``(source, slots, data)`` FQNs in the external schema; the first two exist."""
    from back.core.graphdb.delta import materialize

    schema = os.environ["ONTOBRICKS_EXTERNAL_SCHEMA"]
    source, slots, data = (f"{schema}.ob_it_{leaf}" for leaf in ("customers", "slots", "data"))
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
