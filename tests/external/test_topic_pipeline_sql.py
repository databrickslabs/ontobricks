"""The topic-fed slot pipeline SQL passes a real Lakeflow SDP validation.

``SlotProjectionCompiler.compile(..., source="topic")`` emits ``read_kafka``
views over a Debezium envelope. This creates a development pipeline whose only
library is that SQL (compiled from fixture mappings, typed values included),
runs a validate-only update — SDP parses and analyses every statement and
resolves the dataflow graph, without reading the topic or writing a table — and
deletes the pipeline and its file again.

Opt-in (``external``): skipped unless ``ONTOBRICKS_EXTERNAL_PIPELINE=1`` and
``ONTOBRICKS_EXTERNAL_SCHEMA`` (the ``catalog.schema`` the pipeline publishes
to) are set, with one of two credential paths:

* a logged-in Databricks CLI profile: ``DATABRICKS_CONFIG_PROFILE``;
* host and token, as the nightly ``external-smoke`` job provides them:
  ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN``.

The topic fields default to placeholders (validation does not connect);
``ONTOBRICKS_EXTERNAL_TOPIC_SECRET_SCOPE`` / ``_KEY`` name a real secret when
the workspace resolves ``secret()`` during analysis::

    ONTOBRICKS_EXTERNAL_PIPELINE=1 ONTOBRICKS_EXTERNAL_SCHEMA=<cat.schema> \\
    DATABRICKS_CONFIG_PROFILE=<profile> \\
    uv run --frozen pytest -q -m external tests/external/test_topic_pipeline_sql.py
"""

from __future__ import annotations

import os
import time
import uuid

import pytest


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or "").strip() or default


def _credentials() -> bool:
    return bool(
        _env("DATABRICKS_CONFIG_PROFILE") or (_env("DATABRICKS_HOST") and _env("DATABRICKS_TOKEN"))
    )


pytestmark = [
    pytest.mark.external,
    pytest.mark.skipif(
        not (
            _env("ONTOBRICKS_EXTERNAL_PIPELINE") == "1"
            and _env("ONTOBRICKS_EXTERNAL_SCHEMA").count(".") == 1
            and _credentials()
        ),
        reason=(
            "external: set ONTOBRICKS_EXTERNAL_PIPELINE=1, ONTOBRICKS_EXTERNAL_SCHEMA=<catalog.schema> "
            "and either DATABRICKS_CONFIG_PROFILE or DATABRICKS_HOST + DATABRICKS_TOKEN"
        ),
    ),
]

_TIMEOUT_S = 900
_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
_DONE = {"COMPLETED", "FAILED", "CANCELED"}


@pytest.fixture(scope="module")
def workspace():
    from databricks.sdk import WorkspaceClient

    profile = _env("DATABRICKS_CONFIG_PROFILE")
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient(host=_env("DATABRICKS_HOST"), token=_env("DATABRICKS_TOKEN"))


def _spec(schema: str):
    """The topic slot pipeline of a customer mapping and a composite-key order mapping."""
    from back.core.streaming import SlotProjectionCompiler, TopicSource

    customers, orders = f"{schema}.ob_it_customers", f"{schema}.ob_it_orders"
    entities = {
        "http://ex.org/Customer": {
            "table": customers,
            "sql_query": None,
            "uri_template": "http://ex.org/Customer/{id}",
            "id_column": "id",
            "label_column": "name",
            "class_uri": "http://ex.org/Customer",
            "predicates": {
                _LABEL: {"type": "column", "column": "name"},
                "http://ex.org/born": {"type": "column", "column": "born"},
                "http://ex.org/balance": {"type": "column", "column": "balance"},
            },
        },
        "http://ex.org/Order": {
            "table": orders,
            "sql_query": None,
            "uri_template": "http://ex.org/Order/{region}/{num}",
            "id_column": "region",
            "label_column": "",
            "class_uri": "http://ex.org/Order",
            "predicates": {"http://ex.org/placed": {"type": "column", "column": "placed"}},
        },
    }
    column_types = {
        customers: {"id": "bigint", "name": "string", "born": "date", "balance": "decimal(12,2)"},
        orders: {"region": "string", "num": "int", "placed": "timestamp"},
    }
    topic = TopicSource(
        bootstrap_servers=_env("ONTOBRICKS_EXTERNAL_TOPIC_SERVERS", "broker.invalid:9093"),
        topic=_env("ONTOBRICKS_EXTERNAL_TOPIC", "ontobricks.it.cdc"),
        secret_scope=_env("ONTOBRICKS_EXTERNAL_TOPIC_SECRET_SCOPE", "ontobricks-it"),
        secret_key=_env("ONTOBRICKS_EXTERNAL_TOPIC_SECRET_KEY", "kafka-jaas"),
    )
    return SlotProjectionCompiler.compile(
        entities,
        [],
        base_uri="http://ex.org/",
        slots_fqn=f"{schema}.ob_it_topic_slots",
        source="topic",
        topic=topic,
        column_types=column_types,
    )


def _errors(workspace, pipeline_id: str) -> list[str]:
    events = workspace.pipelines.list_pipeline_events(
        pipeline_id=pipeline_id, filter="level='ERROR'", max_results=20
    )
    return [str(getattr(event, "message", "") or event) for event in events]


def test_the_topic_pipeline_sql_validates(workspace):
    from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
    from databricks.sdk.service.workspace import ImportFormat

    schema = _env("ONTOBRICKS_EXTERNAL_SCHEMA")
    catalog, schema_name = schema.split(".")
    spec = _spec(schema)
    name = f"ontobricks-it-topic-slots-{uuid.uuid4().hex[:8]}"
    folder = f"/Workspace/Users/{workspace.current_user.me().user_name}/.ontobricks/it"
    path = f"{folder}/{name}.sql"
    workspace.workspace.mkdirs(folder)
    workspace.workspace.upload(
        path, spec.to_sql().encode("utf-8"), format=ImportFormat.AUTO, overwrite=True
    )
    pipeline_id = None
    try:
        pipeline_id = workspace.pipelines.create(
            name=name,
            catalog=catalog,
            schema=schema_name,
            serverless=True,
            continuous=False,
            development=True,
            channel="PREVIEW",
            libraries=[PipelineLibrary(file=FileLibrary(path=path))],
        ).pipeline_id
        update_id = workspace.pipelines.start_update(
            pipeline_id=pipeline_id, validate_only=True
        ).update_id
        deadline, state = time.monotonic() + _TIMEOUT_S, ""
        while time.monotonic() < deadline:
            update = workspace.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
            state = str(getattr(update.update.state, "value", update.update.state))
            if state in _DONE:
                break
            time.sleep(10)

        assert state == "COMPLETED", (state, _errors(workspace, pipeline_id))
    finally:
        if pipeline_id:
            workspace.pipelines.delete(pipeline_id=pipeline_id)
        workspace.workspace.delete(path)
