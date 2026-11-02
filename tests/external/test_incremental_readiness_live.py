"""The readiness and status reports the app serves, computed against a real warehouse.

``GET /dtwin/incremental/readiness`` and ``GET /dtwin/incremental/status`` are thin
routes over :meth:`TwinIncrementalReadiness.report` and
:meth:`TwinIncrementalStatus.report`; this runs both on the external schema's
fixture tables (shared with ``test_mv_incremental_plan.py``) with the real
row-tracking probe, ``information_schema`` lookups, pipeline lookup and
``GraphReadRelation`` decision. Only the R2RML extraction is stubbed: the domain
is a fixture, its mappings are the test's.

Opt-in (``external``): the same environment variables as
``test_mv_incremental_plan.py``.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from tests.external.test_mv_incremental_plan import (  # noqa: F401 — fixtures
    _mappings,
    tables,
    warehouse,
)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


pytestmark = [
    pytest.mark.external,
    pytest.mark.skipif(
        not (
            _env("ONTOBRICKS_EXTERNAL_WAREHOUSE") == "1"
            and _env("ONTOBRICKS_EXTERNAL_SCHEMA")
            and (
                (_env("DATABRICKS_CONFIG_PROFILE") and _env("ONTOBRICKS_EXTERNAL_WAREHOUSE_ID"))
                or (
                    _env("DATABRICKS_HOST")
                    and _env("DATABRICKS_TOKEN")
                    and _env("DATABRICKS_SQL_WAREHOUSE_ID")
                )
            )
        ),
        reason="external: see test_mv_incremental_plan.py for the environment variables",
    ),
]


@pytest.fixture
def domain(tables, monkeypatch):
    """A ``microbatch`` Lakehouse domain on the fixture tables; its R2RML is the test's mappings."""
    from back.objects.digitaltwin.build import TwinIncrementalBuild

    source, _slots, _data = tables
    catalog, schema = _env("ONTOBRICKS_EXTERNAL_SCHEMA").split(".", 1)
    entities, relationships = _mappings(source)
    monkeypatch.setattr(
        TwinIncrementalBuild,
        "mappings_for",
        staticmethod(lambda d: (entities, relationships, "http://ex.org/")),
    )
    return SimpleNamespace(
        info={"name": "ob_it", "materialization_mode": "microbatch", "graph_backend": "databricks"},
        delta={"catalog": catalog, "schema": schema},
        current_version="1",
        graph_engine_config={},
        settings={},  # no registry binding: the global-config lookups answer ""
    )


@pytest.fixture
def settings(warehouse):
    """The app settings the helpers read: the external warehouse's host, token and id."""
    auth = warehouse.auth
    return SimpleNamespace(
        databricks_host=getattr(auth, "host", ""),
        databricks_token=getattr(auth, "token", ""),
        databricks_sql_warehouse_id=getattr(auth, "warehouse_id", ""),
        serverless_pipelines_enabled=True,
        realtime_enabled=False,
    )


class TestReadinessReportLive:
    def test_the_fixture_source_has_row_tracking(self, domain, settings, warehouse):
        from back.objects.digitaltwin.build import TwinIncrementalReadiness

        report = TwinIncrementalReadiness.report(domain, settings, warehouse)

        [source] = [s for s in report["sources"] if s["table"].endswith("ob_it_customers")]
        assert source["row_tracking"] is True
        assert report["compile_error"] is None or report["compile_error"] == ""

    def test_the_mappings_are_classed_as_the_pipeline_would_stream_them(self, domain, settings, warehouse):
        from back.objects.digitaltwin.build import TwinIncrementalReadiness

        report = TwinIncrementalReadiness.report(domain, settings, warehouse)

        kinds = {m["class_uri"]: m["kind"] for m in report["mappings"]}
        assert kinds == {"http://ex.org/Customer": "slot", "http://ex.org/Account": "batch"}
        assert (report["slot_mappings"], report["batch_mappings"]) == (1, 1)
        [batch] = [m for m in report["mappings"] if m["kind"] == "batch"]
        assert batch["reason"] == "sql_query source"

    def test_the_lakehouse_rt_facts_and_the_read_relation_are_reported(self, domain, settings, warehouse):
        from back.objects.digitaltwin.build import TwinIncrementalReadiness

        report = TwinIncrementalReadiness.report(domain, settings, warehouse)

        assert set(report["lakehouse_rt"]) == {"query_warehouse_configured", "use_sea", "fast_path"}
        # no RT toggle, no `_data` view for the fixture domain: reads take `_graph`
        assert report["read_relation"] == "graph"
        assert report["lakehouse_rt"]["fast_path"] == "graph"
        assert "slot_pipeline_channel" in report["previews"]


class TestStatusReportLive:
    def test_the_relations_are_looked_up_in_the_catalog(self, domain, settings, warehouse):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        report = TwinIncrementalStatus.report(domain, settings, warehouse)

        assert report["mode_requested"] == "microbatch"
        assert report["read_relation"] == "graph"
        # the domain's own `_slots` / `_data` (derived from its name) do not exist: both lookups
        # ran against information_schema and answered "absent" without an error
        assert (report["slots"]["kind"], report["data"]["kind"]) == ("absent", "absent")
        assert "error" not in report["slots"] and "error" not in report["data"]
        assert report["freshness_lag_s"] is None
        assert report["lakebase_sync"] is None  # a Lakehouse domain syncs nothing

    def test_the_slot_pipeline_lookup_answers_for_a_domain_without_one(self, domain, settings, warehouse):
        from back.objects.digitaltwin.build import TwinIncrementalStatus

        report = TwinIncrementalStatus.report(domain, settings, warehouse)

        assert report["slot_pipeline"]["name"]
        assert report["slot_pipeline"]["state"] in ("absent", "unknown")
        assert "last_build" in report  # the registry answer, or its error, is reported
