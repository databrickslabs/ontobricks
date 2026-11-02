"""``TwinIncrementalReadiness``: the readiness preflight and the row-tracking fix."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.digitaltwin.build import TwinIncrementalBuild, TwinIncrementalReadiness
from tests.units.objects import test_build_pipeline_modes as _modes

pytestmark = pytest.mark.unit

_CUSTOMERS = "main.sales.customers"
_ORDERS = "main.sales.orders"


def _domain(mode="incremental"):
    return SimpleNamespace(
        info={"name": "dom", "materialization_mode": mode, "graph_backend": "databricks"},
        current_version=3,
        delta={"catalog": "cat", "schema": "sch"},
        uc_domain_folder="dom",
    )


class _Client:
    """Answers the row-tracking probe from *tracked*; records and optionally fails ALTERs."""

    def __init__(self, tracked=None, *, probe_error=None, alter_error_on=()):
        self.tracked = dict(tracked or {})
        self.queries: list[str] = []
        self.statements: list[str] = []
        self._probe_error = probe_error
        self._alter_error_on = tuple(alter_error_on)

    def execute_query(self, sql):
        self.queries.append(sql)
        if self._probe_error is not None:
            raise self._probe_error
        table = sql.split()[2]
        return [{"key": "delta.enableRowTracking", "value": str(self.tracked.get(table, False)).lower()}]

    def execute_statement(self, sql):
        self.statements.append(sql)
        if any(t in sql for t in self._alter_error_on):
            raise RuntimeError("PERMISSION_DENIED: not the table owner")
        return True


@pytest.fixture(autouse=True)
def lakehouse_config(monkeypatch):
    """The Lakehouse query warehouse and RT toggle the global config holds (none, off)."""
    from back.core.helpers import DatabricksHelpers

    config = SimpleNamespace(warehouse_id="", use_sea=False)
    monkeypatch.setattr(
        DatabricksHelpers,
        "resolve_lakehouse_query_warehouse_id",
        staticmethod(lambda d, s: config.warehouse_id),
    )
    monkeypatch.setattr(
        DatabricksHelpers, "resolve_lakehouse_use_sea", staticmethod(lambda d, s: config.use_sea)
    )
    return config


@pytest.fixture
def two_sources(monkeypatch):
    """The domain's mappings: two row-local entity mappings on two tables."""
    other = {**_modes._row_local(), "table": _ORDERS, "class_uri": _modes._ORDER}
    entities = {_modes._CUSTOMER: _modes._row_local(), _modes._ORDER: other}
    monkeypatch.setattr(
        TwinIncrementalBuild,
        "mappings_for",
        staticmethod(lambda domain: (entities, [], "http://ex.org/")),
    )


class TestSourceTables:
    def test_the_row_local_sources_of_the_domain(self, two_sources):
        assert TwinIncrementalReadiness.source_tables(_domain(), None) == sorted([_CUSTOMERS, _ORDERS])

    def test_a_domain_without_r2rml_has_none(self, monkeypatch):
        monkeypatch.setattr(TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: None))

        assert TwinIncrementalReadiness.source_tables(_domain(), None) == []


class TestReport:
    def test_an_untracked_source_keeps_the_domain_on_full(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: False})

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), client)

        assert report["mode_requested"] == "incremental"
        assert report["decision"] == {
            "effective": "full",
            "reason": "row_tracking_missing",
            "merge_fallback": False,
        }
        assert report["compile_error"] == ""
        assert report["sources"] == [
            {"table": _CUSTOMERS, "row_tracking": True},
            {"table": _ORDERS, "row_tracking": False},
        ]
        assert report["row_tracking_required"] is True
        assert report["serverless_available"] is True
        assert report["previews"] == {"slot_pipeline_channel": "PREVIEW"}

    def test_tracked_sources_keep_the_requested_mode(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: True})

        report = TwinIncrementalReadiness.report(_domain("microbatch"), SimpleNamespace(), client)

        assert report["decision"] == {
            "effective": "microbatch",
            "reason": "",
            "merge_fallback": False,
        }

    def test_without_serverless_the_gate_says_why(self, two_sources):
        settings = SimpleNamespace(serverless_pipelines_enabled=False)

        report = TwinIncrementalReadiness.report(_domain(), settings, _Client({_CUSTOMERS: True}))

        assert report["serverless_available"] is False
        assert report["decision"] == {
            "effective": "incremental",
            "reason": "serverless_unavailable",
            "merge_fallback": True,
        }

    def test_mappings_that_do_not_compile_are_reported_not_raised(self, monkeypatch):
        from back.core.errors import ValidationError as _ValidationError

        def broken(domain, settings, client, mappings):
            raise _ValidationError("Entity mapping has no class_uri")

        monkeypatch.setattr(TwinIncrementalReadiness, "_spec", staticmethod(broken))

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Client())

        assert report["compile_error"] == "Entity mapping has no class_uri"
        assert report["decision"] == {
            "effective": "full",
            "reason": "incremental_failed:ValidationError",
            "merge_fallback": False,
        }
        assert report["sources"] == []

    def test_a_failed_probe_is_an_infrastructure_error(self, two_sources):
        with pytest.raises(InfrastructureError):
            TwinIncrementalReadiness.report(
                _domain(), SimpleNamespace(), _Client(probe_error=RuntimeError("no USE SCHEMA"))
            )


_PLACED_BY = "http://ex.org/placedBy"


def _join() -> dict:
    """A table-based entity mapping with a reference predicate (a join)."""
    return {
        **_modes._row_local(),
        "predicates": {_PLACED_BY: {"type": "reference", "parent_map": "#CustomerMap"}},
    }


def _relationship() -> dict:
    """A table-based relationship mapping (always batch)."""
    return {
        "predicate": _PLACED_BY,
        "table": _ORDERS,
        "subject_template": "http://ex.org/Order/{order_id}",
        "object_template": "http://ex.org/Customer/{customer_id}",
        "subject_column": "order_id",
        "object_column": "customer_id",
    }


class TestMappingClasses:
    def test_a_row_local_mapping_is_a_slot_mapping_with_its_map_id(self):
        from back.core.streaming import SlotProjectionCompiler

        [entry] = TwinIncrementalReadiness.mapping_classes({_modes._CUSTOMER: _modes._row_local()}, [])

        expected_id = SlotProjectionCompiler.map_id(_modes._row_local())
        assert entry == {
            "class_uri": _modes._CUSTOMER,
            "predicate": None,
            "map_id": expected_id,
            "kind": "slot",
            "reason": "",
        }

    @pytest.mark.parametrize(
        ("entities", "relationships", "reason"),
        [
            ({_modes._CUSTOMER: _join()}, [], "reference predicate (join)"),
            ({_modes._ORDER: _modes._batch()}, [], "sql_query source"),
            ({}, [_relationship()], "relationship (several objects per subject share one slot key)"),
        ],
        ids=["join", "sql_query", "relationship"],
    )
    def test_a_batch_mapping_says_why(self, entities, relationships, reason):
        [entry] = TwinIncrementalReadiness.mapping_classes(entities, relationships)

        assert (entry["kind"], entry["map_id"], entry["reason"]) == ("batch", None, reason)

    def test_the_slot_entries_are_exactly_the_mappings_the_pipeline_streams(self):
        """``slot`` in the panel means streamed: the compiled spec's row-local map ids."""
        orders = {**_modes._row_local(), "table": _ORDERS, "uri_template": "http://ex.org/Order/{id}"}
        entities = {
            _modes._CUSTOMER: _modes._row_local(),
            _modes._ORDER: orders,
            "http://ex.org/Query": _modes._batch(),
            "http://ex.org/Joined": _join(),
        }
        spec = TwinIncrementalBuild.compile_for(
            _domain(), SimpleNamespace(), entities, [_relationship()], base_uri="http://ex.org/"
        )

        classes = TwinIncrementalReadiness.mapping_classes(entities, [_relationship()])

        assert [m["map_id"] for m in classes if m["kind"] == "slot"] == spec.row_local_map_ids
        assert sum(m["kind"] == "batch" for m in classes) == len(spec.batch_mappings) == 3

    def test_a_relationship_is_named_by_its_predicate(self):
        [entry] = TwinIncrementalReadiness.mapping_classes({}, [_relationship()])

        assert (entry["class_uri"], entry["predicate"]) == (None, _PLACED_BY)

    def test_the_report_lists_and_counts_them(self, monkeypatch):
        entities = {_modes._CUSTOMER: _modes._row_local(), _modes._ORDER: _modes._batch()}
        monkeypatch.setattr(
            TwinIncrementalBuild,
            "mappings_for",
            staticmethod(lambda d: (entities, [_relationship()], "http://ex.org/")),
        )

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Client({_CUSTOMERS: True}))

        assert [m["kind"] for m in report["mappings"]] == ["slot", "batch", "batch"]
        assert (report["slot_mappings"], report["batch_mappings"]) == (1, 2)

    def test_mappings_that_do_not_compile_list_nothing_and_count_nothing(self, monkeypatch):
        def broken(domain, settings, client, mappings):
            raise ValidationError("Entity mapping has no class_uri")

        monkeypatch.setattr(TwinIncrementalReadiness, "_spec", staticmethod(broken))

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Client())

        assert (report["mappings"], report["slot_mappings"], report["batch_mappings"]) == ([], None, None)

    def test_a_domain_without_r2rml_has_no_mappings(self, monkeypatch):
        monkeypatch.setattr(TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: None))

        report = TwinIncrementalReadiness.report(_domain(), SimpleNamespace(), _Client())

        assert (report["mappings"], report["slot_mappings"], report["batch_mappings"]) == ([], 0, 0)


class TestLakehouseRTFacts:
    def test_a_lakehouse_domain_reports_the_query_warehouse_and_the_toggle(self, lakehouse_config):
        lakehouse_config.warehouse_id, lakehouse_config.use_sea = "wh-rt", True

        facts = TwinIncrementalReadiness.lakehouse_rt_facts(_domain(), SimpleNamespace(), "slots_live")

        assert facts == {"query_warehouse_configured": True, "use_sea": True, "fast_path": "slots_live"}

    def test_nothing_configured_is_reported_as_off(self):
        facts = TwinIncrementalReadiness.lakehouse_rt_facts(_domain(), SimpleNamespace(), "graph")

        assert facts == {"query_warehouse_configured": False, "use_sea": False, "fast_path": "graph"}

    def test_another_engine_has_none(self):
        domain = _domain()
        domain.info["graph_backend"] = "lakebase"

        assert TwinIncrementalReadiness.lakehouse_rt_facts(domain, SimpleNamespace(), "graph") is None

    def test_the_report_carries_them_with_its_read_relation(self, two_sources):
        report = TwinIncrementalReadiness.report(
            _domain(), SimpleNamespace(), _Client({_CUSTOMERS: True, _ORDERS: True})
        )

        assert report["lakehouse_rt"]["fast_path"] == report["read_relation"] == "graph"


class TestEnableRowTracking:
    def test_the_untracked_sources_are_altered(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: False})

        result = TwinIncrementalReadiness.enable_row_tracking(_domain(), None, client)

        assert result == {"tables": [{"table": _ORDERS, "success": True}]}
        assert client.statements == [
            f"ALTER TABLE {_ORDERS} SET TBLPROPERTIES "
            "(delta.enableRowTracking = true, delta.enableDeletionVectors = true)"
        ]

    def test_a_listed_subset_is_altered(self, two_sources):
        client = _Client()

        result = TwinIncrementalReadiness.enable_row_tracking(
            _domain(), None, client, [_CUSTOMERS.upper()]
        )

        assert result == {"tables": [{"table": _CUSTOMERS, "success": True}]}

    def test_a_table_outside_the_domain_is_refused_before_anything_runs(self, two_sources):
        client = _Client()

        with pytest.raises(ValidationError, match="main.hr.salaries"):
            TwinIncrementalReadiness.enable_row_tracking(
                _domain(), None, client, [_CUSTOMERS, "main.hr.salaries"]
            )
        assert client.statements == []

    def test_a_refused_alter_is_reported_per_table(self, two_sources):
        client = _Client(alter_error_on=(_ORDERS,))

        result = TwinIncrementalReadiness.enable_row_tracking(
            _domain(), None, client, [_CUSTOMERS, _ORDERS]
        )

        assert result["tables"][0] == {"table": _CUSTOMERS, "success": True}
        assert result["tables"][1]["success"] is False
        assert "PERMISSION_DENIED" in result["tables"][1]["error"]


class TestRealtimeFacts:
    _SOURCE = {
        "kind": "kafka",
        "bootstrap_servers": "b:9092",
        "topic": "sales",
        "secret_scope": "s",
        "secret_key": "k",
    }

    def test_a_configured_topic_with_the_setting_on(self):
        domain = SimpleNamespace(info={"realtime_source": self._SOURCE})

        facts = TwinIncrementalReadiness.realtime_facts(domain, SimpleNamespace(realtime_enabled=True))

        assert facts == {
            "enabled": True,
            "backend_eligible": True,
            "source_configured": True,
            "kind": "kafka",
            "experimental": False,
            "topic": "sales",
        }

    def test_a_non_lakebase_domain_is_not_eligible_and_kinesis_is_experimental(self):
        domain = SimpleNamespace(
            info={"graph_backend": "neo4j", "realtime_source": {**self._SOURCE, "kind": "kinesis"}}
        )

        facts = TwinIncrementalReadiness.realtime_facts(domain, SimpleNamespace(realtime_enabled=True))

        assert facts["backend_eligible"] is False and facts["experimental"] is True

    def test_nothing_configured(self):
        facts = TwinIncrementalReadiness.realtime_facts(_domain(), SimpleNamespace())

        assert facts == {
            "enabled": False,
            "backend_eligible": False,
            "source_configured": False,
            "kind": "",
            "experimental": False,
            "topic": "",
        }

    def test_the_report_carries_them(self, two_sources):
        report = TwinIncrementalReadiness.report(
            _domain("realtime"), SimpleNamespace(), _Client({_CUSTOMERS: True, _ORDERS: True})
        )

        assert report["realtime"]["enabled"] is False
        assert report["decision"]["reason"] == "realtime_not_available"

    def _topic_domain(self, kind="kafka"):
        return SimpleNamespace(
            info={
                "name": "dom",
                "materialization_mode": "realtime",
                "graph_backend": "lakebase",
                "realtime_source": {**self._SOURCE, "kind": kind},
            },
            current_version=3,
            delta={"catalog": "cat", "schema": "sch"},
            uc_domain_folder="dom",
        )

    def test_a_topic_fed_pipeline_needs_no_row_tracking(self, two_sources):
        client = _Client({_CUSTOMERS: False, _ORDERS: False})

        report = TwinIncrementalReadiness.report(
            self._topic_domain(), SimpleNamespace(realtime_enabled=True), client
        )

        assert report["decision"] == {"effective": "realtime", "reason": "", "merge_fallback": False}
        assert report["row_tracking_required"] is False
        assert report["sources"] == [
            {"table": _CUSTOMERS, "row_tracking": None},
            {"table": _ORDERS, "row_tracking": None},
        ]
        assert not any("enableRowTracking" in sql for sql in client.queries)

    def test_a_topic_fed_domain_s_sources_are_never_altered(self, two_sources):
        settings = SimpleNamespace(realtime_enabled=True)
        client = _Client({_CUSTOMERS: False, _ORDERS: False})

        result = TwinIncrementalReadiness.enable_row_tracking(self._topic_domain(), settings, client)

        assert result == {"tables": []} and client.statements == []
        with pytest.raises(ValidationError, match="Not a source table"):
            TwinIncrementalReadiness.enable_row_tracking(
                self._topic_domain(), settings, client, tables=[_CUSTOMERS]
            )

    def test_a_kinesis_stream_keeps_the_change_feed_and_its_probe(self, two_sources):
        client = _Client({_CUSTOMERS: True, _ORDERS: False})

        report = TwinIncrementalReadiness.report(
            self._topic_domain("kinesis"), SimpleNamespace(realtime_enabled=True), client
        )

        assert report["row_tracking_required"] is True
        assert report["decision"] == {
            "effective": "full",
            "reason": "row_tracking_missing",
            "merge_fallback": False,
        }
