"""Materialization facts persisted per build run, and the builds that read them back.

* **Persisted trace.** ``build_runs.stats["materialization"]`` carries the ten
  flat trace keys plus ``data_table_version``, ``fingerprint``, ``strategy``,
  ``apply_mode`` and ``apply_fallback_reason``; a later build finds its base
  run through ``TwinBuildPostMaterialize.previous_materialization_stats``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin.build import TwinBuildPostMaterialize
from back.objects.registry.RegistryService import RegistryService
from tests.units.objects import test_build_pipeline_modes as _modes
from tests.units.objects.test_build_pipeline_materialize import _DATA, _VIEW, _bare_pipeline

pytestmark = pytest.mark.unit

_TRACE_KEYS = {
    "triples_source",
    "inferred_reset",
    "materialization_mode_requested",
    "materialization_mode_effective",
    "mode_fallback_reason",
    "refresh_technique",
    "slot_pipeline_id",
    "slots_table",
    "sync_source",
    "sync_mode_effective",
}
_FACT_KEYS = {
    "data_table_version",
    "fingerprint",
    "strategy",
    "apply_mode",
    "apply_fallback_reason",
}


@pytest.fixture(autouse=True)
def _no_databricks_sdk(monkeypatch):
    """Keep every build here off the Databricks SDK (shape of the compute-once guard).

    The slot pipeline runner is the scripted fake from the modes tests, and the
    SDK constructor records any attempt so the teardown can fail loudly
    (``SlotPipelineRunner._w`` would swallow a raising guard).
    """
    _modes._FakeRunner.script = _modes._Script([])
    monkeypatch.setattr("back.core.streaming.SlotPipelineRunner", _modes._FakeRunner)
    attempts: list = []
    monkeypatch.setattr(
        "databricks.sdk.WorkspaceClient",
        lambda *a, **kw: attempts.append((a, kw)) or MagicMock(),
    )
    yield
    assert attempts == [], "unit tests must not build a WorkspaceClient"


@pytest.fixture(autouse=True)
def registry(monkeypatch):
    """The registry every build reads and writes: no Lakebase connection, ever."""
    reg = MagicMock()
    reg.load_build_runs.return_value = []
    monkeypatch.setattr(RegistryService, "from_context", staticmethod(lambda *a, **kw: reg))
    return reg


def _snap() -> SimpleNamespace:
    return SimpleNamespace(current_version=3, ontology={}, assignment={})


def _entry(registry, pipe) -> dict:
    """Run *pipe* (real count + stream, real trace writer); return the entry it persisted."""
    del pipe._record_build_run
    del pipe._apply_full_rebuild
    pipe.domain_snap = _snap()
    pipe.source_client = _modes._Warehouse([])
    pipe.store.bulk_load_into_sync = MagicMock(return_value=5)
    pipe.run()
    return registry.record_build_run.call_args_list[0][0][1]


def _run_record(*, status="success", apply_mode="full_stream", version=5, **identity) -> dict:
    """A persisted build run of the bare pipeline's store (``G_V3`` on app-managed Lakebase)."""
    run = {
        "status": status,
        "graph_engine": "lakebase",
        "sync_mode": "app_managed",
        "graph_name": "G_V3",
        "triple_count": 100,
        "stats": {
            "materialization": {
                "apply_mode": apply_mode,
                "data_table_version": version,
                "fingerprint": f"fp-{version}",
                "strategy": "mv",
            }
        },
    }
    run.update(identity)
    return run


def _previous(registry, runs) -> dict:
    registry.load_build_runs.return_value = runs
    pipe = _bare_pipeline()
    pipe.domain_snap = _snap()
    return TwinBuildPostMaterialize.previous_materialization_stats(pipe)


class TestBuildRunStats:
    def test_the_cockpit_counts_stay_next_to_the_materialization_facts(self):
        pipe = SimpleNamespace(
            view_table=_VIEW,
            graph_name="G_V3",
            domain_snap=SimpleNamespace(ontology={"classes": [{}, {}]}, assignment={}),
        )

        stats = TwinBuildPostMaterialize.build_run_stats(pipe)

        assert stats["ontology"]["classes"] == 2
        assert set(stats["materialization"]) == _TRACE_KEYS | _FACT_KEYS

    def test_a_pipeline_that_did_nothing_reports_empty_facts(self):
        facts = TwinBuildPostMaterialize.build_run_stats(SimpleNamespace(view_table=_VIEW))[
            "materialization"
        ]

        assert {key: facts[key] for key in _FACT_KEYS} == {
            "data_table_version": None,
            "fingerprint": "",
            "strategy": "",
            "apply_mode": "",
            "apply_fallback_reason": "",
        }

    @pytest.mark.parametrize(
        ("attrs", "strategy"),
        [
            ({"data_table": _DATA, "data_relation_mode": "table"}, "ctas"),
            ({"data_table": _DATA, "data_relation_mode": "materialized_view"}, "mv"),
            ({"data_table": _DATA, "data_relation_mode": "view"}, ""),
            ({"data_table": _DATA, "data_relation_mode": "table", "strategy": "merge"}, "merge"),
            ({"data_table": "", "data_relation_mode": "table"}, ""),
        ],
        ids=["ctas", "mv", "pass-through", "merge", "nothing-built"],
    )
    def test_the_strategy_says_how_the_data_relation_was_built(self, attrs, strategy):
        pipe = SimpleNamespace(view_table=_VIEW, **attrs)

        facts = TwinBuildPostMaterialize.build_run_stats(pipe)["materialization"]

        assert facts["strategy"] == strategy

    @pytest.mark.parametrize("value", [True, "7", 7.0, MagicMock()])
    def test_a_version_that_is_not_an_integer_is_dropped(self, value):
        pipe = SimpleNamespace(view_table=_VIEW, data_table_version=value)

        facts = TwinBuildPostMaterialize.build_run_stats(pipe)["materialization"]

        assert facts["data_table_version"] is None


class TestPersistedEntry:
    def test_a_lakebase_full_build_persists_a_full_stream_over_a_ctas(self, registry):
        entry = _entry(registry, _bare_pipeline())

        facts = entry["stats"]["materialization"]
        assert (facts["strategy"], facts["apply_mode"], facts["data_table_version"]) == (
            "ctas",
            "full_stream",
            None,
        )
        assert facts["triples_source"] == entry["triples_source"] == _DATA

    def test_a_databricks_build_persists_no_store_apply(self, registry):
        entry = _entry(registry, _bare_pipeline(info={"graph_backend": "databricks"}))

        assert entry["stats"]["materialization"]["apply_mode"] == "none"

    def test_the_store_identity_and_the_flat_trace_keys_stay_top_level(self, registry):
        entry = _entry(registry, _bare_pipeline())

        assert (entry["graph_engine"], entry["sync_mode"], entry["graph_name"]) == (
            "lakebase",
            "app_managed",
            "G_V3",
        )
        assert _TRACE_KEYS <= set(entry)
        assert entry["version"] == "3"


class TestBuildRunKey:
    def test_the_uc_folder_and_the_snapshot_version(self):
        pipe = _bare_pipeline()
        pipe.domain_snap = _snap()

        assert TwinBuildPostMaterialize.build_run_key(pipe) == ("dom", "3")

    def test_without_a_uc_folder_the_sanitised_name(self):
        pipe = _bare_pipeline()
        pipe.domain.uc_domain_folder = ""
        pipe.domain_name = "My Domain"
        pipe.domain_snap = SimpleNamespace(current_version=None)
        pipe.domain.current_version = None

        folder, version = TwinBuildPostMaterialize.build_run_key(pipe)

        assert folder and folder != "My Domain"
        assert version == ""


class TestPreviousMaterializationStats:
    def test_the_newest_success_on_the_same_store_is_the_base(self, registry):
        facts = _previous(registry, [_run_record(version=9), _run_record(version=4)])

        assert facts["data_table_version"] == 9
        registry.load_build_runs.assert_called_once_with("dom", version="3", limit=10)

    @pytest.mark.parametrize("apply_mode", ["net_changes", "full_stream", "lakeflow_sync"])
    def test_every_apply_that_synced_the_store_is_a_base(self, registry, apply_mode):
        assert _previous(registry, [_run_record(apply_mode=apply_mode)])["apply_mode"] == apply_mode

    @pytest.mark.parametrize(
        "identity",
        [{"graph_engine": "neo4j"}, {"sync_mode": "managed_synced"}, {"graph_name": "G_V2"}],
        ids=["engine", "sync-mode", "graph"],
    )
    def test_a_success_on_another_store_is_passed_over(self, registry, identity):
        facts = _previous(registry, [_run_record(version=9, **identity), _run_record(version=4)])

        assert facts["data_table_version"] == 4

    @pytest.mark.parametrize("apply_mode", ["none", ""], ids=["skipped", "empty"])
    def test_a_success_that_left_the_store_alone_is_passed_over(self, registry, apply_mode):
        facts = _previous(
            registry, [_run_record(version=9, apply_mode=apply_mode), _run_record(version=4)]
        )

        assert facts["data_table_version"] == 4

    def test_a_failure_before_the_apply_is_passed_over(self, registry):
        facts = _previous(
            registry, [_run_record(status="error", apply_mode=""), _run_record(version=4)]
        )

        assert facts["data_table_version"] == 4

    @pytest.mark.parametrize("status", ["error", "cancelled"])
    def test_a_failure_during_the_apply_leaves_no_base(self, registry, status):
        runs = [_run_record(status=status, apply_mode="full_stream"), _run_record(version=4)]

        assert _previous(registry, runs) == {}

    def test_a_run_without_materialization_facts_leaves_no_base(self, registry):
        scheduled = {"status": "success", "graph_engine": "", "sync_mode": "", "stats": {}}

        assert _previous(registry, [scheduled, _run_record(version=4)]) == {}

    def test_no_runs_no_base(self, registry):
        assert _previous(registry, []) == {}

    def test_a_registry_failure_reads_as_no_base(self, registry):
        registry.load_build_runs.side_effect = RuntimeError("registry down")
        pipe = _bare_pipeline()
        pipe.domain_snap = _snap()

        with patch("back.objects.digitaltwin.build.TwinBuildPostMaterialize.logger") as logger:
            assert TwinBuildPostMaterialize.previous_materialization_stats(pipe) == {}
        logger.debug.assert_called_once()

    def test_an_unexpected_answer_reads_as_no_base(self, registry):
        assert _previous(registry, MagicMock()) == {}

    def test_an_unknown_version_reads_nothing(self, registry):
        pipe = _bare_pipeline()
        pipe.domain_snap = SimpleNamespace(current_version=None)
        pipe.domain.current_version = None

        assert TwinBuildPostMaterialize.previous_materialization_stats(pipe) == {}
        registry.load_build_runs.assert_not_called()
