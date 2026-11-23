"""Certified build: compose in Delta, then load the Context backend from the composition."""

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.build import GraphLoad, SyncedGraphLoad
from back.objects.digitaltwin.certify import CertifiedBuild

_BASE = "cat.sch.triplestore_dom_V2_cert_base"
_CERTIFIED = "cat.sch.triplestore_dom_V2_certified"
_SYNC_SOURCE = "cat.sch.triplestore_dom_V2_cert_sync"


def _domain():
    return SimpleNamespace(
        info={"name": "Dom"}, current_version="2", uc_domain_folder="dom"
    )


def _builder(certified_count: int = 42):
    builder = MagicMock()
    builder.cert_base_fqn.return_value = _BASE
    builder.certified_view_fqn.return_value = _CERTIFIED
    builder.cert_sync_source_fqn.return_value = _SYNC_SOURCE
    builder.count_triples.return_value = certified_count
    return builder


class _Harness:
    """Patch the factory, the registry and the warehouse client around one run."""

    def __init__(self, *, backend: str, materialization: str = "view", store=None):
        self.backend = backend
        self.materialization = materialization
        self.builder = _builder()
        self.store = store if store is not None else MagicMock(is_synced=False)
        self.registry = MagicMock()
        self.tm = MagicMock()
        self.get_graphdb = MagicMock(return_value=self.store)

    def run(self, load_outcome: str = GraphLoad.LOADED, load_triples: int = 7):
        loads = []

        def _fake_run(load):
            loads.append(load)
            load.triple_count = load_triples
            return load_outcome

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    GraphDBFactory, "_resolve_graph_backend", return_value=self.backend
                )
            )
            stack.enter_context(
                patch.object(
                    GraphDBFactory,
                    "resolve_lakehouse_materialization",
                    return_value=self.materialization,
                )
            )
            real_get_builder = GraphDBFactory.get_builder

            def _get_builder(domain, settings=None, *, engine=None, client=None):
                if engine == "delta":
                    return self.builder
                return real_get_builder(domain, settings, engine=engine, client=client)

            stack.enter_context(
                patch.object(GraphDBFactory, "get_builder", side_effect=_get_builder)
            )
            stack.enter_context(
                patch.object(GraphDBFactory, "get_graphdb", self.get_graphdb)
            )
            stack.enter_context(
                patch(
                    "back.core.helpers.get_build_sql_credentials",
                    return_value=("https://host", "tok", "wh-1"),
                )
            )
            stack.enter_context(patch("back.core.databricks.DatabricksClient"))
            stack.enter_context(
                patch(
                    "back.objects.registry.RegistryService.RegistryService.from_context",
                    return_value=self.registry,
                )
            )
            stack.enter_context(
                patch.object(GraphLoad, "run", autospec=True, side_effect=_fake_run)
            )
            stack.enter_context(
                patch.object(
                    SyncedGraphLoad, "run", autospec=True, side_effect=_fake_run
                )
            )
            CertifiedBuild(self.tm, "t-cert", _domain(), SimpleNamespace()).run()
        self.loads = loads
        return self

    @property
    def recorded(self) -> dict:
        folder, entry = self.registry.record_build_run.call_args.args
        assert folder == "dom"
        return entry


class TestLakehouseVirtual:
    def test_the_certified_graph_gets_its_own_search_cache(self):
        h = _Harness(backend="databricks", materialization="view").run()

        h.builder.rebuild_certified_companions.assert_called_once_with()

    def test_the_composition_is_the_served_graph_and_nothing_is_copied(self):
        h = _Harness(backend="databricks", materialization="view").run()

        h.builder.ensure_certified_composition.assert_called_once_with()
        h.get_graphdb.assert_not_called()
        assert h.loads == []
        assert h.tm.complete_task.call_args.kwargs["result"]["triple_count"] == 42

    def test_the_run_is_recorded_as_a_certified_build(self):
        h = _Harness(backend="databricks", materialization="view").run()

        assert h.recorded["build_kind"] == "certified"
        assert h.recorded["status"] == "success"
        assert h.recorded["graph_name"] == _CERTIFIED


@pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
class TestGraphEngines:
    def test_the_backend_is_loaded_from_the_certified_base(self, backend):
        h = _Harness(backend=backend).run()

        h.builder.ensure_certified_composition.assert_called_once_with()
        (load,) = h.loads
        assert load.source_relation == _BASE
        assert load.graph_name == "Dom_V2"
        assert load.store is h.store

    def test_a_loaded_backend_completes_and_records_success(self, backend):
        h = _Harness(backend=backend).run(load_triples=7)

        assert h.tm.complete_task.call_args.kwargs["result"]["triple_count"] == 7
        assert h.recorded["status"] == "success"
        assert h.recorded["triple_count"] == 7
        assert h.recorded["graph_engine"] == backend

    def test_an_empty_original_graph_fails_the_build(self, backend):
        h = _Harness(backend=backend).run(load_outcome=GraphLoad.EMPTY, load_triples=0)

        h.tm.complete_task.assert_not_called()
        assert "Original Graph" in h.tm.fail_task.call_args.args[1]
        assert h.recorded["status"] == "error"

    def test_a_failed_load_records_an_error(self, backend):
        h = _Harness(backend=backend).run(load_outcome=GraphLoad.FAILED)

        h.tm.complete_task.assert_not_called()
        assert h.recorded["status"] == "error"


class TestLakehouseMaterialized:
    def test_the_base_is_copied_and_the_certified_view_is_served(self):
        h = _Harness(backend="databricks", materialization="table").run()

        h.builder.ensure_certified_composition.assert_called_once_with()
        h.builder.materialize_certified_base.assert_called_once_with()
        h.get_graphdb.assert_not_called()
        assert h.loads == []
        assert h.recorded["graph_name"] == _CERTIFIED
        assert h.tm.complete_task.call_args.kwargs["result"]["triple_count"] == 42


class TestLakebaseManagedSynced:
    def _synced(self):
        return _Harness(backend="lakebase", store=MagicMock(is_synced=True))

    def test_lakeflow_syncs_a_source_view_with_object_hash_over_the_base(self):
        h = self._synced().run()

        (load,) = h.loads
        assert type(load).__name__ == "SyncedGraphLoad"
        assert load.source_relation == _SYNC_SOURCE
        view_sql = h.builder.create_view.call_args.args[1]
        assert h.builder.create_view.call_args.args[0] == _SYNC_SOURCE
        assert "object_hash" in view_sql
        assert f"FROM {_BASE}" in view_sql

    def test_a_synced_load_completes_and_records_success(self):
        h = self._synced().run(load_triples=9)

        assert h.recorded["status"] == "success"
        assert h.recorded["triple_count"] == 9


class TestUnsupportedTargets:
    def test_no_graph_backend_fails_before_composing(self):
        h = _Harness(backend="none").run()

        h.builder.ensure_certified_composition.assert_not_called()
        assert h.tm.fail_task.called
        assert h.recorded["status"] == "error"

    def test_an_unreachable_backend_fails_the_build(self):
        h = _Harness(backend="lakebase")
        h.get_graphdb.return_value = None
        h.run()

        assert h.loads == []
        assert h.recorded["status"] == "error"


class TestEnsureEditable:
    """Lifecycle lock (D9): only a DRAFT version gets a certified build."""

    def _check(self, status):
        registry = MagicMock()
        registry.get_version_status.return_value = status
        with patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            CertifiedBuild.ensure_editable(_domain(), SimpleNamespace())
        return registry

    @pytest.mark.parametrize("status", ["PUBLISHED", "IN-REVIEW"])
    def test_a_read_only_version_is_refused(self, status):
        from back.core.errors import ConflictError

        with pytest.raises(ConflictError, match=status):
            self._check(status)

    @pytest.mark.parametrize("status", ["DRAFT", None])
    def test_a_draft_or_new_version_is_accepted(self, status):
        registry = self._check(status)

        registry.get_version_status.assert_called_once_with("dom", "2")


class TestTaskSteps:
    def test_lakehouse_steps_compose_then_serve(self):
        with patch.object(
            GraphDBFactory, "_resolve_graph_backend", return_value="databricks"
        ):
            steps = CertifiedBuild.task_steps(_domain(), SimpleNamespace())

        assert [s["name"] for s in steps] == ["compose", "serve", "cache"]

    def test_graph_engine_steps_compose_then_follow_the_load(self):
        load_steps = [{"name": "graph", "description": "Updating the knowledge graph"}]
        with (
            patch.object(
                GraphDBFactory, "_resolve_graph_backend", return_value="lakebase"
            ),
            patch.object(CertifiedBuild, "load_steps", return_value=load_steps),
        ):
            steps = CertifiedBuild.task_steps(_domain(), SimpleNamespace())

        assert [s["name"] for s in steps] == ["compose", "graph"]


class TestHasSucceeded:
    def _runs(self, runs):
        registry = MagicMock()
        registry.load_build_runs.return_value = runs
        with patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            ok = CertifiedBuild.has_succeeded(_domain(), SimpleNamespace())
        return ok, registry

    def test_a_successful_certified_run_counts(self):
        ok, registry = self._runs(
            [
                {"build_kind": "session", "status": "success"},
                {"build_kind": "certified", "status": "success"},
            ]
        )

        assert ok is True
        registry.load_build_runs.assert_called_once_with("dom", version="2")

    def test_failed_or_non_certified_runs_do_not_count(self):
        ok, _ = self._runs(
            [
                {"build_kind": "certified", "status": "error"},
                {"build_kind": "session", "status": "success"},
            ]
        )

        assert ok is False

    def test_an_unreadable_registry_reads_as_not_certified(self):
        registry = MagicMock()
        registry.load_build_runs.side_effect = RuntimeError("down")
        with patch(
            "back.objects.registry.RegistryService.RegistryService.from_context",
            return_value=registry,
        ):
            assert CertifiedBuild.has_succeeded(_domain(), SimpleNamespace()) is False


class TestStatus:
    def _status(self, backend, *, materialization="view", status="DRAFT", runs=()):
        registry = MagicMock()
        registry.get_version_status.return_value = status
        registry.load_build_runs.return_value = list(runs)
        with (
            patch.object(GraphDBFactory, "_resolve_graph_backend", return_value=backend),
            patch.object(
                GraphDBFactory,
                "resolve_lakehouse_materialization",
                return_value=materialization,
            ),
            patch(
                "back.objects.registry.RegistryService.RegistryService.from_context",
                return_value=registry,
            ),
        ):
            return CertifiedBuild.status(
                SimpleNamespace(
                    info={"name": "Dom"},
                    current_version="2",
                    uc_domain_folder="dom",
                    delta={"catalog": "cat", "schema": "sch"},
                ),
                SimpleNamespace(),
            )

    def test_a_lakehouse_target_serves_the_certified_view(self):
        out = self._status("databricks")

        assert out["target"] == "lakehouse_virtual"
        assert out["served_relation"] == "cat.sch.triplestore_dom_V2_certified"

    def test_a_graph_engine_target_serves_the_versioned_graph(self):
        out = self._status("lakebase")

        assert out["target"] == "lakebase"
        assert out["served_relation"] == "Dom_V2"

    def test_a_successful_certified_run_certifies_and_no_runs_are_returned(self):
        out = self._status(
            "neo4j",
            runs=[
                {"build_kind": "certified", "status": "error"},
                {"build_kind": "session", "status": "success"},
                {"build_kind": "certified", "status": "success"},
            ],
        )

        assert out["certified"] is True
        assert "runs" not in out

    @pytest.mark.parametrize("status,editable", [("DRAFT", True), ("PUBLISHED", False)])
    def test_the_version_lock_is_reported(self, status, editable):
        out = self._status("lakebase", status=status)

        assert out["version_status"] == status
        assert out["editable"] is editable

    def test_no_backend_has_no_certified_graph(self):
        out = self._status("none")

        assert out["target"] == "none"
        assert out["served_relation"] == ""
