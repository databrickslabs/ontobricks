"""Pure-function tests for `_BuildPipeline` (private build orchestrator).

`_BuildPipeline` (`src/back/objects/digitaltwin/_build_pipeline.py`) is the
Fowler "Method Object" extracted from the legacy 839-line
`DigitalTwin.run_build_task`. Most of it is heavy I/O — Databricks SQL,
the triple store, the task manager — which lives in the integration tier.

This file covers the **pure** surface:

- `__init__` derived state (`is_api`, `domain_name`, `parts`,
  `phase_times` initialization, lazy-state defaults).
- `_log_phase` — records elapsed time on `self.phase_times` and logs.
- `_persist_last_build_to_registry` — registry write after a successful
  build so the Submit-for-Review gate is unblocked.

Behaviour-rich phases (the various ``_apply_*`` and ``_*_progress``
methods) are exercised end-to-end in higher tiers.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin._build_pipeline import _BuildPipeline
from back.objects.registry.RegistryService import RegistryService


def _make_pipeline(**overrides: Any) -> _BuildPipeline:
    """Build a pipeline instance with sensible defaults; override per test."""
    defaults: dict[str, Any] = dict(
        tm=MagicMock(),
        task_id="task-001",
        domain=SimpleNamespace(info={"name": "sales"}),
        settings={},
        domain_snap=MagicMock(),
        host="host",
        token="token",
        warehouse_id="wh-1",
        view_table="cat.schema.view",
        graph_name="g1",
        r2rml_content="",
        base_uri="http://ex/",
        mapping_config={},
        ontology_config={},
        delta_cfg={},
        build_kind="session",
    )
    defaults.update(overrides)
    return _BuildPipeline(**defaults)


# --- __init__ derived state ----------------------------------------------


@pytest.mark.unit
class TestInit:
    def test_session_build_is_not_api(self) -> None:
        pipe = _make_pipeline(build_kind="session")
        assert pipe.is_api is False

    def test_api_build_sets_is_api_flag(self) -> None:
        pipe = _make_pipeline(build_kind="api")
        assert pipe.is_api is True

    def test_view_table_split_into_parts(self) -> None:
        pipe = _make_pipeline(view_table="cat_x.schema_y.view_z")
        assert pipe.parts == ["cat_x", "schema_y", "view_z"]

    def test_view_table_with_two_parts(self) -> None:
        """``view_table`` is fed straight into ``str.split`` — two-segment
        forms (no catalog prefix) become a 2-element list."""
        pipe = _make_pipeline(view_table="schema.view")
        assert pipe.parts == ["schema", "view"]

    def test_domain_name_uses_info_field(self) -> None:
        pipe = _make_pipeline(domain=SimpleNamespace(info={"name": "hr"}))
        assert pipe.domain_name == "hr"

    def test_domain_name_falls_back_when_info_missing(self) -> None:
        # The constructor uses ``(domain.info or {}).get("name", "<unknown>")`` —
        # if ``info`` is falsy or has no name, we get the sentinel.
        pipe = _make_pipeline(domain=SimpleNamespace(info=None))
        assert pipe.domain_name == "<unknown>"

    def test_domain_name_falls_back_when_name_missing(self) -> None:
        pipe = _make_pipeline(domain=SimpleNamespace(info={}))
        assert pipe.domain_name == "<unknown>"

    def test_phase_times_starts_empty(self) -> None:
        pipe = _make_pipeline()
        assert pipe.phase_times == {}

    def test_build_recorded_flag_starts_false(self) -> None:
        """``_build_recorded`` guards the build-run trace so a build is
        recorded exactly once across all terminal paths."""
        pipe = _make_pipeline()
        assert pipe._build_recorded is False

    def test_lazy_state_initialised_to_none_or_empty(self) -> None:
        pipe = _make_pipeline()
        assert pipe.source_client is None
        assert pipe.store is None
        assert pipe.entity_mappings == []
        assert pipe.relationship_mappings == []
        assert pipe.spark_sql == ""
        assert pipe.triple_count == 0
        assert pipe._lakebase_engine_config == {}
        assert pipe._graph_engine == ""
        assert pipe._is_lakebase_synced is False

    def test_start_time_is_set_to_now(self) -> None:
        # The exact value isn't critical -- just that it's a recent epoch.
        before = time.time()
        pipe = _make_pipeline()
        after = time.time()
        assert before <= pipe.start_time <= after

    def test_build_kind_is_stored(self) -> None:
        pipe = _make_pipeline(build_kind="api")
        assert pipe.build_kind == "api"

    def test_simple_attributes_are_stored_verbatim(self) -> None:
        """The constructor stashes every input on ``self`` — verify the
        plain ones don't get accidentally transformed."""
        pipe = _make_pipeline(
            task_id="task-XYZ",
            host="https://example.databricks.com",
            token="dapi-token-123",
            warehouse_id="wh-abc",
            graph_name="custom_graph",
            r2rml_content="<<r2rml>>",
            base_uri="http://my-base/",
        )
        assert pipe.task_id == "task-XYZ"
        assert pipe.host == "https://example.databricks.com"
        assert pipe.token == "dapi-token-123"
        assert pipe.warehouse_id == "wh-abc"
        assert pipe.graph_name == "custom_graph"
        assert pipe.r2rml_content == "<<r2rml>>"
        assert pipe.base_uri == "http://my-base/"


# --- _log_phase ----------------------------------------------------------


@pytest.mark.unit
class TestLogPhase:
    def test_records_elapsed_time_in_phase_times(self) -> None:
        pipe = _make_pipeline()
        t0 = time.time() - 1.5  # Pretend the phase took 1.5 seconds.
        pipe._log_phase("prepare", t0)
        assert "prepare" in pipe.phase_times
        # Allow a generous tolerance for wall-clock noise.
        assert 1.4 < pipe.phase_times["prepare"] < 2.5

    def test_multiple_phases_accumulate(self) -> None:
        pipe = _make_pipeline()
        now = time.time()
        pipe._log_phase("prepare", now - 0.5)
        pipe._log_phase("apply", now - 0.2)
        pipe._log_phase("snapshot", now - 0.1)
        assert set(pipe.phase_times.keys()) == {"prepare", "apply", "snapshot"}
        # Each should be positive.
        for name, val in pipe.phase_times.items():
            assert val >= 0, f"phase {name} had negative elapsed: {val}"

    def test_same_phase_overwrites(self) -> None:
        # If a phase is logged twice (e.g., retry), the second value wins.
        pipe = _make_pipeline()
        now = time.time()
        pipe._log_phase("apply", now - 2.0)
        first = pipe.phase_times["apply"]
        pipe._log_phase("apply", now - 0.1)
        second = pipe.phase_times["apply"]
        assert second < first  # The retry was faster than the first attempt.


# --- _persist_last_build_to_registry ------------------------------------


def _make_domain(
    *,
    last_build: str = "2026-06-19T09:00:00+00:00",
    current_version: str = "1",
    uc_domain_folder: str = "supplychain",
    name: str = "supplychain",
) -> SimpleNamespace:
    """Create a minimal domain stand-in for persist tests."""
    return SimpleNamespace(
        info={"name": name},
        last_build=last_build,
        current_version=current_version,
        uc_domain_folder=uc_domain_folder,
        export_for_save=lambda: {"info": {"last_build": last_build}},
    )


def _make_registry_svc(write_ok: bool = True, write_msg: str = "") -> MagicMock:
    """Return a mock RegistryService whose _store.write_version returns (write_ok, write_msg)."""
    svc = MagicMock()
    svc._store.write_version.return_value = (write_ok, write_msg)
    return svc


@pytest.mark.unit
class TestPersistLastBuildToRegistry:
    """_persist_last_build_to_registry writes domain.last_build to the registry DB."""

    def _make_pipe(self, domain=None, **overrides):
        dom = domain or _make_domain()
        snap = MagicMock()
        snap.current_version = dom.current_version
        return _make_pipeline(domain=dom, domain_snap=snap, **overrides)

    def test_calls_write_version_on_success(self) -> None:
        """Happy path: write_version is called once with folder + version."""
        pipe = self._make_pipe()
        svc = _make_registry_svc()

        with patch.object(RegistryService, "from_context", return_value=svc):
            pipe._persist_last_build_to_registry()

        svc._store.write_version.assert_called_once()
        call_args = svc._store.write_version.call_args
        folder, version, _ = call_args.args
        assert folder == "supplychain"
        assert version == "1"

    def test_domain_data_includes_last_build(self) -> None:
        """The domain_data passed to write_version carries last_build."""
        pipe = self._make_pipe()
        svc = _make_registry_svc()

        with patch.object(RegistryService, "from_context", return_value=svc):
            pipe._persist_last_build_to_registry()

        _, _, domain_data = svc._store.write_version.call_args.args
        assert domain_data["info"]["last_build"] == "2026-06-19T09:00:00+00:00"

    def test_api_build_stamps_last_build_when_empty(self) -> None:
        """API build path: domain.last_build is empty before the build; the method
        stamps it so the registry write carries a non-empty timestamp."""
        dom = _make_domain(last_build="")
        snap = MagicMock()
        snap.current_version = dom.current_version
        # export_for_save must reflect the updated last_build after stamping.
        def _export():
            return {"info": {"last_build": dom.last_build}}
        dom.export_for_save = _export

        pipe = _make_pipeline(domain=dom, domain_snap=snap, build_kind="api")
        svc = _make_registry_svc()

        with patch.object(RegistryService, "from_context", return_value=svc):
            pipe._persist_last_build_to_registry()

        assert dom.last_build  # was stamped
        _, _, domain_data = svc._store.write_version.call_args.args
        assert domain_data["info"]["last_build"]

    def test_skips_when_folder_cannot_be_resolved(self) -> None:
        """No folder available → method returns early without calling write_version."""
        snap = MagicMock()
        snap.current_version = ""
        pipe = _make_pipeline(domain=SimpleNamespace(
            info={},  # name not set → sanitize_domain_folder returns ""
            last_build="ts",
            current_version="",
            uc_domain_folder="",
            export_for_save=lambda: {},
        ), domain_snap=snap)
        svc = _make_registry_svc()

        with (
            patch.object(RegistryService, "from_context", return_value=svc),
            patch("back.objects.session.sanitize_domain_folder", return_value=""),
        ):
            pipe._persist_last_build_to_registry()

        svc._store.write_version.assert_not_called()

    def test_handles_write_version_failure_gracefully(self) -> None:
        """write_version returning (False, msg) is logged but does not raise."""
        pipe = self._make_pipe()
        svc = _make_registry_svc(write_ok=False, write_msg="DB error")

        with patch.object(RegistryService, "from_context", return_value=svc):
            pipe._persist_last_build_to_registry()  # must not raise

        svc._store.write_version.assert_called_once()

    def test_handles_registry_service_exception_gracefully(self) -> None:
        """An exception from RegistryService.from_context must not propagate."""
        pipe = self._make_pipe()

        with patch.object(
            RegistryService, "from_context", side_effect=RuntimeError("connection refused")
        ):
            pipe._persist_last_build_to_registry()  # must not raise
