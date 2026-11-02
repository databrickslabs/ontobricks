"""The per-version Lakeflow slot pipeline of an incremental / microbatch domain.

Extracted from :class:`TwinIncrementalBuild` (Fowler Extract Class), in the
same style: static methods that take the running pipeline (*pipe*) or explicit
arguments. Steps b and c of an incremental build: create/update the pipeline,
run an update and wait for it; plus stopping a pipeline the build no longer
uses, and the settings (channel, timeout) every caller shares.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

from back.core.errors import OntoBricksError, OperationCancelledError
from back.core.graphdb.GraphDBFactory import CONTINUOUS_MATERIALIZATION_MODES
from back.core.logging import get_logger
from back.objects.digitaltwin.build.TwinBuildRunHistory import TwinBuildRunHistory

logger = get_logger(__name__)

_TIMEOUT_ENV = "DTWIN_SLOT_PIPELINE_TIMEOUT_S"
_DEFAULT_TIMEOUT_S = 3600
_DEFAULT_CHANNEL = "PREVIEW"

#: Pipeline states in which an update may be running (it must be stopped first).
_ACTIVE_PIPELINE_STATES = frozenset(
    {"DEPLOYING", "RECOVERING", "RESETTING", "RUNNING", "STARTING", "STOPPING"}
)


class TwinSlotPipeline:
    """Create, update, wait for and stop a domain version's slot pipeline."""

    @staticmethod
    def channel(settings: Any) -> str:
        """Lakeflow channel of the slot pipeline (``PREVIEW`` unless configured)."""
        return str(getattr(settings, "slot_pipeline_channel", "") or _DEFAULT_CHANNEL)

    @staticmethod
    def timeout_s() -> int:
        """``DTWIN_SLOT_PIPELINE_TIMEOUT_S``; 3600 when unset, invalid or not positive."""
        try:
            value = int((os.environ.get(_TIMEOUT_ENV) or "").strip())
        except ValueError:
            return _DEFAULT_TIMEOUT_S
        return value if value > 0 else _DEFAULT_TIMEOUT_S

    @staticmethod
    def run(pipe: Any, spec: Any, *, force_full_refresh: bool = False) -> None:
        """Ensure the pipeline for *spec*, then update it and wait.

        A full refresh runs when the pipeline is new, its stored SQL
        fingerprint differs (flows were added or removed, so stale slots must
        go) or *force_full_refresh* asks for one (a rewritten source). It always runs as a *triggered* update to ``COMPLETED`` — a
        continuous one would let ``…_data`` be built over a still-empty
        ``…_slots`` — and the pipeline keeps its previous fingerprint until
        then, so a failed refresh is retried by the next build. Afterwards the
        new fingerprint is stamped and a ``microbatch`` pipeline is switched
        to continuous (waited for until ``RUNNING``).

        Without a full refresh a ``microbatch`` pipeline already running the
        current SQL is left running; any other active pipeline is stopped
        before the update. ``pipe.slot_update_id`` / ``pipe.slots_changed``: the first
        update and the rows it wrote (:meth:`SlotPipelineRunner.update_metrics`).
        """
        from back.core.streaming import SlotPipelineRunner

        runner = SlotPipelineRunner()
        name = TwinSlotPipeline._name(pipe)
        catalog, schema, _ = spec.slots_fqn.split(".", 2)
        definition = {
            "name": name,
            "catalog": catalog,
            "schema": schema,
            "sql_text": spec.to_sql(),
            "serverless": True,
            "channel": TwinSlotPipeline.channel(pipe.settings),
        }
        current = SlotPipelineRunner.fingerprint(definition["sql_text"])
        continuous = pipe.mode_decision.effective in CONTINUOUS_MATERIALIZATION_MODES

        existing = runner.find_pipeline_id(name)
        stored = runner.stored_fingerprint(existing) if existing else ""
        full_refresh = force_full_refresh or stored != current
        if existing and continuous and not full_refresh and runner.get_state(existing) == "RUNNING":
            pipe.slot_pipeline_id = existing
            logger.info(
                "[DT-BUILD %s] slot pipeline %s already streams the current SQL",
                pipe.task_id,
                name,
            )
            return

        pipe.tm.update_progress(pipe.task_id, 27, f"Preparing slot pipeline {name}...")
        pipeline_id = runner.ensure(
            **definition,
            continuous=continuous and not full_refresh,
            fingerprint=stored if full_refresh else current,
        )
        pipe.slot_pipeline_id = pipeline_id
        if runner.get_state(pipeline_id) in _ACTIVE_PIPELINE_STATES:
            runner.stop(pipeline_id)
        update_id = TwinSlotPipeline._update(
            pipe,
            runner,
            pipeline_id,
            name,
            full_refresh=full_refresh,
            until_running=continuous and not full_refresh,
        )
        pipe.slot_update_id = update_id
        pipe.slots_changed = runner.update_metrics(pipeline_id, update_id)
        if not full_refresh:
            return
        runner.ensure(**definition, continuous=continuous, fingerprint=current)
        if continuous:
            TwinSlotPipeline._update(
                pipe, runner, pipeline_id, name, full_refresh=False, until_running=True
            )

    @staticmethod
    def stop_if_active(pipe: Any) -> None:
        """Best-effort stop of this version's slot pipeline when it is active.

        Failures are logged and swallowed: a pipeline left running costs
        compute, not correctness, and must not fail the full build.
        """
        # Follow-up (docs/incremental-materialization.md, Known limitations):
        # pipelines of earlier versions keep running after a version bump;
        # only the current version's pipeline is stopped here.
        from back.core.streaming import SlotPipelineRunner

        name = TwinSlotPipeline._name(pipe)
        try:
            runner = SlotPipelineRunner()
            pipeline_id = runner.find_pipeline_id(name)
            if pipeline_id and runner.get_state(pipeline_id) in _ACTIVE_PIPELINE_STATES:
                runner.stop(pipeline_id)
                logger.info("[DT-BUILD %s] stopped slot pipeline %s", pipe.task_id, name)
        except OperationCancelledError:
            raise
        except OntoBricksError as exc:
            logger.warning(
                "[DT-BUILD %s] could not stop slot pipeline %s (non-fatal): %s",
                pipe.task_id,
                name,
                exc,
            )

    @staticmethod
    def run_update(
        runner: Any,
        pipeline_id: str,
        *,
        full_refresh: bool,
        until_running: bool,
        cancel_check: Callable[[], bool] | None = None,
        on_state: Callable[[str], None] | None = None,
    ) -> str:
        """Start one update of *pipeline_id*, wait for it (``COMPLETED``, or ``RUNNING``); its id."""
        update_id = runner.start_update(pipeline_id, full_refresh=full_refresh)
        runner.wait_for_update(
            pipeline_id,
            update_id,
            timeout_s=TwinSlotPipeline.timeout_s(),
            cancel_check=cancel_check,
            on_state=on_state,
            until_running=until_running,
        )
        return update_id

    @staticmethod
    def _update(
        pipe: Any,
        runner: Any,
        pipeline_id: str,
        name: str,
        *,
        full_refresh: bool,
        until_running: bool,
    ) -> str:
        """:meth:`run_update` with the build's cancel check and progress reports; the update id."""
        return TwinSlotPipeline.run_update(
            runner,
            pipeline_id,
            full_refresh=full_refresh,
            until_running=until_running,
            cancel_check=pipe._is_cancelled,
            on_state=lambda state: pipe.tm.update_progress(
                pipe.task_id, 28, f"Slot pipeline {name}: {state.lower()}"
            ),
        )

    @staticmethod
    def _name(pipe: Any) -> str:
        """The pipeline name of the running build's domain version."""
        from back.core.streaming import SlotPipelineRunner

        return SlotPipelineRunner.pipeline_name(
            pipe.domain_name, TwinBuildRunHistory.build_run_key(pipe)[1]
        )
