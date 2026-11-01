"""Wait for a CONTINUOUS Lakebase synced table to start streaming.

Collaborator of :meth:`SyncedTableManager.trigger_and_wait`. A continuous
pipeline is never "idle" and nothing is triggered on it, but a build must not
read the Postgres table while the initial snapshot is still provisioning. This
polls ``mgr.get(name)`` until the state leaves ``PROVISIONING*`` — it reaches
``ONLINE_CONTINUOUS_UPDATE`` or a terminal-OK state — and raises on a terminal
failure or when *timeout_s* runs out.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Optional

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

logger = get_logger(__name__)

#: The steady state of a continuous synced table that serves reads.
STREAMING_STATE = "ONLINE_CONTINUOUS_UPDATE"


class ContinuousSyncWait:
    """Poll a continuous synced table until it streams (no trigger, no idle wait)."""

    @staticmethod
    def until_streaming(
        mgr: Any,
        name: str,
        timeout_s: int,
        cancel_check: Optional[Callable[[], bool]] = None,
        on_state_change: Optional[Callable[[str], None]] = None,
    ) -> str:
        """Return the first streaming (or terminal-OK) state of *name*.

        *cancel_check* is consulted before every poll (``OperationCancelledError``);
        *on_state_change* receives each new state. Raises
        :class:`InfrastructureError` on a terminal failure state or after
        *timeout_s* seconds.
        """
        # The manager module imports this one: its private helpers load lazily.
        from back.core.graphdb.lakebase.SyncedTableManager import (
            _POLL_INTERVAL_S,
            _TERMINAL_FAIL,
            _TERMINAL_OK,
            _raise_if_cancelled,
        )

        deadline = time.time() + max(1, int(timeout_s))
        last_state = ""
        while True:
            _raise_if_cancelled(cancel_check, f"waiting for continuous synced table {name}")
            synced = mgr.get(name)
            state = mgr._extract_state(synced) if synced is not None else "MISSING"
            if state != last_state:
                logger.info("Continuous synced table %s state=%s", name, state)
                last_state = state
                ContinuousSyncWait._notify(on_state_change, state)
            if state in _TERMINAL_FAIL:
                raise InfrastructureError(
                    f"Continuous synced table {name} reached terminal failure state: {state}"
                )
            if state == STREAMING_STATE or state in _TERMINAL_OK:
                return state
            remaining = deadline - time.time()
            if remaining <= 0:
                raise InfrastructureError(
                    f"Timed out after {timeout_s}s waiting for continuous synced table "
                    f"{name} to start streaming (last state={state or '?'})"
                )
            mgr._sleep(min(_POLL_INTERVAL_S, remaining))

    @staticmethod
    def _notify(on_state_change: Optional[Callable[[str], None]], state: str) -> None:
        """Report *state*; a failing progress callback never fails the wait."""
        if on_state_change is None:
            return
        try:
            on_state_change(state)
        except Exception:  # noqa: BLE001 — progress reporting only, as in wait_for_completion
            logger.debug("on_state_change callback failed for state %s", state, exc_info=True)
