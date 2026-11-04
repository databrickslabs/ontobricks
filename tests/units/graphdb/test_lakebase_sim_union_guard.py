"""Guard: the Explorer sim-union view must not crash on a missing sandbox.

Regression: a simulation marked ``ready`` in the registry can lose its physical
Lakebase sandbox table (the graph was rebuilt / re-synced after the run).
``SimulationUnion.ensure`` used to run ``CREATE OR REPLACE VIEW ... FROM
<sandbox>`` blindly, so the missing table surfaced as an opaque ``UndefinedTable``
-> 502 that the Explorer mislabelled as "gateway time limit". It must now probe
first and raise a clear :class:`ValidationError` (400).
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

pytest.importorskip("psycopg")

from back.core.errors import ValidationError
from back.core.graphdb.lakebase.SimulationUnion import SimulationUnion

pytestmark = pytest.mark.unit

_VIEW = "cust360auto_v5__smu_s1"
_LIVE = "cust360auto_v5"
_SANDBOX = "cust360auto_v5__sim_s1"


class _FakeCursor:
    def __init__(self, *, sandbox_present: bool):
        self._sandbox_present = sandbox_present
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchone(self):
        # The probe (`SELECT 1 WHERE to_regclass(...) IS NOT NULL`) yields a row
        # only when the relation exists.
        return (1,) if self._sandbox_present else None


class _FakeStore:
    def __init__(self, cur: _FakeCursor):
        self._cur = cur
        self._schema = "graphs"

    def _cursor(self):
        @contextmanager
        def _cm():
            yield self._cur

        return _cm()


def _created_view(cur: _FakeCursor) -> bool:
    return any("CREATE OR REPLACE VIEW" in s for s in cur.executed)


def test_ensure_raises_clear_error_when_sandbox_missing():
    cur = _FakeCursor(sandbox_present=False)
    union = SimulationUnion(_FakeStore(cur))

    with pytest.raises(ValidationError, match="[Rr]e-run the simulation"):
        union.ensure(_VIEW, _LIVE, _SANDBOX)

    # The view DDL must never run when the sandbox is gone.
    assert not _created_view(cur)


def test_ensure_builds_view_when_sandbox_present():
    cur = _FakeCursor(sandbox_present=True)
    union = SimulationUnion(_FakeStore(cur))

    view_id = union.ensure(_VIEW, _LIVE, _SANDBOX)

    assert _created_view(cur)
    assert view_id
