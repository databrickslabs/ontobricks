"""A failed graph probe must not be cached as a confirmed "Graph not built".

The KG sub-pages (Analytics, Explorer, Chat, …) render their readiness badge
from ``/dtwin/sync/info`` → ``triplestore_status.has_data``, which ``sync_info``
serves cache-first through :meth:`DigitalTwin.get_or_fetch_graph_status`.

Two defects combined to show "Graph not built" on Analytics while the Build page
— which force-refreshes its own live probe — correctly reported the graph as
present:

1. ``fetch_graph_triplestore_status`` swallowed every exception from
   ``table_exists``/``get_status`` and returned a confident ``has_data=False``
   with ``reason="Graph does not exist yet"``. A timeout was therefore
   indistinguishable from a graph that had never been built.
2. ``get_or_fetch_graph_status`` cached that answer unconditionally for the
   full ``_TS_STATS_CACHE_TTL_SECONDS``, with no way to force a re-check.

On a remote engine (Neo4j Aura, where a single ``SHOW CONSTRAINTS`` was observed
taking ~1s) one slow call poisoned the badge on every KG page for five minutes.

``get_or_fetch_dt_existence`` already guards against exactly this and says so in
its docstring — "caching a transient failure as if it were a confirmed absence
is the bug this whole pathway exists to prevent". These tests hold the ``status``
section to the same contract.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.objects.digitaltwin.DigitalTwin import DigitalTwin

pytestmark = pytest.mark.unit


def _domain() -> SimpleNamespace:
    return SimpleNamespace(triplestore={}, save=MagicMock())


async def _run_blocking(fn, *args, **kwargs):
    """Stand-in for the thread-pool offload: call through, keep the await."""
    return fn(*args, **kwargs)


def _store(*, exists=True, count=7) -> MagicMock:
    """A graph store whose probe answers, or raises when given an exception."""
    store = MagicMock()
    if isinstance(exists, Exception):
        store.table_exists.side_effect = exists
    else:
        store.table_exists.return_value = exists
    if isinstance(count, Exception):
        store.get_status.side_effect = count
    else:
        store.get_status.return_value = {"count": count, "path": None}
    return store


@contextmanager
def _probing(store):
    """Patch the helpers ``fetch_graph_triplestore_status`` imports at call time."""
    with patch(
        "back.core.helpers.effective_graph_query_table",
        return_value="ContactCenter_V2",
    ), patch(
        "back.core.helpers.effective_view_table", return_value="main.ob.cc_view"
    ), patch(
        "back.core.helpers.run_blocking", _run_blocking
    ), patch(
        "back.core.graphdb.get_graphdb", return_value=store
    ):
        yield


class TestAFailedProbeIsNotAConfirmedAbsence:
    async def test_a_raising_table_exists_yields_an_unknown_not_a_false(self):
        """``has_data`` must be None: we did not learn that the graph is absent."""
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=TimeoutError("Aura cold start"))):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert result["has_data"] is None

    async def test_a_raising_count_yields_an_unknown_too(self):
        """The graph demonstrably exists here, so False would be doubly wrong."""
        dt = DigitalTwin(_domain())
        with _probing(_store(count=TimeoutError("read timed out"))):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert result["has_data"] is None

    async def test_the_failure_is_reported_so_the_ui_can_say_so(self):
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=TimeoutError("Aura cold start"))):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert "Aura cold start" in str(result.get("graph_check_error") or "")

    async def test_it_does_not_claim_the_graph_was_never_built(self):
        """The old copy sent users to Build to re-create a graph they had."""
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=TimeoutError("Aura cold start"))):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert "does not exist" not in (result.get("reason") or "")


class TestConfidentAnswersAreUnchanged:
    async def test_a_populated_graph_still_reads_as_built(self):
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=True, count=42)):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert result["has_data"] is True
        assert result["count"] == 42

    async def test_a_genuinely_absent_graph_still_reads_as_not_built(self):
        """A clean False — the probe answered — must stay a definite False."""
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=False)):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert result["has_data"] is False
        assert result.get("graph_check_error") is None
        assert "does not exist" in result["reason"]

    async def test_an_existing_but_empty_graph_is_distinguished(self):
        dt = DigitalTwin(_domain())
        with _probing(_store(exists=True, count=0)):
            result = await dt.fetch_graph_triplestore_status(MagicMock())

        assert result["has_data"] is False
        assert result["reason"] == "Graph is empty"


class TestTheCacheNeverStoresAnInconclusiveProbe:
    async def test_a_failed_probe_is_not_written_to_the_cache(self):
        """Otherwise the badge lies for the whole TTL on every KG page."""
        domain = _domain()
        dt = DigitalTwin(domain)
        with _probing(_store(exists=TimeoutError("Aura cold start"))):
            await dt.get_or_fetch_graph_status(MagicMock())

        assert dt.get_ts_cache("status") is None

    async def test_the_next_call_therefore_probes_again(self):
        """A recovered engine must be picked up immediately, not in 5 minutes."""
        domain = _domain()
        dt = DigitalTwin(domain)
        with _probing(_store(exists=TimeoutError("Aura cold start"))):
            await dt.get_or_fetch_graph_status(MagicMock())
        with _probing(_store(exists=True, count=9)):
            second = await dt.get_or_fetch_graph_status(MagicMock())

        assert second["has_data"] is True

    async def test_a_confident_answer_is_still_cached(self):
        """The cache exists for a reason: the probe is a remote round-trip."""
        domain = _domain()
        dt = DigitalTwin(domain)
        with _probing(_store(exists=True, count=9)):
            await dt.get_or_fetch_graph_status(MagicMock())

        assert (dt.get_ts_cache("status") or {}).get("has_data") is True

    async def test_a_confident_false_is_cached_too(self):
        domain = _domain()
        dt = DigitalTwin(domain)
        with _probing(_store(exists=False)):
            await dt.get_or_fetch_graph_status(MagicMock())

        assert (dt.get_ts_cache("status") or {}).get("has_data") is False


class TestForceRefreshCanBypassAPoisonedEntry:
    async def test_force_refresh_ignores_the_cached_answer(self):
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("status", {"success": True, "has_data": False, "count": 0})

        with _probing(_store(exists=True, count=5)):
            result = await dt.get_or_fetch_graph_status(MagicMock(), force_refresh=True)

        assert result["has_data"] is True

    async def test_without_force_refresh_the_cache_still_wins(self):
        """Guards the premise — this is why the stale badge survived."""
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("status", {"success": True, "has_data": False, "count": 0})

        with _probing(_store(exists=True, count=5)):
            result = await dt.get_or_fetch_graph_status(MagicMock())

        assert result["has_data"] is False

    async def test_a_forced_refresh_that_fails_does_not_overwrite_with_a_false(self):
        """A forced re-check that times out must not poison the cache either."""
        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("status", {"success": True, "has_data": True, "count": 5})

        with _probing(_store(exists=TimeoutError("boom"))):
            await dt.get_or_fetch_graph_status(MagicMock(), force_refresh=True)

        assert (dt.get_ts_cache("status") or {}).get("has_data") is not False


class TestTheTtlIsWhyThisMattered:
    def test_a_poisoned_status_entry_would_survive_for_minutes(self):
        import importlib

        module = importlib.import_module("back.objects.digitaltwin.DigitalTwin")
        assert module._TS_STATS_CACHE_TTL_SECONDS >= 60

        domain = _domain()
        dt = DigitalTwin(domain)
        dt.set_ts_cache("status", {"success": True, "has_data": False})
        domain.triplestore["stats"]["status"]["_ts"] = time.time() - 30
        assert dt.get_ts_cache("status") is not None
