"""Per-domain ``materialization_mode``: normalizer, requested-mode lookup and fallback ladder."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.core.graphdb import MaterializationModeResolver, ModeDecision
from back.core.graphdb.GraphDBFactory import (
    DEFAULT_MATERIALIZATION_MODE,
    MATERIALIZATION_MODES,
    GraphDBFactory,
    normalize_materialization_mode,
)
from back.objects.digitaltwin.build import TwinBuildPostMaterialize

pytestmark = pytest.mark.unit

_OPEN = dict(
    serverless_available=True,
    sources_row_tracked=True,
    backend="databricks",
    previews={},
)


def _resolve(requested, **overrides):
    return MaterializationModeResolver.resolve(requested, **{**_OPEN, **overrides})


class TestConstantsAndNormalizer:
    def test_allowed_modes_in_ladder_order(self):
        assert MATERIALIZATION_MODES == ("full", "incremental", "microbatch", "realtime")

    def test_default_is_full(self):
        assert DEFAULT_MATERIALIZATION_MODE == "full"

    @pytest.mark.parametrize(
        "value, expected",
        [
            (None, "full"),
            ("", "full"),
            ("   ", "full"),
            ("bogus", "full"),
            (5, "full"),
            (["full"], "full"),
            ("full", "full"),
            ("FULL", "full"),
            (" Incremental ", "incremental"),
            ("microbatch", "microbatch"),
            ("MICROBATCH", "microbatch"),
            ("realtime", "realtime"),
            (" Realtime", "realtime"),
        ],
    )
    def test_normalize(self, value, expected):
        assert normalize_materialization_mode(value) == expected


class TestRequestedModeLookup:
    def test_reads_the_stored_mode(self):
        d = MagicMock()
        d.info = {"materialization_mode": "incremental"}
        assert GraphDBFactory.resolve_materialization_mode(d) == "incremental"

    def test_normalizes_what_is_stored(self):
        d = MagicMock()
        d.info = {"materialization_mode": " MICROBATCH "}
        assert GraphDBFactory.resolve_materialization_mode(d, None) == "microbatch"

    def test_missing_key_defaults_to_full(self):
        d = MagicMock()
        d.info = {"graph_backend": "databricks"}
        assert GraphDBFactory.resolve_materialization_mode(d) == "full"

    def test_invalid_value_defaults_to_full(self):
        d = MagicMock()
        d.info = {"materialization_mode": "nope"}
        assert GraphDBFactory.resolve_materialization_mode(d) == "full"

    def test_non_dict_info_defaults_to_full(self):
        d = MagicMock()
        d.info = None
        assert GraphDBFactory.resolve_materialization_mode(d) == "full"

    def test_domain_without_info_defaults_to_full(self):
        assert GraphDBFactory.resolve_materialization_mode(object()) == "full"

    def test_backend_does_not_gate_the_requested_mode(self):
        """The resolver decides what runs; the lookup only reports the request."""
        d = MagicMock()
        d.info = {"graph_backend": "lakebase", "materialization_mode": "incremental"}
        assert GraphDBFactory.resolve_materialization_mode(d) == "incremental"


class TestModeDecision:
    def test_is_a_frozen_value(self):
        decision = ModeDecision("full", "full", "")
        with pytest.raises(AttributeError):
            decision.effective = "incremental"

    def test_value_equality(self):
        assert ModeDecision("a", "b", "c") == ModeDecision("a", "b", "c")


class TestLadderStaysPut:
    @pytest.mark.parametrize("mode", ["full", "incremental", "microbatch"])
    def test_an_available_mode_is_kept_with_no_reason(self, mode):
        assert _resolve(mode) == ModeDecision(mode, mode, "")

    @pytest.mark.parametrize("backend", ["databricks", "lakebase", "neo4j"])
    def test_every_supported_backend_is_eligible(self, backend):
        assert _resolve("incremental", backend=backend).effective == "incremental"

    def test_full_never_degrades_even_when_nothing_is_available(self):
        decision = _resolve(
            "full",
            serverless_available=False,
            sources_row_tracked=False,
            backend="none",
        )
        assert decision == ModeDecision("full", "full", "")


_COMPLETE_SOURCE = {
    "kind": "kafka",
    "bootstrap_servers": "broker-1:9092,broker-2:9092",
    "topic": "sales.customers",
    "secret_scope": "ontobricks",
    "secret_key": "kafka-sasl",
    "key_format": "json",
}


class TestRealtime:
    def test_realtime_without_a_topic_resolves_to_microbatch_first(self):
        assert _resolve("realtime") == ModeDecision(
            "realtime", "microbatch", "realtime_not_available"
        )

    def test_realtime_with_its_topic_runs_realtime(self):
        decision = _resolve("realtime", realtime_topic_available=True)
        assert decision == ModeDecision("realtime", "realtime", "")

    def test_realtime_without_row_tracking_walks_down_one_step_at_a_time(self):
        decision = _resolve("realtime", sources_row_tracked=False)
        assert decision.effective == "full"
        assert decision.reason == "realtime_not_available"

    def test_realtime_with_its_topic_but_untracked_sources_is_incremental_then_full(self):
        """The stream bootstraps from ``_slots``, which the change feed fills."""
        decision = _resolve("realtime", realtime_topic_available=True, sources_row_tracked=False)
        assert decision == ModeDecision("realtime", "full", "row_tracking_missing")

    def test_realtime_without_serverless_ends_full_with_the_first_reason(self):
        decision = _resolve("realtime", serverless_available=False)
        assert decision == ModeDecision("realtime", "full", "realtime_not_available")

    def test_realtime_with_its_topic_still_needs_serverless(self):
        decision = _resolve("realtime", realtime_topic_available=True, serverless_available=False)
        assert decision == ModeDecision("realtime", "full", "serverless_unavailable")

    @pytest.mark.parametrize(
        "kwargs",
        [{}, {"serverless_available": False}, {"sources_row_tracked": False}, {"backend": "none"}],
    )
    def test_realtime_needs_its_topic(self, kwargs):
        assert _resolve("realtime", **kwargs).effective != "realtime"


class TestServerlessAndBackendGate:
    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    def test_no_serverless_falls_back_to_full(self, mode):
        assert _resolve(mode, serverless_available=False) == ModeDecision(
            mode, "full", "serverless_unavailable"
        )

    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    @pytest.mark.parametrize("backend", ["none", "", "postgres"])
    def test_ineligible_backend_falls_back_to_full(self, mode, backend):
        assert _resolve(mode, backend=backend) == ModeDecision(
            mode, "full", "backend_not_eligible"
        )

    def test_missing_serverless_is_reported_before_the_backend(self):
        decision = _resolve("incremental", serverless_available=False, backend="none")
        assert decision.reason == "serverless_unavailable"


class TestRowTrackingStep:
    def test_incremental_steps_down_to_full(self):
        assert _resolve("incremental", sources_row_tracked=False) == ModeDecision(
            "incremental", "full", "row_tracking_missing"
        )

    def test_microbatch_walks_down_through_incremental_to_full(self):
        """Both steps test the same input, so the walk ends at full with one reason."""
        assert _resolve("microbatch", sources_row_tracked=False) == ModeDecision(
            "microbatch", "full", "row_tracking_missing"
        )

    def test_row_tracking_is_irrelevant_to_full(self):
        assert _resolve("full", sources_row_tracked=False).effective == "full"

    def test_the_first_downgrade_reason_wins(self):
        """Serverless is checked before row tracking, so its reason survives."""
        decision = _resolve(
            "microbatch", serverless_available=False, sources_row_tracked=False
        )
        assert decision == ModeDecision("microbatch", "full", "serverless_unavailable")


class TestInputHandling:
    @pytest.mark.parametrize("requested", [None, "", "bogus", 7])
    def test_an_invalid_request_is_full(self, requested):
        assert _resolve(requested) == ModeDecision("full", "full", "")

    def test_the_request_is_normalized_in_the_decision(self):
        assert _resolve(" Incremental ") == ModeDecision(
            "incremental", "incremental", ""
        )

    def test_previews_are_accepted_but_do_not_change_the_outcome(self):
        assert _resolve("incremental", previews={"lakeflow": False}) == _resolve(
            "incremental", previews={"lakeflow": True}
        )


def _pipe(info, *, tracked=True, **settings):
    """A pipeline stand-in whose warehouse answers the row-tracking probe."""
    client = MagicMock()
    client.execute_query.return_value = [
        {"key": "delta.enableRowTracking", "value": str(tracked).lower()}
    ]
    return SimpleNamespace(
        domain=SimpleNamespace(info=info),
        settings=SimpleNamespace(**settings),
        task_id="t-1",
        view_table="cat.sch.v",
        source_client=client,
    )


_SOURCES = ["main.sales.customers"]


class TestDecideMode:
    def test_the_default_domain_runs_full_without_a_reason(self):
        decision = TwinBuildPostMaterialize.decide_mode(_pipe({"name": "d"}))
        assert decision == ModeDecision("full", "full", "")

    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    def test_disabled_serverless_pipelines_fall_back_to_full(self, mode):
        pipe = _pipe(
            {"graph_backend": "databricks", "materialization_mode": mode},
            serverless_pipelines_enabled=False,
        )

        decision = TwinBuildPostMaterialize.decide_mode(pipe, source_tables=_SOURCES)

        assert decision == ModeDecision(mode, "full", "serverless_unavailable")
        pipe.source_client.execute_query.assert_not_called()

    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    def test_tracked_sources_keep_the_requested_mode(self, mode):
        pipe = _pipe({"graph_backend": "databricks", "materialization_mode": mode})

        decision = TwinBuildPostMaterialize.decide_mode(pipe, source_tables=_SOURCES)

        assert decision == ModeDecision(mode, mode, "")
        pipe.source_client.execute_query.assert_called_once_with(
            "SHOW TBLPROPERTIES main.sales.customers ('delta.enableRowTracking')"
        )

    def test_microbatch_without_row_tracking_cascades_to_full(self):
        """Ladder steps 3 and 4 read the same input: microbatch never stops at incremental."""
        pipe = _pipe(
            {"graph_backend": "databricks", "materialization_mode": "microbatch"},
            tracked=False,
        )

        decision = TwinBuildPostMaterialize.decide_mode(pipe, source_tables=_SOURCES)

        assert decision == ModeDecision("microbatch", "full", "row_tracking_missing")

    def test_no_row_local_source_counts_as_tracked(self):
        pipe = _pipe({"graph_backend": "databricks", "materialization_mode": "incremental"})

        decision = TwinBuildPostMaterialize.decide_mode(pipe, source_tables=[])

        assert decision.effective == "incremental"
        pipe.source_client.execute_query.assert_not_called()

    def test_the_sources_are_not_probed_before_they_are_known(self):
        """The call at the start of run() has no compiled mappings yet."""
        pipe = _pipe({"graph_backend": "databricks", "materialization_mode": "incremental"})

        decision = TwinBuildPostMaterialize.decide_mode(pipe)

        assert decision == ModeDecision("incremental", "full", "row_tracking_missing")
        pipe.source_client.execute_query.assert_not_called()

    def test_serverless_defaults_to_available(self):
        assert TwinBuildPostMaterialize.serverless_available(SimpleNamespace()) is True
        assert (
            TwinBuildPostMaterialize.serverless_available(
                SimpleNamespace(serverless_pipelines_enabled=False)
            )
            is False
        )

    def test_realtime_reports_its_own_reason(self):
        decision = TwinBuildPostMaterialize.decide_mode(
            _pipe({"materialization_mode": "realtime"})
        )
        assert decision == ModeDecision("realtime", "full", "realtime_not_available")

    def test_realtime_is_available_with_a_complete_source_and_the_setting_on(self):
        from back.objects.digitaltwin.build import TwinBuildPostMaterialize as Post

        info = {"materialization_mode": "realtime", "realtime_source": _COMPLETE_SOURCE}
        on, off = SimpleNamespace(realtime_enabled=True), SimpleNamespace(realtime_enabled=False)
        domain = SimpleNamespace(info=info)

        assert Post._resolver_inputs(domain, on)["realtime_topic_available"] is True
        assert Post._resolver_inputs(domain, off)["realtime_topic_available"] is False

    def test_a_graphless_domain_requesting_a_mode_still_runs_full(self):
        decision = TwinBuildPostMaterialize.decide_mode(
            _pipe({"graph_backend": "none", "materialization_mode": "incremental"})
        )
        assert decision.effective == "full"
        assert decision.requested == "incremental"

    def test_the_backend_input_is_the_domain_backend_not_the_triple_store(self):
        """A ``none`` domain maps to a lakebase triple store; the resolver must see ``none``."""
        from unittest.mock import patch

        with patch.object(
            MaterializationModeResolver, "resolve", wraps=MaterializationModeResolver.resolve
        ) as spy:
            TwinBuildPostMaterialize.decide_mode(
                _pipe({"graph_backend": "none", "materialization_mode": "incremental"})
            )
        assert spy.call_args.kwargs["backend"] == "none"
        assert spy.call_args.kwargs["serverless_available"] is True
        assert spy.call_args.kwargs["sources_row_tracked"] is False
        assert spy.call_args.kwargs["previews"] == {}
