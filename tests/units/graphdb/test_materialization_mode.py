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


class TestRealtime:
    def test_realtime_resolves_to_microbatch_first(self):
        assert _resolve("realtime") == ModeDecision(
            "realtime", "microbatch", "realtime_not_available"
        )

    def test_the_topic_kwarg_is_ignored_in_this_release(self):
        decision = _resolve("realtime", realtime_topic_available=True)
        assert decision == ModeDecision(
            "realtime", "microbatch", "realtime_not_available"
        )

    def test_realtime_without_row_tracking_walks_down_one_step_at_a_time(self):
        decision = _resolve("realtime", sources_row_tracked=False)
        assert decision.effective == "full"
        assert decision.reason == "realtime_not_available"

    def test_realtime_without_serverless_ends_full_with_the_first_reason(self):
        decision = _resolve("realtime", serverless_available=False)
        assert decision == ModeDecision("realtime", "full", "realtime_not_available")

    def test_realtime_is_never_the_effective_mode(self):
        for kwargs in (
            {},
            {"realtime_topic_available": True},
            {"serverless_available": False},
            {"sources_row_tracked": False},
            {"backend": "none"},
        ):
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


def _pipe(info):
    return SimpleNamespace(
        domain=SimpleNamespace(info=info),
        settings=SimpleNamespace(),
        task_id="t-1",
        view_table="cat.sch.v",
    )


class TestDecideMode:
    def test_the_default_domain_runs_full_without_a_reason(self):
        decision = TwinBuildPostMaterialize.decide_mode(_pipe({"name": "d"}))
        assert decision == ModeDecision("full", "full", "")

    @pytest.mark.parametrize("mode", ["incremental", "microbatch"])
    def test_a_requested_mode_falls_back_until_a_probe_exists(self, mode):
        decision = TwinBuildPostMaterialize.decide_mode(
            _pipe({"graph_backend": "databricks", "materialization_mode": mode})
        )
        assert decision == ModeDecision(mode, "full", "serverless_unavailable")

    def test_realtime_reports_its_own_reason(self):
        decision = TwinBuildPostMaterialize.decide_mode(
            _pipe({"materialization_mode": "realtime"})
        )
        assert decision == ModeDecision("realtime", "full", "realtime_not_available")

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
        assert spy.call_args.kwargs["serverless_available"] is False
        assert spy.call_args.kwargs["sources_row_tracked"] is False
        assert spy.call_args.kwargs["previews"] == {}
