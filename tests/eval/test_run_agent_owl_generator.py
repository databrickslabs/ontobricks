"""Focused tests for ``tests/eval/run_agent_owl_generator.py``'s ``--live``
wiring (final-review closure item 1).

``--live`` must invoke the real production staged entry points
(``agents.agent_owl_generator.staged.detect_entities`` /
``infer_relations`` / ``infer_attributes`` / ``infer_axioms``) — never the
deprecated one-shot ``agents.agent_owl_generator.engine.run_agent`` bridge,
which has no remaining production caller (SPEC.md §2). These tests fake only
the network boundary (``staged.call_serving_endpoint``), so ``_live_runner``
and ``staged_contract.score_staged_examples_live`` really do drive
``staged.detect_entities``/``infer_*`` end to end.
"""

from __future__ import annotations

import ast
import inspect
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import run_agent_owl_generator as runner
import staged_contract

from agents.agent_owl_generator import staged

ROOT = Path(__file__).resolve().parents[2]
DATASET = ROOT / "tests/eval/datasets/agent_owl_generator/baseline.jsonl"


def _load_example(example_id: str) -> dict:
    for line in DATASET.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("id") == example_id:
            return row
    raise AssertionError(f"example {example_id!r} not found in dataset")


def _fake_llm_answer(content: str) -> dict:
    return {
        "choices": [{"finish_reason": "stop", "message": {"content": content}}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 10},
    }


class TestModulesNeverImportLegacyEngine:
    """Source-level (AST) proof — mirrors
    ``test_owl_generator_staged.py::test_staged_module_never_imports_legacy_engine``
    — that neither eval module references the deprecated bridge module."""

    @staticmethod
    def _assert_no_engine_import(module) -> None:
        tree = ast.parse(inspect.getsource(module))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert not any(
                    "agent_owl_generator.engine" in alias.name for alias in node.names
                )
            elif isinstance(node, ast.ImportFrom):
                mod = node.module or ""
                assert "agent_owl_generator.engine" not in mod
                if mod == "agents.agent_owl_generator":
                    assert not any(alias.name == "engine" for alias in node.names)

    def test_run_agent_owl_generator_never_imports_legacy_engine(self):
        self._assert_no_engine_import(runner)

    def test_staged_contract_never_imports_legacy_engine(self):
        self._assert_no_engine_import(staged_contract)


class TestLiveRunnerTargetsStagedDetectEntities:
    """``_live_runner`` (parsed-corpus contract, ``--live``) must drive the
    real ``staged.detect_entities()`` entry point, not the legacy bridge."""

    def test_calls_detect_entities_not_run_agent(self):
        example = _load_example("owl-ready-pdf")
        fake_llm = Mock(return_value=_fake_llm_answer(json.dumps({"candidate_entities": []})))
        with patch.object(staged, "call_serving_endpoint", fake_llm), patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy_detect:
            with patch(
                "agents.agent_owl_generator.engine.run_agent",
                side_effect=AssertionError("legacy engine.run_agent must never be called"),
            ):
                tools_called, observed_text, listed_names = runner._live_runner(
                    example, host="https://test.databricks.com", token="tok", endpoint="ep"
                )
        assert spy_detect.call_count == 1
        assert listed_names == ["spec.pdf"]
        assert isinstance(tools_called, list)
        assert isinstance(observed_text, str)

    def test_wires_endpoint_kwargs_through_to_detect_entities(self):
        example = _load_example("owl-ready-pdf")
        fake_llm = Mock(return_value=_fake_llm_answer(json.dumps({"candidate_entities": []})))
        with patch.object(staged, "call_serving_endpoint", fake_llm), patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy_detect:
            runner._live_runner(
                example, host="https://live.databricks.com", token="secret-tok", endpoint="my-ep"
            )
        _, kwargs = spy_detect.call_args
        assert kwargs["host"] == "https://live.databricks.com"
        assert kwargs["token"] == "secret-tok"
        assert kwargs["endpoint_name"] == "my-ep"


class TestScoreStagedExamplesLiveTargetsStagedEntryPoints:
    """``staged_contract.score_staged_examples_live`` must drive the real
    staged entry points for both the detect rows and the completion chain."""

    def test_detect_rows_call_real_detect_entities(self):
        fake_llm = Mock(
            return_value=_fake_llm_answer(
                json.dumps({"candidate_entities": [{"canonical_label": "Carrier"}]})
            )
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm), patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy_detect, patch.object(
            staged, "infer_relations", wraps=staged.infer_relations
        ), patch.object(
            staged, "infer_attributes", wraps=staged.infer_attributes
        ), patch.object(
            staged, "infer_axioms", wraps=staged.infer_axioms
        ):
            scores = staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        assert spy_detect.call_count == 5  # the 5 "detect"-tagged staged rows
        assert "aggregate" in scores
        assert all(isinstance(v, float) for v in scores.values())

    def test_completion_chain_calls_real_infer_relations_attributes_axioms(self):
        fake_llm = Mock(
            return_value=_fake_llm_answer(
                json.dumps({"candidate_entities": [{"canonical_label": "Carrier"}]})
            )
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm), patch.object(
            staged, "infer_relations", wraps=staged.infer_relations
        ) as spy_relations, patch.object(
            staged, "infer_attributes", wraps=staged.infer_attributes
        ) as spy_attributes, patch.object(
            staged, "infer_axioms", wraps=staged.infer_axioms
        ) as spy_axioms:
            staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        assert spy_relations.call_count == 1
        # attributes/axioms only run if the previous substage succeeded —
        # they must at least have been reachable (patched), never skipped
        # in favour of a legacy one-shot call.
        assert spy_attributes.call_count in (0, 1)
        assert spy_axioms.call_count in (0, 1)

    def test_never_calls_legacy_run_agent(self):
        fake_llm = Mock(
            return_value=_fake_llm_answer(json.dumps({"candidate_entities": []}))
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm):
            with patch(
                "agents.agent_owl_generator.engine.run_agent",
                side_effect=AssertionError("legacy engine.run_agent must never be called"),
            ):
                staged_contract.score_staged_examples_live(
                    DATASET, ROOT / "tests/eval/thresholds.yaml",
                    host="https://test.databricks.com", token="tok", endpoint="ep",
                )

    def test_offline_scoring_unaffected_after_live_context_exits(self):
        """`live_endpoint()` must fully restore offline (scripted) scoring —
        a prior --live run must never leak into a later offline run in the
        same process."""
        fake_llm = Mock(
            return_value=_fake_llm_answer(json.dumps({"candidate_entities": []}))
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm):
            staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        assert staged_contract._LIVE_ENDPOINT is None
        # Offline scoring (no mocking of call_serving_endpoint by us here)
        # must still work exactly as before, fully scripted internally.
        aggregate = staged_contract.score_staged_examples(
            DATASET, ROOT / "tests/eval/thresholds.yaml"
        )
        assert aggregate == 1.0


class TestScoreStagedExamplesLiveReportsDetectionFailureExplicitly:
    """Final-review live-eval investigation finding: a rejected/invalid
    ``detect_entities()`` structured-output failure must be reported as its
    own explicit dimension/diagnostic, not just masquerade as a 0.0 on each
    dependent anchor/alt-label dedup dimension (which was indistinguishable
    from a real dedup miss in the prior implementation)."""

    def test_prose_reply_is_reported_as_a_detection_structured_output_failure(
        self, capsys
    ):
        # A real endpoint answering with prose (no tool call, not JSON) is
        # exactly the shape that triggered `staged-locked-anchor-dedup-001`'s
        # live failure: `detect_entities` rejects it before any dedup logic
        # runs.
        fake_llm = Mock(
            return_value=_fake_llm_answer(
                "I'm not sure how to answer that without more context."
            )
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm):
            scores = staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        assert scores["detection_returned_valid_structured_output"] == 0.0
        captured = capsys.readouterr()
        assert "DETECTION FAILED" in captured.out
        # The diagnostic must name the row and explain this is a structured-
        # output failure, not a dedup miss, so it is never confused with the
        # anchor/alternate-label dimensions it otherwise drags down.
        assert "staged-locked-anchor-dedup-001" in captured.out
        assert "not" in captured.out and "dedup" in captured.out

    def test_valid_json_reply_is_reported_as_a_detection_structured_output_success(
        self,
    ):
        fake_llm = Mock(
            return_value=_fake_llm_answer(
                json.dumps({"candidate_entities": [{"canonical_label": "Carrier"}]})
            )
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm):
            scores = staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        assert scores["detection_returned_valid_structured_output"] == 1.0

    def test_does_not_cost_an_extra_live_call_per_row(self):
        """The diagnostic must reuse the already-cached detection result —
        never trigger a second live LLM round-trip per row just to report
        success/failure."""
        fake_llm = Mock(
            return_value=_fake_llm_answer(
                json.dumps({"candidate_entities": [{"canonical_label": "Carrier"}]})
            )
        )
        with patch.object(staged, "call_serving_endpoint", fake_llm) as spy_llm, patch.object(
            staged, "detect_entities", wraps=staged.detect_entities
        ) as spy_detect:
            staged_contract.score_staged_examples_live(
                DATASET, ROOT / "tests/eval/thresholds.yaml",
                host="https://test.databricks.com", token="tok", endpoint="ep",
            )
        # 5 detect-tagged rows -> exactly one detect_entities() call each,
        # regardless of how many constraints each row declares (dedup/
        # inclusion/synonym/zero-candidate constraints total across these 5
        # rows) — the diagnostic reuses the cached result, never a second
        # detect_entities() invocation per row.
        assert spy_detect.call_count == 5
        # Each detect_entities() call now makes exactly TWO LLM round-trips
        # (live-reliability fix: a bounded tool-gathering call whose
        # no-tool-call content is discarded, then one separate
        # schema-enforced finalization call — see
        # `staged.detect_entities`'s two-phase docstring) -> 5 * 2 = 10,
        # +1 for the completion chain's "relations" substage call (it stops
        # there since this fake reply rejects as a relations payload) — no
        # extra calls beyond two per detect row plus the chain's own calls.
        assert spy_llm.call_count == 11


class TestLiveModeInitializesTracingBeforeAnyFoundationModelCall:
    """Final-review live-eval investigation finding: ``--live`` never called
    ``agents.tracing.setup_tracing()``, so the ``@trace_agent``/``@trace_llm``
    decorators already on ``staged.detect_entities``/``infer_*``/
    ``call_serving_endpoint`` silently no-opped — the live MLflow run carried
    zero trace/span evidence. ``_init_live_tracing`` must call
    ``setup_tracing()`` (never passing ``host``/``token``) before
    ``run_contract`` — and therefore before ``_live_runner``/
    ``score_staged_examples_live`` make their first real Foundation Model
    call."""

    def test_init_live_tracing_calls_setup_tracing_with_the_configured_experiment(
        self,
    ):
        with patch.object(runner, "setup_tracing") as mock_setup, patch.object(
            runner, "mlflow"
        ) as mock_mlflow:
            mock_setup.return_value = True
            result = runner._init_live_tracing("databricks", "/Shared/my-exp")
        assert result is True
        mock_setup.assert_called_once_with(experiment_name="/Shared/my-exp")
        mock_mlflow.set_tracking_uri.assert_called_once_with("databricks")

    def test_init_live_tracing_never_passes_host_or_token(self):
        """``setup_tracing()``'s only parameter is the experiment name — no
        secret can be logged through this call by construction."""
        with patch.object(runner, "setup_tracing") as mock_setup, patch.object(
            runner, "mlflow"
        ):
            runner._init_live_tracing("databricks", "/Shared/my-exp")
        _, kwargs = mock_setup.call_args
        assert set(kwargs) == {"experiment_name"}
        assert "host" not in kwargs and "token" not in kwargs

    def test_init_live_tracing_diagnostic_print_never_contains_a_token(
        self, capsys
    ):
        with patch.object(runner, "setup_tracing", return_value=False), patch.object(
            runner, "mlflow"
        ):
            runner._init_live_tracing("databricks", "/Shared/my-exp")
        captured = capsys.readouterr()
        assert "super-secret-token-value" not in captured.out

    def test_main_calls_tracing_setup_before_run_contract_and_before_live_runner(
        self, monkeypatch
    ):
        order = []
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "run_agent_owl_generator.py",
                "--live",
                "--host",
                "https://test.databricks.com",
                "--token",
                "super-secret-token-value",
                "--endpoint",
                "ep",
            ],
        )
        monkeypatch.setattr(
            runner,
            "_init_live_tracing",
            lambda *a, **k: order.append("tracing") or True,
        )
        monkeypatch.setattr(
            runner, "score_staged_examples", lambda *a, **k: order.append("staged_score")
        )
        monkeypatch.setattr(
            runner,
            "_live_runner",
            lambda *a, **k: (order.append("live_runner"), [], "", [])[1:],
        )

        def fake_run_contract(**kwargs):
            order.append("run_contract")
            # Simulate what run_contract really does: call the live runner
            # for at least one example, from inside this same call.
            kwargs["live_runner"]({"input": {"documents": []}})

        monkeypatch.setattr(runner, "run_contract", fake_run_contract)
        runner.main()

        assert order.index("tracing") < order.index("run_contract")
        assert order.index("tracing") < order.index("live_runner")

    def test_offline_mode_never_initializes_tracing(self, monkeypatch):
        """``--live`` is required for tracing setup — the deterministic/
        scripted offline contract stays untouched (no MLflow side effects)."""
        monkeypatch.setattr(
            sys,
            "argv",
            ["run_agent_owl_generator.py"],
        )
        mock_init = MagicMock()
        monkeypatch.setattr(runner, "_init_live_tracing", mock_init)
        monkeypatch.setattr(runner, "score_staged_examples", lambda *a, **k: None)
        monkeypatch.setattr(runner, "run_contract", lambda **k: None)
        runner.main()
        mock_init.assert_not_called()
