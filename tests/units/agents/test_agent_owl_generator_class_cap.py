"""Over-generation (class-cap) guard tests for
``agents.agent_owl_generator.engine``.

Regression coverage for the scenario-1 auto-map timeout: the generator emitted
a 110-class ontology (one class per column/value) which ballooned downstream
auto-mapping (~5 classes/chunk with cool-downs → ~22 chunks) past the scenario
budget. The guard asks the model — bounded — to consolidate to the core
entities before accepting.

``call_serving_endpoint`` is patched to return scripted Turtle answers so the
guard runs without a live endpoint. The pitfall/quality loop is disabled via
``options={"generation_max_iterations": 0}`` to isolate the class-cap guard.
"""

from __future__ import annotations

from unittest.mock import patch

from agents.agent_owl_generator import engine as owl_engine


_CRM_GUIDELINE = (
    "Generate a simple ontology for a Customer Relationship Management (CRM) "
    "domain in the energy sector: customers, contacts, interactions, invoices."
)


def _turtle_with_classes(n: int, prefix: str = "C") -> str:
    """Build a valid-ish Turtle string with *n* owl:Class declarations."""
    lines = [
        "@prefix : <http://ex.org/crm#> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
    ]
    lines += [f":{prefix}{i} a owl:Class ." for i in range(n)]
    return "\n".join(lines) + "\n"


def _complete(turtle: str) -> dict:
    return {
        "choices": [{"finish_reason": "stop", "message": {"content": turtle}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 200},
    }


def _run(responses, options=None):
    opts = {"generation_max_iterations": 0}
    if options:
        opts.update(options)
    with patch.object(owl_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = responses
        return owl_engine.run_agent(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            registry={"catalog": "main", "schema": "ob", "volume": "documents"},
            metadata={"tables": []},
            guidelines=_CRM_GUIDELINE,
            options=opts,
            base_uri="http://ex.org/crm#",
        )


class TestCountHelper:
    def test_counts_class_declarations(self):
        assert owl_engine._count_owl_classes(_turtle_with_classes(7)) == 7

    def test_counts_rdf_type_form(self):
        turtle = (
            "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
            ":A rdf:type owl:Class .\n:B a owl:Class .\n"
        )
        assert owl_engine._count_owl_classes(turtle) == 2

    def test_empty_is_zero(self):
        assert owl_engine._count_owl_classes("") == 0


class TestClassCapGuard:
    def test_overcap_then_consolidated_recovers(self):
        """An over-cap ontology is challenged; the consolidated re-emission is
        accepted as the final ontology."""
        big = _turtle_with_classes(60)
        small = _turtle_with_classes(8)
        result = _run([_complete(big), _complete(small)])
        assert result.success is True
        assert result.owl_content == small
        assert result.iterations == 2

    def test_under_cap_accepted_directly(self):
        """A concise ontology is accepted on the first pass (no consolidation)."""
        small = _turtle_with_classes(12)
        result = _run([_complete(small)])
        assert result.success is True
        assert result.owl_content == small
        assert result.iterations == 1

    def test_guard_can_be_disabled(self):
        """max_classes<=0 disables the guard — a large ontology is accepted."""
        big = _turtle_with_classes(60)
        result = _run([_complete(big)], options={"max_classes": 0})
        assert result.success is True
        assert result.owl_content == big
        assert result.iterations == 1

    def test_persistent_overcap_accepts_best_after_bound(self):
        """If the model never consolidates, the guard stops after the bounded
        number of rounds and accepts the best available ontology rather than
        looping forever."""
        big = _turtle_with_classes(60)
        # round 1 + round 2 retries, then accept on the 3rd answer.
        result = _run([_complete(big), _complete(big), _complete(big)])
        assert result.success is True
        assert owl_engine._count_owl_classes(result.owl_content) == 60
        assert result.iterations == 1 + owl_engine._MAX_CONSOLIDATE_ROUNDS
