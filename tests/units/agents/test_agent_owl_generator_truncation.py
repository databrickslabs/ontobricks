"""Truncation-guard tests for ``agents.agent_owl_generator.engine``.

Regression coverage for the scenario-1 failure where the OWL generator
accepted a length-truncated LLM response as the final ontology: the cut-off
Turtle failed to parse in every RDF syntax downstream and landed an empty
ontology (0 classes), which the generation wizard polled on until timeout.

``call_serving_endpoint`` is patched to return scripted responses carrying a
``finish_reason`` so we can drive the guard without a live endpoint. The
generation-quality (pitfall) loop is disabled via
``options={"generation_max_iterations": 0}`` so a complete Turtle answer is
accepted directly.
"""

from __future__ import annotations

from unittest.mock import patch

from agents.agent_owl_generator import engine as owl_engine


# The exact guideline used by Ontology → Generate / scenario 1.
_CRM_GUIDELINE = (
    "Generate a simple ontology for a Customer Relationship Management (CRM) "
    "domain in the energy sector:\n"
    "- Create entities for customers, contacts, interactions, invoices, and "
    "meter informations\n"
    "- Include relationships like 'hasContact', 'belongsToAccount', 'relatedTo'\n"
    "- Consider that customers can have multiple contacts and interactions\n"
    "- Include properties for status, dates, and monetary values\n"
    "- Focus on the customer journey"
)

_COMPLETE_TURTLE = (
    "@prefix : <http://ex.org/crm#> .\n"
    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
    ":Customer a owl:Class .\n"
    ":Contact a owl:Class .\n"
)


def _truncated(content: str = ":Customer a owl:Cla") -> dict:
    """A length-capped completion — Turtle cut off mid-statement."""
    return {
        "choices": [{"finish_reason": "length", "message": {"content": content}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 8192},
    }


def _complete(turtle: str = _COMPLETE_TURTLE) -> dict:
    return {
        "choices": [{"finish_reason": "stop", "message": {"content": turtle}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 200},
    }


def _run(responses):
    with patch.object(owl_engine, "call_serving_endpoint") as mock_llm:
        mock_llm.side_effect = responses
        return owl_engine.run_agent(
            host="https://test.databricks.com",
            token="tok",
            endpoint_name="dbx-llm",
            registry={"catalog": "main", "schema": "ob", "volume": "documents"},
            metadata={"tables": []},
            guidelines=_CRM_GUIDELINE,
            options={"generation_max_iterations": 0},
            base_uri="http://ex.org/crm#",
        )


class TestTruncationGuard:
    def test_truncated_then_complete_recovers(self):
        """A length-truncated answer is retried, and the complete re-emission
        is accepted as the final ontology."""
        result = _run([_truncated(), _complete()])
        assert result.success is True
        assert result.owl_content == _COMPLETE_TURTLE
        assert result.iterations == 2

    def test_truncated_answer_is_not_accepted_as_final(self):
        """A single truncated answer must never be accepted verbatim — the
        old bug set success=True with the cut-off body."""
        result = _run([_truncated(), _complete()])
        # The accepted content is the *complete* re-emission, never the stub.
        assert ":Customer a owl:Cla\n" not in result.owl_content
        assert result.owl_content.strip().startswith("@prefix")

    def test_persistent_truncation_fails_loudly(self):
        """If every attempt truncates, the run fails with a clear error rather
        than silently producing an empty ontology."""
        result = _run(lambda *a, **k: _truncated())
        assert result.success is False
        assert "truncat" in (result.error or "").lower()
