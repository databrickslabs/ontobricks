"""Offline tests for the #173 large-schema stress harness.

The harness is meant to be run live (see ``large_schema_stress.py``); these
tests script the LLM so CI proves the harness itself drives the real
detect -> relations -> attributes -> axioms chain, observes paging, and
reports the failing stage correctly.
"""

from __future__ import annotations

import json
import re

import pytest

import large_schema_stress as stress


def _answer(content: str, finish_reason: str = "stop") -> dict:
    return {
        "choices": [{"finish_reason": finish_reason, "message": {"content": content}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 50},
    }


class _ScriptedLLM:
    """Answers by stage. Attributes arrive in 3 pages of 10 to prove the
    harness observes paging; ``truncate_attributes`` makes every attributes
    call hit the output cap."""

    def __init__(self, truncate_attributes: bool = False):
        self.truncate_attributes = truncate_attributes
        self.attribute_pages = 0

    def __call__(self, host, token, endpoint, messages, **kwargs):
        stage = kwargs["trace_name"].rsplit(".", 1)[-1]
        if stage == "detect":
            if kwargs.get("tools"):
                return _answer("")  # no tool call -> gathering ends
            labels = ["Shipment", "Carrier", "Port"]
            return _answer(
                json.dumps(
                    {"candidate_entities": [{"canonical_label": x} for x in labels]}
                )
            )
        ids = re.findall(r"id: ([\w-]+)", messages[1]["content"])
        if stage == "relations":
            rel = {"label": "carries", "domain": ids[1], "range": ids[0]}
            return _answer(json.dumps({"relations": [rel], "has_more": False}))
        if stage == "attributes":
            if self.truncate_attributes:
                return _answer('{"attributes": [{"label": "x', "length")
            page = self.attribute_pages
            self.attribute_pages += 1
            attrs = [
                {"label": f"attr{page}_{j}", "domain": ids[0], "datatype": "xsd:string"}
                for j in range(10)
            ]
            return _answer(json.dumps({"attributes": attrs, "has_more": page < 2}))
        return _answer(json.dumps({"axioms": [], "has_more": False}))


def _run(name: str, llm) -> dict:
    return stress.run_scenario(name, host="h", token="t", endpoint="e", llm=llm)


@pytest.mark.parametrize(
    "name, tables, cols", [("wide_table_360", 1, 360), ("many_tables_100x18", 100, 18)]
)
def test_synthetic_metadata_matches_issue_shapes(name, tables, cols):
    md = stress.synthetic_metadata(*stress.SCENARIOS[name])
    assert len(md["tables"]) == tables
    full_names = [t["full_name"] for t in md["tables"]]
    assert len(set(full_names)) == tables
    for table in md["tables"]:
        names = [c["name"] for c in table["columns"]]
        assert len(names) == cols
        assert len(set(names)) == cols, "column names must be unique"


def test_detection_visibility_measures_the_real_metadata_tool_caps():
    # Documents the Stage-1 side of #173: the detection overview caps
    # columns per table and tables, so most of a wide schema is invisible
    # unless the model calls get_table_detail.
    wide = stress.detection_visibility(stress.synthetic_metadata(1, 360))
    assert wide["columns_total"] == 360
    assert wide["columns_visible"] < wide["columns_total"]
    many = stress.detection_visibility(stress.synthetic_metadata(100, 18))
    assert many["tables_total"] == 100
    assert many["tables_visible"] < many["tables_total"]


def test_full_chain_reports_paging_per_stage():
    report = _run("wide_table_360", _ScriptedLLM())
    assert report["success"] is True
    assert report["stage_reached"] == "done"
    stages = report["stages"]
    assert stages["detect"]["items"] == 3
    assert stages["relations"]["items"] == 1
    assert stages["attributes"]["items"] == 30
    assert stages["attributes"]["llm_calls"] == 3
    # Paged requests stay a single [system, user] exchange.
    assert stages["attributes"]["max_messages"] == 2
    assert stages["attributes"]["completion_tokens"] == 150
    json.dumps(report)  # report must be JSON-serialisable for MLflow


def test_failing_stage_and_user_message_are_reported():
    report = _run("many_tables_100x18", _ScriptedLLM(truncate_attributes=True))
    assert report["success"] is False
    assert report["stage_reached"] == "attributes"
    assert report["stages"]["attributes"]["truncated_calls"] == 2
    assert "truncated" in report["user_message"]
    assert "attributes" not in [s for s, v in report["stages"].items() if v["success"]]
