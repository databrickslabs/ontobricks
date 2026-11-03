"""Regression for GH #214 — collapsed Unity Catalog FQNs in mapping SQL."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agents.agent_auto_assignment.engine import SYSTEM_PROMPT
from agents.agent_mapping_pge.generators.entity import SYSTEM_PROMPT as PGE_ENTITY_PROMPT
from agents.agent_mapping_pge.generators.relationship import (
    SYSTEM_PROMPT as PGE_REL_PROMPT,
)
from agents.tools.context import ToolContext
from agents.tools.sql import tool_execute_sql

pytestmark = pytest.mark.unit

_FORBIDDEN = "`catalog.schema.table`"
_ALLOWED_SPLIT = "`catalog`.`schema`.`table`"


def test_auto_map_prompt_forbids_collapsed_uc_fqn():
    assert "never wrap the full catalog.schema.table" in SYSTEM_PROMPT.lower()
    assert _FORBIDDEN in SYSTEM_PROMPT
    assert _ALLOWED_SPLIT in SYSTEM_PROMPT


def test_pge_prompts_forbid_collapsed_uc_fqn():
    for prompt in (PGE_ENTITY_PROMPT, PGE_REL_PROMPT):
        assert "never wrap the full catalog.schema.table" in prompt.lower()
        assert _FORBIDDEN in prompt
        assert _ALLOWED_SPLIT in prompt


_ROOT = Path(__file__).resolve().parents[3]
_REGRESSION_DATASETS = (
    _ROOT / "tests/eval/datasets/agent_auto_assignment/regression.jsonl",
    _ROOT / "tests/eval/datasets/agent_mapping_pge/regression.jsonl",
)


def _load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_eval_regression_rows_score_the_rewrite():
    from back.core.helpers.SQLHelpers import SQLHelpers

    rows = []
    for path in _REGRESSION_DATASETS:
        rows.extend(_load_jsonl(path))
    uc_rows = [r for r in rows if "uc_fqn" in r.get("tags", [])]
    assert len(uc_rows) >= 6
    for row in uc_rows:
        original = row["input"]["sql_query"]
        rewritten = SQLHelpers.rewrite_collapsed_uc_fqn(original)
        kinds = {c["kind"]: c for c in row["expected"]["constraints"]}
        assert SQLHelpers.find_collapsed_uc_fqn(rewritten) == []
        if "sql_contains" in kinds:
            assert kinds["sql_contains"]["value"] in rewritten
        if "sql_unchanged" in kinds:
            assert rewritten == original


def test_execute_sql_rewrites_collapsed_uc_fqn_before_warehouse():
    client = MagicMock()
    client.execute_query.return_value = [{"ID": "1"}]
    ctx = ToolContext(host="https://test.databricks.com", token="fake", client=client)
    payload = json.loads(
        tool_execute_sql(
            ctx,
            sql=(
                "SELECT participant_id AS ID FROM "
                "`catalog_poc_dard.anonym_bcp_ddv_lyt_modeloloyalty."
                "hd_activity_participant`"
            ),
        )
    )
    assert payload["success"] is True
    sent = client.execute_query.call_args[0][0]
    assert (
        "`catalog_poc_dard`.`anonym_bcp_ddv_lyt_modeloloyalty`"
        ".`hd_activity_participant`"
    ) in sent
    assert (
        "`catalog_poc_dard.anonym_bcp_ddv_lyt_modeloloyalty."
        "hd_activity_participant`"
    ) not in sent
