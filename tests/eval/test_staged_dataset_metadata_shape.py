"""Dataset-fixture contract: staged rows' ``input.metadata.tables`` must use
the same shape production metadata tools consume.

Root cause (final-review live-eval investigation, see SPEC.md §10): four
staged dataset rows used ``{catalog, schema, table}`` keys. Production's
``agents.tools.metadata`` (``tool_get_metadata``/``tool_get_table_detail``)
and the staged/legacy orchestrators (``agents.agent_owl_generator.staged``/
``engine``) key every table entry off ``name``/``full_name`` (see
``back/core/databricks/uc/MetadataService.py``'s real Unity Catalog fetch
for the production shape) — never ``catalog``/``schema``/``table``. Against
a live endpoint this silently produced table entries with ``name: null``
from ``get_metadata``, so ``staged-locked-anchor-dedup-001`` (which has no
``input.corpus`` fallback to ground the model otherwise) got no usable
schema info, the model replied with prose instead of JSON, and
``detect_entities`` rejected the response *before* its anchor-dedup logic
ever ran (the reported anchor/alternate-label failures were downstream of
this, not a dedup miss). ``staged-detect-new-entities-001`` has the
identical malformed shape but happened to pass because its ``input.corpus``
grounded the model independently of the (broken) metadata tool.

This fixes the fixture at the source — no production metadata tolerance for
``catalog``/``schema``/``table`` is added anywhere; that shape was never a
real production shape.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from agents.tools.context import ToolContext
from agents.tools.metadata import tool_get_metadata, tool_get_table_detail

ROOT = Path(__file__).resolve().parents[2]
DATASET_PATHS = [
    ROOT / "tests/eval/datasets/agent_owl_generator/baseline.jsonl",
    ROOT / ".planning/agents/agent_owl_generator/eval/dataset.jsonl",
]


def _load_rows(path: Path) -> List[Dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class TestMirroredDatasetsStayByteIdentical:
    def test_baseline_and_planning_mirror_match(self):
        contents = [path.read_text(encoding="utf-8") for path in DATASET_PATHS]
        assert contents[0] == contents[1], (
            "tests/eval/datasets/agent_owl_generator/baseline.jsonl and "
            ".planning/agents/agent_owl_generator/eval/dataset.jsonl must "
            "stay byte-identical mirrors"
        )


@pytest.mark.parametrize(
    "dataset_path", DATASET_PATHS, ids=[p.name for p in DATASET_PATHS]
)
class TestStagedMetadataTablesMatchProductionShape:
    """Every ``input.metadata.tables[*]`` entry in a staged row must carry
    the ``name``/``full_name`` keys production metadata tools require
    (``t.get("name")``/``t.get("full_name")`` in
    ``agents.tools.metadata.tool_get_metadata`` and
    ``t.get("full_name") or t.get("name")`` in
    ``agents.agent_owl_generator.staged``/``engine``)."""

    def test_no_row_uses_the_stale_catalog_schema_table_shape(self, dataset_path):
        rows = _load_rows(dataset_path)
        offenders = []
        for row in rows:
            tables = row.get("input", {}).get("metadata", {}).get("tables", [])
            for table in tables:
                if "catalog" in table or "schema" in table or "table" in table:
                    offenders.append(row["id"])
        assert offenders == [], (
            f"{dataset_path}: rows still use the stale catalog/schema/table "
            f"metadata shape (production uses name/full_name): {offenders}"
        )

    def test_every_metadata_table_has_name_and_full_name(self, dataset_path):
        rows = _load_rows(dataset_path)
        for row in rows:
            tables = row.get("input", {}).get("metadata", {}).get("tables", [])
            for table in tables:
                assert table.get(
                    "name"
                ), f"{row['id']}: metadata table missing 'name': {table!r}"
                assert table.get(
                    "full_name"
                ), f"{row['id']}: metadata table missing 'full_name': {table!r}"
                assert table["full_name"].endswith(table["name"]), (
                    f"{row['id']}: full_name {table['full_name']!r} does not "
                    f"end with name {table['name']!r}"
                )

    def test_locked_anchor_dedup_row_has_the_expected_resolved_table(
        self, dataset_path
    ):
        """The specific row that failed live: proves the exact realistic
        shape landed, not just presence of the keys."""
        rows = _load_rows(dataset_path)
        row = next(r for r in rows if r["id"] == "staged-locked-anchor-dedup-001")
        tables = row["input"]["metadata"]["tables"]
        assert len(tables) == 1
        table = tables[0]
        assert table["name"] == "purchase_orders"
        assert table["full_name"] == "demo.sales.purchase_orders"
        assert table["columns"][0]["name"] == "po_id"


class TestProductionMetadataToolsResolveDatasetRows:
    """Direct regression test against the real tool functions the LLM
    calls — catches the root-cause bug at its source (null table names from
    ``get_metadata``/``get_table_detail``) rather than only checking JSON
    field names."""

    def test_locked_anchor_dedup_row_resolves_via_real_tool_functions(self):
        rows = _load_rows(DATASET_PATHS[0])
        row = next(r for r in rows if r["id"] == "staged-locked-anchor-dedup-001")
        ctx = ToolContext(host="h", token="t", metadata=row["input"]["metadata"])

        listing = json.loads(tool_get_metadata(ctx))
        assert listing["tables"][0]["name"] == "purchase_orders"
        assert listing["tables"][0]["full_name"] == "demo.sales.purchase_orders"

        detail = json.loads(
            tool_get_table_detail(ctx, table_name="demo.sales.purchase_orders")
        )
        assert "error" not in detail
        assert detail["columns"][0]["name"] == "po_id"

    def test_all_staged_rows_with_metadata_resolve_via_real_tool_functions(self):
        """No staged row's metadata tables should ever produce a null name
        from the real ``get_metadata`` tool (the root cause of this bug)."""
        rows = _load_rows(DATASET_PATHS[0])
        for row in rows:
            if "staged" not in row.get("tags", []):
                continue
            tables = row.get("input", {}).get("metadata", {}).get("tables", [])
            if not tables:
                continue
            ctx = ToolContext(host="h", token="t", metadata=row["input"]["metadata"])
            listing = json.loads(tool_get_metadata(ctx))
            for entry in listing["tables"]:
                assert entry["name"] is not None, (
                    f"{row['id']}: get_metadata returned a null table name"
                )
                assert entry["full_name"] is not None, (
                    f"{row['id']}: get_metadata returned a null full_name"
                )
