"""MCP describe_entity formatting contracts for paging metadata."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))

from server.formatting import _format_find_response  # type: ignore[import-not-found]


def test_format_find_uses_exact_total_and_has_more_hint():
    text = _format_find_response(
        {
            "success": True,
            "seed_count": 1,
            "depth": 1,
            "count": 2,
            "total": 7,
            "entity_count": 3,
            "has_more": True,
            "triples": [
                {
                    "subject": "https://ex/Customer/CUST1",
                    "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
                    "object": "Cust One",
                },
                {
                    "subject": "https://ex/Customer/CUST1",
                    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    "object": "https://ex/Customer",
                },
            ],
        }
    )

    assert "2 of 7 triples" in text
    assert "increase limit or use pagination for more" in text
