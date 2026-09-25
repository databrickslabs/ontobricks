"""MCP /triples/find formatting contracts."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
MCP_SRC = REPO_ROOT / "src" / "mcp-server"

if str(MCP_SRC) not in sys.path:
    sys.path.insert(0, str(MCP_SRC))

from server.formatting import _format_find_response


def test_find_pagination_hint_uses_explicit_has_more():
    payload = {
        "success": True,
        "seed_count": 1,
        "depth": 1,
        "total": 7,
        "triples": [
            {
                "subject": "https://ex/Customer/CUST1",
                "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
                "object": "Cust One",
            }
        ],
    }

    text_no_more = _format_find_response({**payload, "has_more": False})
    text_with_more = _format_find_response({**payload, "has_more": True})

    assert "7 triples across" in text_no_more
    assert "Showing 1 of 7 triples" not in text_no_more
    assert "Showing 1 of 7 triples" in text_with_more
