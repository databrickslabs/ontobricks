"""Help Center workflow copy must stay in sync with menu_config.json.

Regression guard for drift found during the 2026-08-05 Help Center review:
the Step-by-Step Workflow accordion referenced menu items ("Graph Viewer",
"GraphQL", Registry's "Schedule"/"API") that had been renamed or moved
(Explorer/Query, Settings) without the Help Center copy being updated, and
missed newer sections entirely (Chat, Cohorts, Analytics, Build/Runs,
Pitfalls). These tests lock in the current, correct labels.
"""

import html
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_HELP = Path("src/front/templates/partials/layout/help_modal.html")
_MENU = Path("src/front/config/menu_config.json")


def _help_html() -> str:
    return _HELP.read_text(encoding="utf-8")


def _menus():
    return {m["id"]: m for m in json.loads(_MENU.read_text(encoding="utf-8"))["menus"]}


def test_registry_workflow_step_matches_menu_icon():
    html = _help_html()
    icon = _menus()["registry"]["icon"]
    assert f'<i class="bi {icon} me-2"></i><strong>1. Registry</strong>' in html
    # Schedule/API/Registry Location live under Settings, not the Registry modal.
    assert "<strong>Schedule</strong>" not in html
    assert "<strong>Registry Location</strong>" not in html


def test_knowledge_graph_workflow_uses_current_item_labels():
    """digitaltwin-data items were renamed from Graph Viewer/GraphQL to Explorer/Query."""
    html = _help_html()
    digitaltwin = _menus()["digitaltwin"]
    all_items = {
        item["id"]: item["label"]
        for group in digitaltwin["groups"]
        for item in group["items"]
    }
    assert all_items["sigmagraph"] == "Explorer"
    assert all_items["graphql"] == "Query"

    hw5_start = html.index('id="hw5"')
    hw5_end = html.index("</div>\n                                </div>", hw5_start)
    hw5_body = html[hw5_start:hw5_end]

    assert "<strong>Explorer</strong>" in hw5_body
    assert "<strong>Query</strong>" in hw5_body
    # Every real digitaltwin item must be mentioned so the guide can't silently drift again.
    for label in all_items.values():
        assert label in hw5_body, f"Knowledge Graph workflow doc is missing '{label}'"


def test_ontology_advanced_list_matches_menu_items():
    """ontology-advanced group items must all be named under 'Advanced features'."""
    page = _help_html()
    ontology = _menus()["ontology"]
    advanced_group = next(g for g in ontology["groups"] if g["id"] == "ontology-advanced")
    advanced_labels = {item["label"] for item in advanced_group["items"]}

    hw3_start = page.index('id="hw3"')
    hw3_end = page.index("</div>\n                                </div>", hw3_start)
    hw3_body = page[hw3_start:hw3_end]
    advanced_start = hw3_body.index("Advanced features")
    advanced_body = hw3_body[advanced_start:]

    for label in advanced_labels:
        escaped = html.escape(label)
        assert escaped in advanced_body, f"Ontology Advanced doc is missing '{label}'"
