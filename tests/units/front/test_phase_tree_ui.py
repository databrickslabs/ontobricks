"""Contract for the shared phase tree (Graph → Inference, Simulation → Reasoning,
Graph → Data quality)."""

import re
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

FRONT = Path(__file__).resolve().parents[3] / "src" / "front"
TEMPLATES = FRONT / "templates"

_CTX = {
    "owlrl_available": False,
    "if_then_rules_count": 3,
    "object_properties_count": 1,
    "sparql_rules_count": 0,
}
_IDS = {"tbox": "pT", "if_then": "pI", "graph": "pG", "sparql_rules": "pS"}


def _read(rel: str) -> str:
    return (FRONT / rel).read_text(encoding="utf-8")


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(str(TEMPLATES)))


def _render_inference(**overrides) -> str:
    ctx = {
        "tree_id": "tree",
        "tree_ctx": _CTX,
        "tree_ids": _IDS,
        "tree_checked": ["tbox", "if_then", "graph"],
        "tree_lock_tbox": True,
        **overrides,
    }
    return _env().get_template("partials/layout/_inference_phase_tree.html").render(**ctx)


def _render_generic(nodes, tree_id="tree") -> str:
    return _env().get_template("partials/layout/_phase_tree.html").render(
        tree_id=tree_id, tree_caption="Things", tree_nodes=nodes
    )


# ── Inference phases ────────────────────────────────────────


def test_inference_lists_phases_in_execution_order_with_icons():
    html = _render_inference()
    order = [html.index(f'data-tree-node="{k}"') for k in ("tbox", "if_then", "graph", "sparql_rules")]
    assert order == sorted(order)
    for icon in ("bi-diagram-3", "bi-code-square", "bi-share", "bi-braces"):
        assert icon in html
    assert 'role="tree"' in html
    assert "onclick=" not in html and "onchange=" not in html


def test_rule_phases_are_expandable_and_badged():
    html = _render_inference()
    assert 'data-tree-toggle="if_then"' in html
    assert 'data-tree-children="sparql_rules"' in html
    assert 'data-tree-toggle="graph"' not in html
    assert 'data-tree-count="if_then"' in html
    assert "3 rules" in html
    assert "no rules" in html
    assert "1 object prop<" in html


def test_tbox_locks_without_owlrl_only_when_asked():
    locked = _render_inference()
    assert re.search(r'id="pT" data-phase="tbox"\s+disabled data-tree-locked', locked)
    assert not re.search(r'id="pT"[^>]*checked', locked)
    unlocked = _render_inference(tree_lock_tbox=False)
    assert "data-tree-locked" not in unlocked


# ── Generic partial ─────────────────────────────────────────


def test_generic_node_without_badge_renders_no_badge():
    html = _render_generic([
        {"key": "a", "input_id": "ia", "icon": "bi-x", "tone": "", "title": "A", "desc": "d", "checked": True},
    ])
    assert 'data-phase="a"' in html
    assert "ob-tree-badge" not in html


def test_generic_expandable_node_keeps_an_empty_count_badge_for_sync():
    html = _render_generic([
        {"key": "a", "input_id": "ia", "icon": "bi-x", "tone": "", "title": "A", "desc": "d",
         "checked": True, "expandable": True, "leaf_icon": "bi-y"},
    ])
    assert 'data-tree-count="a"' in html
    assert 'data-tree-children="a"' in html
    assert 'data-leaf-icon="bi-y"' in html


def test_generic_caption_names_the_toolbar_and_the_tree():
    html = _render_generic([])
    assert 'aria-label="Things"' in html
    assert "Things" in html


# ── Wiring on each screen ───────────────────────────────────


def test_both_inference_screens_use_the_wrapper_and_load_phase_tree_js():
    graph = _read("templates/partials/dtwin/_query_reasoning.html")
    sim = _read("templates/partials/simulation/_sim_workbench.html")
    for tpl, tree_id in ((graph, "reasoningPhaseTree"), (sim, "simWbPhases")):
        assert "partials/layout/_inference_phase_tree.html" in tpl
        assert f'tree_id="{tree_id}"' in tpl
    for page, module in (("templates/dtwin.html", "query/js/query-reasoning.js"),
                         ("templates/simulation.html", "simulation/js/simulation-rules.js")):
        src = _read(page)
        assert src.index("global/js/phase-tree.js") < src.index(module)
    assert "reasoning-tile" not in graph
    assert "toggleAllOptions" not in graph


def test_data_quality_uses_the_shared_tree_with_one_node_per_dimension():
    html = _read("templates/partials/dtwin/_query_dataquality.html")
    assert "partials/layout/_phase_tree.html" in html
    assert 'tree_id = "dqPhaseTree"' in html
    for dim in ("completeness", "cardinality", "uniqueness", "consistency", "conformance", "structural"):
        assert f'"key": "{dim}"' in html
    for gone in ("reasoning-tile", "dq-rules-toggle", "dq-rule-list", "data-dimension", "toggle-all-dims"):
        assert gone not in html


def test_data_quality_script_loads_phase_tree_before_its_module():
    src = _read("templates/dtwin.html")
    assert src.index("global/js/phase-tree.js") < src.index("query/js/query-dataquality.js")


def test_reasoning_module_delegates_rule_selection_to_tree():
    js = _read("static/query/js/query-reasoning.js")
    assert "new PhaseTree(" in js
    assert "selectedRules(phases)" in js
    assert "PhaseTree.fromNamedRules(" in js
    assert "if_then_rule_names" in js and "sparql_rule_names" in js
    for gone in ("_RULE_TILE_MAP", "_toggleRuleList", "toggleAllOptions", "_syncTile", "RULE_TYPES"):
        assert gone not in js


def test_phase_tree_exposes_shared_api():
    js = _read("static/global/js/phase-tree.js")
    for method in ("renderRules(", "fromNamedRules(", "expandableKeys(", "phases()", "selectedRules(",
                   "checkedValues(", "applyRules(", "setAll(", "setBusy(", "expand("):
        assert method in js
    assert "window.PhaseTree = PhaseTree" in js
    assert "RULE_TYPES" not in js
