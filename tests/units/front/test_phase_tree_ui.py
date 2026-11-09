"""Contract for the inference phase tree shared by Graph → Inference and Simulation → Reasoning."""

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


def _render(**overrides) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)))
    ctx = {
        "tree_id": "tree",
        "tree_ctx": _CTX,
        "tree_ids": _IDS,
        "tree_checked": ["tbox", "if_then", "graph"],
        "tree_lock_tbox": True,
        **overrides,
    }
    return env.get_template("partials/layout/_phase_tree.html").render(**ctx)


def test_tree_lists_phases_in_execution_order_with_icons():
    html = _render()
    order = [html.index(f'data-tree-node="{k}"') for k in ("tbox", "if_then", "graph", "sparql_rules")]
    assert order == sorted(order)
    for icon in ("bi-diagram-3", "bi-code-square", "bi-share", "bi-braces"):
        assert icon in html
    assert 'role="tree"' in html
    assert "onclick=" not in html and "onchange=" not in html


def test_rule_phases_are_expandable_and_badged():
    html = _render()
    assert 'data-tree-toggle="if_then"' in html
    assert 'data-tree-children="sparql_rules"' in html
    assert 'data-tree-toggle="graph"' not in html
    assert "3 rules" in html
    assert "no rules" in html
    assert "1 object prop<" in html


def test_tbox_locks_without_owlrl_only_when_asked():
    locked = _render()
    assert re.search(r'id="pT" data-phase="tbox"\s+disabled data-tree-locked', locked)
    assert not re.search(r'id="pT"[^>]*checked', locked)
    unlocked = _render(tree_lock_tbox=False)
    assert "data-tree-locked" not in unlocked


def test_both_screens_include_the_tree_and_load_phase_tree_js():
    graph = _read("templates/partials/dtwin/_query_reasoning.html")
    sim = _read("templates/partials/simulation/_sim_workbench.html")
    for tpl, tree_id in ((graph, "reasoningPhaseTree"), (sim, "simWbPhases")):
        assert "partials/layout/_phase_tree.html" in tpl
        assert f'tree_id="{tree_id}"' in tpl
    for page, module in (("templates/dtwin.html", "query/js/query-reasoning.js"),
                         ("templates/simulation.html", "simulation/js/simulation-rules.js")):
        src = _read(page)
        assert src.index("global/js/phase-tree.js") < src.index(module)
    assert "reasoning-tile" not in graph
    assert "toggleAllOptions" not in graph


def test_reasoning_module_delegates_rule_selection_to_tree():
    js = _read("static/query/js/query-reasoning.js")
    assert "new PhaseTree(" in js
    assert "ruleNames(phases)" in js
    assert "if_then_rule_names" in js and "sparql_rule_names" in js
    for gone in ("_RULE_TILE_MAP", "_toggleRuleList", "toggleAllOptions", "_syncTile"):
        assert gone not in js


def test_phase_tree_exposes_shared_api():
    js = _read("static/global/js/phase-tree.js")
    for method in ("renderRules(", "phases()", "ruleNames(", "applyRules(", "setAll(", "setBusy(", "expand("):
        assert method in js
    assert "window.PhaseTree = PhaseTree" in js
