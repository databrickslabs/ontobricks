"""Contract: SWRL attribute conditions offer every selected IF entity.

Selecting N entities as IF must propose the same N entities as condition
subjects. Attribute resolution walks the parent chain (inherited attributes
count) and the editor re-seeds its class/property source from the graph config
so a stale ``init()`` snapshot cannot hide an entity.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SWRL_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-swrl.js"


def _source() -> str:
    return SWRL_JS.read_text(encoding="utf-8")


def _method_body(name: str) -> str:
    """Return the body of the ``[async ]name(...) { ... }`` object method."""
    source = _source()
    match = re.search(rf"\n    (?:async\s+)?{re.escape(name)}\(", source)
    assert match, f"method {name} not found in ontology-swrl.js"
    start = match.start()
    i = source.index("{", start) + 1
    depth = 1
    body_start = i
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    assert depth == 0, f"unbalanced braces in {name}"
    return source[body_start : i - 1]


def test_condition_subjects_are_not_filtered_by_attributes():
    body = _method_body("_renderConditions")
    assert "const entIds = [...this.ifNodes];" in body, (
        "every IF entity must be offered as a condition subject"
    )
    assert "_dataPropsForClass(id).length" not in body, (
        "_renderConditions must not filter IF entities on attribute presence"
    )


def test_new_condition_row_falls_back_to_any_if_entity():
    body = _method_body("addConditionRow")
    assert "ifIds[0]" in body, (
        "subject default must fall back to any IF entity, not stay empty"
    )


def test_attribute_lookup_walks_parent_chain():
    chain = _method_body("_classChain")
    assert "cls.parent" in chain or "cls ? cls.parent" in chain, (
        "_classChain must follow the parent link"
    )
    assert "seen" in chain, "_classChain must guard against inheritance cycles"

    props = _method_body("_dataPropsForClass")
    assert "_classChain(nodeId)" in props, (
        "_dataPropsForClass must include inherited attributes via _classChain"
    )


def test_editor_reseeds_raw_classes_from_graph_config():
    body = _method_body("_openEditor")
    assert "this._rawClasses = config.classes" in body, (
        "editor must re-seed _rawClasses from the graph config"
    )
    assert "this._rawProperties = config.properties" in body, (
        "editor must re-seed _rawProperties from the graph config"
    )
