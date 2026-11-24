"""Contracts for grouping Lakebase physical objects by domain/version."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SETTINGS_JS = Path("src/front/static/config/js/settings.js")


def _object_base_block() -> str:
    source = SETTINGS_JS.read_text(encoding="utf-8")
    start = source.index("function objectBase(name, kind)")
    end = source.index("\n            function kindBadge", start)
    return source[start:end]


def test_lakebase_domain_grouping_recognizes_all_owned_table_suffixes():
    block = _object_base_block()
    for suffix in (
        "_sync",
        "__app",
        "_adj_in",
        "_adj_out",
        "_entity_search",
        "_entity_search_asserted",
        "_props",
    ):
        assert f"'{suffix}'" in block


def test_lakebase_domain_grouping_entity_search_asserted_precedes_entity_search():
    """_entity_search_asserted must appear before _entity_search to prevent partial match."""
    block = _object_base_block()
    pos_asserted = block.index("'_entity_search_asserted'")
    pos_union = block.index("'_entity_search'")
    assert pos_asserted < pos_union


def test_lakebase_domain_grouping_keeps_unrecognized_names_unchanged():
    assert "return name;" in _object_base_block()


def test_lakebase_domain_grouping_folds_simulation_objects_into_graph_base():
    source = SETTINGS_JS.read_text(encoding="utf-8")
    assert "__(sim|smu|sms|smp|smb|smo)_" in source
    assert "lk-sim-section" in source
    assert "lk-sim-group-row" in source
    assert "lk-drop-sim-btn" in source
    assert "dt-drop-sim-btn" in source
    assert "function dropSimulationObjects" in source
    assert "function dropDeltaSimulationObjects" in source
    assert "Simulations" in source


def test_simulation_suffix_pattern_matches_all_companion_markers():
    import re

    source = SETTINGS_JS.read_text(encoding="utf-8")
    start = source.index("/__(sim")
    end = source.index("/i", start) + 2
    raw_pattern = source[start + 1 : end - 2]
    pattern = re.compile(raw_pattern, re.I)

    for marker in ("sim", "smu", "sms", "smp", "smb", "smo"):
        name = f"dom_v1__{marker}_s123"
        m = pattern.search(name)
        assert m is not None, f"Expected {marker} to match simulation suffix regex"
        assert m.group(1).lower() == marker
        assert m.group(2).lower() == "s123"


def test_lakebase_group_count_and_delete_registry_use_normalized_base():
    source = SETTINGS_JS.read_text(encoding="utf-8")
    assert "const base = objectBase(o.name, o.kind);" in source
    assert "_lkDomainRegistry[base].items.push(o);" in source
    assert "+ grp.items.length +" in source
    assert "grp.sortedItems = sorted;" in source
