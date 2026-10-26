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
        "_props",
    ):
        assert f"'{suffix}'" in block


def test_lakebase_domain_grouping_keeps_unrecognized_names_unchanged():
    assert "return name;" in _object_base_block()


def test_lakebase_group_count_and_delete_registry_use_normalized_base():
    source = SETTINGS_JS.read_text(encoding="utf-8")
    assert "const base = objectBase(o.name, o.kind);" in source
    assert "_lkDomainRegistry[base].items.push(o);" in source
    assert "+ grp.items.length +" in source
    assert "grp.sortedItems = sorted;" in source
