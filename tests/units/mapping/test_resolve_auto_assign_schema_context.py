"""Auto-Map needs Unity Catalog tables; a missing-metadata error says where to load them."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.errors import ValidationError
from back.objects.mapping.Mapping import Mapping

pytestmark = pytest.mark.unit


def _domain(*, tables=None):
    return SimpleNamespace(catalog_metadata={"tables": tables or []})


def test_uses_override_tables_when_present():
    domain = _domain(tables=[{"full_name": "c.s.ignored"}])
    out = Mapping(domain).resolve_auto_assign_schema_context(
        {"tables": [{"full_name": "a.b.t"}]}
    )
    assert out["tables"][0]["full_name"] == "a.b.t"


def test_uses_catalog_metadata_when_present():
    domain = _domain(tables=[{"full_name": "c.s.t"}])
    out = Mapping(domain).resolve_auto_assign_schema_context({})
    assert out["tables"][0]["full_name"] == "c.s.t"


def test_empty_tables_rejected_with_actionable_message():
    with pytest.raises(ValidationError, match="Context → Metadata"):
        Mapping(_domain(tables=[])).resolve_auto_assign_schema_context({})
