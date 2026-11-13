"""URI helpers for pattern materialisation."""

import pytest

from back.core.graph_analysis.PatternVocabulary import PatternVocabulary as V

pytestmark = pytest.mark.unit

BASE = "http://example.org/onto"


def test_pattern_uri_roundtrip():
    assert V.pattern_class(BASE) == f"{BASE}/Pattern"
    assert V.in_pattern(BASE) == f"{BASE}/inPattern"
    assert V.from_discovery(BASE) == f"{BASE}/fromDiscovery"
    assert V.pattern_size(BASE) == f"{BASE}/patternSize"
    uri = V.pattern(BASE, "fr-buyers-ab12cd34")
    assert uri == f"{BASE}/pattern/fr-buyers-ab12cd34"
    assert V.parse_saved_id(uri) == "fr-buyers-ab12cd34"
    assert V.parse_saved_id("not-a-pattern") == ""


def test_join_respects_hash_separator():
    hashed = "http://example.org/onto#"
    assert V.pattern_class(hashed) == "http://example.org/onto#Pattern"
    uri = V.pattern(hashed, "fr-buyers-ab12cd34")
    assert uri == "http://example.org/onto#pattern/fr-buyers-ab12cd34"
    assert V.parse_saved_id(uri) == "fr-buyers-ab12cd34"


def test_invalid_saved_id_rejected():
    with pytest.raises(ValueError):
        V.pattern(BASE, "bad id!")
