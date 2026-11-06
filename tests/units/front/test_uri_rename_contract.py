"""Static contract: the editor adopts the URIs the backend re-derives on rename."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
CORE_JS = REPO_ROOT / "src/front/static/global/js/ontology-core.js"
SHARED_PANELS_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-shared-panels.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_successful_save_applies_stored_uris():
    js = _read(CORE_JS)

    assert "function applyStoredUris(result)" in js
    # Only a successful save carries the re-derived URIs.
    assert "applyStoredUris(result);" in js
    assert "result.uri_renames" in js
    assert "result.attribute_uris" in js


def test_attribute_uris_are_matched_by_position():
    js = _read(CORE_JS)

    # Attributes may share a name across classes, so they cannot be keyed by value.
    assert "uris.length !== attrs.length" in js


def test_shared_panels_no_longer_claim_the_uri_is_frozen():
    js = _read(SHARED_PANELS_JS)

    assert "Preserve server-assigned URI" not in js
    assert js.count("see applyStoredUris") == 2
