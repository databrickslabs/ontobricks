"""Contracts for Data Sources table-details persistence."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DOMAIN_METADATA_JS = REPO_ROOT / "src/front/static/domain/js/domain-metadata.js"


def _source() -> str:
    return DOMAIN_METADATA_JS.read_text(encoding="utf-8")


def test_save_table_details_persists_via_metadata_save():
    """Modal Save Changes must persist comments, not only update the cache."""
    source = _source()
    start = source.index("async function saveTableDetails")
    body = source[start : start + 1200]
    assert "await saveMetadataChanges" in body
    assert "/domain/metadata/save" in source
    # Must not leave users with a dead "click Save Changes to persist" path.
    assert 'Click "Save Changes" to persist' not in body


def test_update_mappings_automatically_persists_domain_to_registry():
    """Updating mappings must not leave the registry behind the session."""
    source = _source()
    start = source.index("async function updateMappingsFromMetadata")
    end = source.index("async function updateMetadataFromUC", start)
    body = source[start:end]

    update_call = body.index("/domain/metadata/update-mappings")
    registry_save = body.index("await doDomainSave()")
    assert update_call < registry_save
    assert "if (data.success)" in body[update_call:registry_save]
