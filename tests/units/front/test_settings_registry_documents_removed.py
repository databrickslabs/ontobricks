"""Contract: Settings Registry section no longer includes the Volume/documents block."""

from pathlib import Path
import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_HTML = REPO_ROOT / "src/front/templates/partials/registry/_registry_configuration.html"
REGISTRY_CONFIG_JS = REPO_ROOT / "src/front/static/config/js/settings-registry-configuration.js"


def test_registry_configuration_has_no_volume_or_documents_section():
    html = REGISTRY_HTML.read_text(encoding="utf-8")

    # Volume/documents elements removed
    assert "registryVolumeLabel" not in html
    assert "registryVolumeCheckBadge" not in html
    assert "registryVolumeCheckDetail" not in html
    assert "binary context document uploads" not in html
    assert "documents/" not in html

    # Core registry UC schema and Lakebase connection remain
    assert "registrySchemaLabel" in html
    assert "registrySchemaCheckBadge" in html
    assert "registrySchemaCheckDetail" in html
    assert "Lakebase Connection" in html


def test_registry_configuration_js_does_not_require_volume_elements():
    js = REGISTRY_CONFIG_JS.read_text(encoding="utf-8")
    assert "registryVolumeCheckBadge" not in js
    assert "registryVolumeCheckDetail" not in js
