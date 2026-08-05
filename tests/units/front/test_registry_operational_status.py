"""Contract: Registry Browse shows a one-line operational status."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"
DOMAINS_HTML = REPO_ROOT / "src/front/templates/partials/registry/_registry_domains.html"


def test_registry_status_container_exists():
    html = DOMAINS_HTML.read_text(encoding="utf-8")
    assert 'id="registryStatus"' in html


def test_update_registry_status_shows_operational_line():
    js = REGISTRY_JS.read_text(encoding="utf-8")
    assert "function updateRegistryStatus" in js
    assert "Registry is operational" in js
    assert "Registry is not operational" in js
    # When healthy, the status line must stay visible (not display:none).
    assert "Registry is operational</p>" in js
    assert "if (div) div.style.display = 'none';" not in js.split("function updateRegistryStatus")[1].split("// --- Helpers ---")[0]
