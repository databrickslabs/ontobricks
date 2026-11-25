"""Contract: Settings → Registry offers a first-run setup form when no location is set."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_HTML = REPO_ROOT / "src/front/templates/partials/registry/_registry_configuration.html"
REGISTRY_CONFIG_JS = REPO_ROOT / "src/front/static/config/js/settings-registry-configuration.js"
REGISTRY_JS = REPO_ROOT / "src/front/static/registry/js/registry.js"


def _form_tag() -> str:
    match = re.search(r'<form id="registrySetupForm"[^>]*>', REGISTRY_HTML.read_text(encoding="utf-8"))
    assert match, "registrySetupForm missing"
    return match.group(0)


def test_setup_form_is_hidden_until_the_script_shows_it():
    assert "d-none" in _form_tag()


def test_setup_form_is_not_admin_gated_by_css():
    # [data-requires-app] uses `display: revert !important`, which would
    # override d-none and show the form after setup; /settings is admin-only.
    assert "data-requires-app" not in _form_tag()


def test_setup_form_has_labelled_catalog_and_schema_inputs():
    html = REGISTRY_HTML.read_text(encoding="utf-8")
    for field in ("registrySetupCatalog", "registrySetupSchema"):
        assert f'for="{field}"' in html and f'id="{field}"' in html


def test_setup_form_posts_to_the_setup_route():
    assert "'/settings/registry/setup'" in REGISTRY_CONFIG_JS.read_text(encoding="utf-8")


def test_setup_form_visibility_follows_the_registry_location():
    js = REGISTRY_JS.read_text(encoding="utf-8")
    assert "getElementById('registrySetupForm')" in js
    assert "window.reloadRegistryConfig = loadRegistryConfig" in js


ACCESS_DENIED_HTML = REPO_ROOT / "src/front/templates/access_denied.html"


def test_setup_form_offers_a_read_only_prerequisite_check():
    html = REGISTRY_HTML.read_text(encoding="utf-8")
    assert 'id="btnRegistrySetupCheck"' in html and 'type="button"' in html


def test_check_results_group_both_identities():
    html = REGISTRY_HTML.read_text(encoding="utf-8")
    assert 'id="registrySetupChecksYou"' in html and 'id="registrySetupChecksApp"' in html


def test_check_results_are_announced_to_screen_readers():
    assert re.search(r'id="registrySetupChecks"[^>]*aria-live="polite"', REGISTRY_HTML.read_text(encoding="utf-8"))


def test_setup_form_has_troubleshooting_help():
    assert 'id="registrySetupHelp"' in REGISTRY_HTML.read_text(encoding="utf-8")


def test_checks_run_as_the_admin_and_as_the_service_principal():
    js = REGISTRY_CONFIG_JS.read_text(encoding="utf-8")
    assert "'/settings/registry/setup/check'" in js and "'/settings/diagnostics/readiness'" in js


def test_failed_setup_shows_the_checks_automatically():
    js = REGISTRY_CONFIG_JS.read_text(encoding="utf-8")
    failure = js[js.index("Registry setup failed: ' + (data.message"):]
    assert "_runSetupChecks(form)" in failure[:400]


def test_access_denied_bootstrap_page_covers_marketplace_installs():
    html = ACCESS_DENIED_HTML.read_text(encoding="utf-8")
    assert "Set up the registry" in html and "scripts/bootstrap/app-permissions.sh" in html
