"""Contract: Context open/create/validate keeps a loading overlay until chrome paints."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[3]
BASE = REPO / "src/front/templates/base.html"
NAVBAR = REPO / "src/front/static/global/js/navbar.js"
REGISTRY = REPO / "src/front/static/registry/js/registry.js"
HOME_TASKS = REPO / "src/front/static/home/js/home-tasks.js"
VERSIONS = REPO / "src/front/static/domain/js/domain-versions.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestDomainLoadingOverlayPersistence:
    def test_base_has_overlay_and_early_restore(self):
        html = _read(BASE)
        assert 'id="domainLoadingOverlay"' in html
        assert "ob_domain_loading" in html
        assert "sessionStorage.getItem('ob_domain_loading')" in html
        # Inline restore sits right after the overlay markup; navbar.js is
        # deferred in <head>, so the restore paints before initNavbar runs.
        assert html.index('id="domainLoadingOverlay"') < html.index(
            "sessionStorage.getItem('ob_domain_loading')"
        )
        assert 'navbar.js' in html and "defer" in html

    def test_show_persists_and_hide_clears(self):
        js = _read(NAVBAR)
        assert "DOMAIN_LOADING_KEY = 'ob_domain_loading'" in js
        show = js.split("function showDomainLoading")[1].split("\nfunction ")[0]
        hide = js.split("function hideDomainLoading")[1].split("\nfunction ")[0]
        assert "sessionStorage.setItem" in show
        assert "DOMAIN_LOADING_KEY" in show
        assert "sessionStorage.removeItem(DOMAIN_LOADING_KEY)" in hide

    def test_overlay_not_hidden_by_restore_before_section_switch(self):
        """The remembered payload paints chrome during init, but the overlay
        must stay up: hiding it there exposes the default Home section before
        domain.js switches to the requested section."""
        js = _read(NAVBAR)
        apply_fn = js.split("function applyDomainInfo")[1].split("\nfunction ")[0]
        assert "_readPendingDomainLoading()" not in apply_fn
        assert "hideDomainLoading()" not in apply_fn

    def test_overlay_settled_after_navbar_state_settles(self):
        js = _read(NAVBAR)
        fn = js.split("async function loadNavbarState")[1].split("\n}\n")[0]
        assert "finally" in fn
        assert "_settlePendingDomainLoading()" in fn
        helper = js.split("function _settlePendingDomainLoading")[1].split("\n}\n")[0]
        assert "hideDomainLoading()" in helper

    def test_domain_new_shows_spinner_before_save(self):
        js = _read(NAVBAR)
        fn = js.split("async function domainNew")[1].split("\nasync function ")[0]
        assert "showDomainLoading('Creating new context…" in fn
        assert fn.index("showDomainLoading(") < fn.index("doDomainSave")

    def test_do_domain_load_keeps_spinner_across_navigate(self):
        js = _read(NAVBAR)
        fn = js.split("async function doDomainLoad")[1].split("\nfunction ")[0]
        assert "showDomainLoading(`Loading ${domainSlug}...`)" in fn
        # Must not await loadNavbarState before navigate (that would clear the overlay).
        success = fn.split("if (data.success)")[1].split("} else {")[0]
        assert "await loadNavbarState()" not in success
        assert "rememberLoadedDomainOrClear(data)" in success
        assert "navigateToDomainGeneral()" in success
        assert "setTimeout" not in success

    def test_registry_open_paths_show_spinner(self):
        js = _read(REGISTRY)
        load_fn = js.split("async function loadRegistryDomainVersion")[1].split(
            "\n    async function "
        )[0]
        assert "showDomainLoading(" in load_fn
        assert "hideDomainLoading()" in load_fn
        open_fn = js.split("async function openVersionInValidation")[1].split(
            "\n    document.getElementById"
        )[0]
        assert "showDomainLoading(" in open_fn
        assert "hideDomainLoading()" in open_fn

    def test_home_validate_shows_spinner(self):
        js = _read(HOME_TASKS)
        fn = js.split("async function loadDomainAndReview")[1].split("\n    function ")[0]
        assert "showDomainLoading(" in fn
        assert "hideDomainLoading()" in fn

    def test_versions_load_shows_spinner(self):
        js = _read(VERSIONS)
        load_fn = js.split("async function loadVersionFromList")[1].split(
            "\nasync function "
        )[0]
        assert "showDomainLoading(" in load_fn
        reload_fn = js.split("async function reloadLastSavedVersion")[1].split(
            "\ndocument.addEventListener"
        )[0]
        assert "showDomainLoading(" in reload_fn
