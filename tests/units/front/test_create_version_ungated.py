"""New Version must stay available regardless of ontology/mapping readiness.

Branching a domain version is a registry snapshot operation — it must not be
gated on ``ontology_valid`` / ``mapping_valid`` (Build / Submit-for-review
remain gated separately).
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from back.objects.domain import Domain

REPO_ROOT = Path(__file__).resolve().parents[3]
PERMISSIONS_CSS = REPO_ROOT / "src/front/static/global/css/permissions.css"
VERSIONS_HTML = (
    REPO_ROOT / "src/front/templates/partials/domain/_domain_versions.html"
)
ACTIONS_JS = REPO_ROOT / "src/front/static/domain/js/domain-actions.js"
VERSIONS_JS = REPO_ROOT / "src/front/static/domain/js/domain-versions.js"

pytestmark = pytest.mark.unit


def _gated(css: str, selector: str) -> bool:
    idx = 0
    while True:
        pos = css.find(selector, idx)
        if pos < 0:
            return False
        rule_start = css.rfind("body:is(.read-only-version", 0, pos)
        brace = css.find("{", pos)
        props = css[brace : css.find("}", brace) + 1] if brace >= 0 else ""
        if rule_start >= 0 and "pointer-events: none" in props:
            return True
        idx = pos + len(selector)


class TestNewVersionUiNotGatedOnReadiness:
    def test_button_present_and_not_pre_disabled(self):
        html = VERSIONS_HTML.read_text(encoding="utf-8")
        assert 'id="btnAddVersion"' in html
        # No static disabled attribute on the New Version control.
        btn = re.search(
            r"<button[^>]*id=\"btnAddVersion\"[^>]*>", html, re.DOTALL
        )
        assert btn, "btnAddVersion button markup missing"
        assert "disabled" not in btn.group(0)

    def test_not_in_read_only_css_gate(self):
        """Branching from a PUBLISHED tip must stay clickable in the UI."""
        assert not _gated(
            PERMISSIONS_CSS.read_text(encoding="utf-8"), "#btnAddVersion"
        )

    def test_domain_actions_keeps_new_version_enabled(self):
        js = ACTIONS_JS.read_text(encoding="utf-8")
        assert "btnAddVersion" in js
        # Must force-enable, never disable, the New Version button.
        assert "versionBtn.disabled = false" in js
        assert "versionBtn.disabled = true" not in js
        # Dead ID must not return — that used to disable create-version on
        # non-DRAFT and would regress if reintroduced against btnAddVersion.
        assert "btnCreateVersion" not in js

    def test_add_new_version_js_ignores_mapping_ontology_flags(self):
        js = VERSIONS_JS.read_text(encoding="utf-8")
        fn = js.split("async function addNewVersionFromList")[1].split(
            "async function "
        )[0]
        assert "mapping_valid" not in fn
        assert "ontology_valid" not in fn
        assert "/domain/create-version" in fn


class TestCreateVersionBackendIgnoresReadiness:
    def test_source_has_no_ontology_or_mapping_validity_gate(self):
        src = inspect.getsource(Domain.create_new_domain_version)
        assert "ontology_valid" not in src
        assert "mapping_valid" not in src
        assert "is_ontology_valid" not in src
        assert "mapping_complete" not in src

    def test_creates_version_with_incomplete_mappings(self, monkeypatch):
        from unittest.mock import MagicMock

        domain = MagicMock()
        domain.registry = {"catalog": "c", "schema": "s", "volume": "v"}
        domain.domain_folder = "demo"
        domain.uc_domain_folder = "demo"
        domain.is_active_version = True
        domain.current_version = "1"
        domain.info = {"status": "DRAFT"}
        domain.get_entity_mappings.return_value = []  # incomplete
        domain.get_relationship_mappings.return_value = []
        domain.get_classes.return_value = [{"name": "Person"}]  # ontology present
        domain.export_for_save.return_value = {
            "info": {},
            "versions": {"2": {"assignment": {"entities": [], "relationships": []}}},
        }
        domain.clear_generated_content = MagicMock()
        domain.save = MagicMock()

        svc = MagicMock()
        svc.cfg.catalog = "c"
        svc.cfg.schema = "s"
        svc.cfg.volume = "v"
        svc.write_version.return_value = (True, "")
        svc.copy_version_documents.return_value = (0, [])

        import importlib

        domain_module = importlib.import_module("back.objects.domain.Domain")
        monkeypatch.setattr(
            domain_module, "invalidate_registry_cache", lambda: None
        )

        result = Domain(domain).create_new_domain_version(svc)
        assert result["success"] is True
        assert result["new_version"] == "2"
        svc.write_version.assert_called_once()
