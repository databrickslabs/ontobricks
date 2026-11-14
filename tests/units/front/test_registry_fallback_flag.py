"""SQLite registry mode is flagged in the UI as a fallback, never shown as a nominal mode."""

from __future__ import annotations

from pathlib import Path

import pytest
from jinja2 import Environment

pytestmark = pytest.mark.unit

TEMPLATES = Path(__file__).resolve().parents[3] / "src" / "front" / "templates"


def _render(rel: str, **ctx) -> str:
    return Environment(autoescape=True).from_string((TEMPLATES / rel).read_text()).render(**ctx)


def _navbar_fallback_block() -> str:
    text = (TEMPLATES / "base.html").read_text()
    start = text.index("{% if registry_fallback() %}")
    end = text.index("{% endif %}", start) + len("{% endif %}")
    return text[start:end]


class TestNavbarBadge:
    def test_badge_shows_on_sqlite(self):
        html = Environment(autoescape=True).from_string(_navbar_fallback_block()).render(registry_fallback=lambda: True)
        assert 'id="registryFallbackBadge"' in html and "SQLite fallback" in html

    def test_no_badge_on_lakebase(self):
        html = Environment(autoescape=True).from_string(_navbar_fallback_block()).render(registry_fallback=lambda: False)
        assert "registryFallbackBadge" not in html


class TestSettingsAlert:
    def test_alert_explains_the_fallback(self):
        html = _render("partials/registry/_registry_configuration.html", registry_backend="sqlite", url_for=lambda *a, **k: "")
        assert 'id="registryFallbackAlert"' in html
        assert "not the target architecture" in html

    def test_no_alert_on_lakebase(self):
        html = _render("partials/registry/_registry_configuration.html", registry_backend="lakebase", url_for=lambda *a, **k: "")
        assert "registryFallbackAlert" not in html


class TestHealthWording:
    def test_sqlite_check_says_fallback(self, tmp_path, monkeypatch):
        from back.objects.registry.RegistryCfg import RegistryCfg
        from shared.config.settings import get_settings
        from shared.fastapi import health

        monkeypatch.setattr(get_settings(), "registry_backend", "sqlite")
        monkeypatch.setenv("REGISTRY_SQLITE_PATH", str(tmp_path / "flag.sqlite"))
        monkeypatch.setenv("REGISTRY_SQLITE_SNAPSHOT_PATH", "")
        monkeypatch.setattr(health, "_resolve_registry_cfg", lambda s: RegistryCfg(catalog="", schema="", volume=""))
        _status, detail = health._check_lakebase(get_settings())
        assert "fallback" in detail.lower()


class TestNewDomainDialog:
    def test_body_exposes_the_fallback_flag(self):
        text = (TEMPLATES / "base.html").read_text()
        assert "data-registry-fallback=" in text

    def test_dialog_marks_lakebase_unavailable_in_fallback(self):
        js = (TEMPLATES.parent / "static" / "global" / "js" / "utils.js").read_text()
        assert "registryFallback === '1'" in js and "not available on this workspace" in js


class TestLakebaseCardStaysDisabled:
    """The page script re-enables editable inputs; an unavailable Lakebase radio must stay disabled."""

    def test_unavailable_radio_is_marked(self):
        tpl = (TEMPLATES / "partials" / "domain" / "_domain_information.html").read_text()
        assert "{% if not lakebase_available %}disabled data-unavailable=\"1\"{% endif %}" in tpl

    def test_script_skips_unavailable_inputs(self):
        js = (TEMPLATES.parent / "static" / "domain" / "js" / "domain-information.js").read_text()
        assert ".domain-editable:not(#domainName):not([data-unavailable])" in js

    def test_card_editability_sync_keeps_unavailable_disabled(self):
        js = (TEMPLATES.parent / "static" / "domain" / "js" / "domain-information.js").read_text()
        assert "option.disabled = readOnly || backendSelect.disabled || option.dataset.unavailable === '1';" in js
