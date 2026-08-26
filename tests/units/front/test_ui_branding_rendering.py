"""Server-side UI branding rendering contract tests."""

from __future__ import annotations

from html import escape
from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit


def _custom_branding() -> dict:
    return {
        "version": 1,
        "app_title": "Acme Graph",
        "primary_color": "#123456",
        "logo_data_url": "data:image/png;base64,AAAA",
        "logo_url": "data:image/png;base64,AAAA",
        "is_custom_logo": True,
        "palette": {
            "primary_rgb": "18, 52, 86",
            "primary_dark": "#102D49",
            "primary_darker": "#0C243A",
            "primary_light": "rgba(18, 52, 86, 0.10)",
            "hover": "rgba(18, 52, 86, 0.06)",
            "focus": "rgba(18, 52, 86, 0.18)",
            "on_primary": "#FFFFFF",
            "selected_text": "#0C243A",
        },
    }


def test_configured_branding_is_in_first_html_response(client):
    with patch(
        "back.objects.session.GlobalConfigService.global_config_service.get_ui_branding",
        return_value=_custom_branding(),
    ):
        html = client.get("/", headers={"accept": "text/html"}).text

    assert "<title>Home - Acme Graph</title>" in html
    assert '--db-primary: #123456;' in html
    assert '--db-primary-rgb: 18, 52, 86;' in html
    assert '--db-on-primary: #FFFFFF;' in html
    assert '--db-primary-selected-text:' in html
    assert '--db-focus-ring:' in html
    assert '--db-shadow-primary:' in html
    assert 'href="data:image/png;base64,AAAA"' in html
    assert 'id="brandLogoImg" data-brand-icon src="data:image/png;base64,AAAA"' in html
    assert 'id="brandTitleText" data-brand-title' in html
    assert 'data-brand-icon src="data:image/png;base64,AAAA"' in html
    assert 'alt="Acme Graph"' in html
    assert ">Acme Graph<" in html
    assert '<span data-brand-title>Acme Graph</span> Help Center' in html
    assert "Welcome to <span data-brand-title>Acme Graph</span>" in html
    assert html.index("global/css/permissions.css") < html.index('id="uiBrandingTokens"')
    assert html.index('id="uiBrandingTokens"') < html.index("home/css/home.css")


def test_branding_resolution_failure_falls_back_to_defaults(client):
    with patch(
        "back.objects.session.GlobalConfigService.global_config_service.get_ui_branding",
        side_effect=RuntimeError("boom"),
    ):
        html = client.get("/", headers={"accept": "text/html"}).text

    assert "<title>Home - OntoBricks</title>" in html
    assert '--db-primary: #4F46E5;' in html
    assert 'href="/static/global/img/favicon.svg"' in html


def test_graphql_playground_uses_resolved_branding(client):
    fake_domain = SimpleNamespace(info={"name": "Retail"}, ontology={"classes": [1]})
    with (
        patch("back.fastapi.graphql_routes._load_domain_from_registry", return_value=fake_domain),
        patch("back.fastapi.graphql_routes._get_schema_and_context", return_value=(object(), {})),
        patch(
            "back.objects.session.GlobalConfigService.global_config_service.get_ui_branding",
            return_value=_custom_branding(),
        ),
    ):
        html = client.get("/graphql/retail", headers={"accept": "text/html"}).text

    assert "<title>Acme Graph GraphQL - Retail</title>" in html
    assert 'href="data:image/png;base64,AAAA"' in html


def test_graphiql_escapes_title_domain_and_logo_url(client):
    malicious_title = 'Acme"><script>alert(1)</script>'
    malicious_logo = 'data:image/svg+xml,<svg onload="alert(1)"></svg>'
    malicious_domain = 'Retail</title><script>alert(2)</script>'
    fake_domain = SimpleNamespace(info={"name": malicious_domain}, ontology={"classes": [1]})

    with (
        patch("back.fastapi.graphql_routes._load_domain_from_registry", return_value=fake_domain),
        patch("back.fastapi.graphql_routes._get_schema_and_context", return_value=(object(), {})),
        patch(
            "back.objects.session.GlobalConfigService.global_config_service.get_ui_branding",
            return_value={
                **_custom_branding(),
                "app_title": malicious_title,
                "logo_data_url": malicious_logo,
                "logo_url": malicious_logo,
            },
        ),
    ):
        html = client.get("/graphql/retail", headers={"accept": "text/html"}).text

    unsafe_title = f"{malicious_title} GraphQL - {malicious_domain}"
    assert unsafe_title not in html
    assert "<script>alert(1)</script>" not in html
    assert "<script>alert(2)</script>" not in html
    assert escape(unsafe_title, quote=True) in html
    assert f'href="{escape(malicious_logo, quote=True)}"' in html
