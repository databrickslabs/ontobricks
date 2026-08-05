"""Bridge /resolve redirect must bust the navbar domain-identity cache.

When a cross-domain bridge loads the target domain server-side, the
redirect to /dtwin/ must include ``domain_switched=1`` so the navbar
invalidates its 15 s sessionStorage ``/navbar/state`` cache before
painting the previous domain name/version.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from back.objects.domain import Domain


def _mock_domain(folder: str = "source_domain"):
    domain = MagicMock()
    domain.info = {"name": folder, "version": "1", "status": "DRAFT"}
    domain.domain_folder = folder
    domain.ontology = {"base_uri": f"http://{folder}.org#"}
    domain.save = MagicMock()
    return domain


class TestBuildResolveEntityRedirectUrl:
    def test_includes_domain_switched_flag_when_requested(self):
        op = Domain(_mock_domain())
        url = op._build_resolve_entity_redirect_url(
            "http://target.org#Entity/1",
            domain_switched=True,
        )
        assert "domain_switched=1" in url
        assert "section=sigmagraph" in url
        assert "focus=" in url
        assert "&domain=" not in url

    def test_omits_domain_switched_by_default(self):
        op = Domain(_mock_domain())
        url = op._build_resolve_entity_redirect_url("http://target.org#Entity/1")
        assert "domain_switched" not in url

    def test_client_fallback_keeps_domain_param_without_switched_flag(self):
        op = Domain(_mock_domain())
        url = op._build_resolve_entity_redirect_url(
            "http://target.org#Entity/1",
            bridge_domain="target_domain",
        )
        assert "domain=target_domain" in url
        assert "domain_switched" not in url


class TestResolveEntityUriRedirectNavbarBust:
    @pytest.mark.asyncio
    async def test_successful_switch_adds_domain_switched(self, monkeypatch):
        op = Domain(_mock_domain("source_domain"))
        monkeypatch.setattr(
            op,
            "_switch_domain_if_needed_for_resolve",
            AsyncMock(return_value=True),
        )
        url = await op.resolve_entity_uri_redirect(
            "http://target.org#Entity/1",
            domain_hint="target_domain",
        )
        assert "domain_switched=1" in url
        assert "&domain=" not in url

    @pytest.mark.asyncio
    async def test_failed_switch_falls_back_to_domain_param(self, monkeypatch):
        op = Domain(_mock_domain("source_domain"))
        monkeypatch.setattr(
            op,
            "_switch_domain_if_needed_for_resolve",
            AsyncMock(return_value=False),
        )
        url = await op.resolve_entity_uri_redirect(
            "http://target.org#Entity/1",
            domain_hint="target_domain",
        )
        assert "domain=target_domain" in url
        assert "domain_switched" not in url

    @pytest.mark.asyncio
    async def test_no_target_domain_omits_bust_flag(self, monkeypatch):
        op = Domain(_mock_domain("source_domain"))
        monkeypatch.setattr(
            op,
            "_bridge_domain_for_entity_uri",
            AsyncMock(return_value=None),
        )
        url = await op.resolve_entity_uri_redirect("http://unknown.org#Entity/1")
        assert "domain_switched" not in url
        assert "&domain=" not in url
