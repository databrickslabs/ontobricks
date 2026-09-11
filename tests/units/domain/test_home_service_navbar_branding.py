"""HomeService navbar branding context tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from back.objects.domain import HomeService

pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_navbar_branding_resolves_app_level_context():
    domain = MagicMock()
    settings = MagicMock()
    app_ctx = (
        "app-host",
        "app-token",
        {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"},
    )

    with patch(
        "back.core.helpers.resolve_app_registry_context",
        return_value=app_ctx,
    ), patch(
        "back.objects.session.global_config_service.get_ui_branding",
        return_value={
            "logo_url": "data:image/png;base64,AAAA",
            "is_custom_logo": True,
            "app_title": "Acme Graph",
        },
    ) as get_branding, patch(
        "back.objects.domain.Domain.Domain.get_domain_info",
        return_value={"name": "Retail"},
    ):
        payload = await HomeService.get_navbar_state(domain, settings, warehouse_id="wh-1")

    assert payload["branding"]["logo_url"] == "data:image/png;base64,AAAA"
    assert payload["branding"]["app_title"] == "Acme Graph"
    assert payload["branding"]["is_custom"] is True
    get_branding.assert_called_once_with(
        "app-host",
        "app-token",
        {"catalog": "app-cat", "schema": "app-sch", "volume": "app-vol"},
    )
