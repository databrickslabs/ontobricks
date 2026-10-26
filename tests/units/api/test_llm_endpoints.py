"""Tests for the combined Gateway and legacy LLM target catalog."""

from unittest.mock import MagicMock, patch

import pytest

from api.routers.internal.mapping import get_llm_endpoints
from back.core.sqlwizard import SQLWizardService


@pytest.mark.asyncio
async def test_llm_endpoints_puts_gateway_before_serving():
    client = MagicMock()
    client.get_ai_gateway_model_services.return_value = [
        {"name": "main.ai.mine", "kind": "ai_gateway", "comment": ""}
    ]
    with (
        patch(
            "api.routers.internal.mapping.get_databricks_client",
            return_value=client,
        ),
        patch.object(
            SQLWizardService,
            "get_model_serving_endpoints",
            return_value=[{"name": "legacy", "state": "READY"}],
        ),
    ):
        payload = await get_llm_endpoints(MagicMock(), MagicMock())

    assert [row["kind"] for row in payload["endpoints"]] == [
        "ai_gateway",
        "serving",
    ]


@pytest.mark.asyncio
async def test_gateway_listing_failure_keeps_serving():
    client = MagicMock()
    client.get_ai_gateway_model_services.side_effect = RuntimeError(
        "gateway unavailable"
    )
    with (
        patch(
            "api.routers.internal.mapping.get_databricks_client",
            return_value=client,
        ),
        patch.object(
            SQLWizardService,
            "get_model_serving_endpoints",
            return_value=[{"name": "legacy", "state": "READY"}],
        ),
    ):
        payload = await get_llm_endpoints(MagicMock(), MagicMock())

    assert payload["endpoints"] == [
        {"name": "legacy", "state": "READY", "kind": "serving"}
    ]
