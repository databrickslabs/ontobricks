from types import SimpleNamespace
from unittest.mock import patch

import pytest

from back.core.errors import ValidationError
from back.core.helpers.DatabricksHelpers import DatabricksHelpers

_NO_LLM_MSG = "No LLM selected. Select one in Domain → General → AI."
_DEFAULT_INFO = object()


def _domain(endpoint="", kind="", info=_DEFAULT_INFO):
    if info is _DEFAULT_INFO:
        info = {"llm_endpoint": endpoint, "llm_endpoint_kind": kind}
    return SimpleNamespace(info=info)


@patch.object(
    DatabricksHelpers,
    "get_databricks_host_and_token",
    return_value=("https://workspace", "token"),
)
def test_require_domain_llm_returns_normalized_target(_credentials):
    assert DatabricksHelpers.require_domain_llm(
        _domain("main.ai.model", ""), SimpleNamespace()
    ) == ("https://workspace", "token", "main.ai.model", "ai_gateway")


@patch.object(
    DatabricksHelpers,
    "get_databricks_host_and_token",
    return_value=("https://workspace", "token"),
)
def test_require_domain_llm_rejects_empty_endpoint(_credentials):
    with pytest.raises(ValidationError, match=_NO_LLM_MSG):
        DatabricksHelpers.require_domain_llm(_domain(), SimpleNamespace())


@patch.object(
    DatabricksHelpers,
    "get_databricks_host_and_token",
    return_value=("", ""),
)
def test_require_domain_llm_rejects_empty_endpoint_before_credentials(_credentials):
    with pytest.raises(ValidationError, match=_NO_LLM_MSG):
        DatabricksHelpers.require_domain_llm(_domain(), SimpleNamespace())


@patch.object(
    DatabricksHelpers,
    "get_databricks_host_and_token",
    return_value=("https://workspace", "token"),
)
def test_require_domain_llm_rejects_whitespace_endpoint(_credentials):
    with pytest.raises(ValidationError, match=_NO_LLM_MSG):
        DatabricksHelpers.require_domain_llm(_domain("   "), SimpleNamespace())


def test_require_domain_llm_handles_none_domain_info():
    with pytest.raises(ValidationError, match=_NO_LLM_MSG):
        DatabricksHelpers.require_domain_llm(_domain(info=None), SimpleNamespace())
