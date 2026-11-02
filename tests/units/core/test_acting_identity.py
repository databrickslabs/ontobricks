"""MCP acting-identity swap: only the MCP SP may forward an end user."""

from back.core.databricks.ActingIdentity import ActingIdentity


def test_swaps_when_proxy_is_mcp_sp():
    ident = ActingIdentity.resolve(
        proxy_email="mcp-sp-id",
        proxy_token="m2m",
        acting_email="user@x",
        acting_token="UT",
        mcp_sp_client_id="mcp-sp-id",
    )
    assert ident.swapped is True
    assert ident.email == "user@x"
    assert ident.user_token == "UT"


def test_match_is_case_insensitive():
    ident = ActingIdentity.resolve(
        proxy_email="MCP-SP-ID",
        proxy_token="",
        acting_email="user@x",
        acting_token="",
        mcp_sp_client_id="mcp-sp-id",
    )
    assert ident.swapped is True
    assert ident.email == "user@x"


def test_ignores_acting_headers_from_a_browser_user():
    ident = ActingIdentity.resolve(
        proxy_email="alice@x",
        proxy_token="alice-tok",
        acting_email="eve@x",
        acting_token="stolen",
        mcp_sp_client_id="mcp-sp-id",
    )
    assert ident.swapped is False
    assert ident.email == "alice@x"
    assert ident.user_token == "alice-tok"


def test_mcp_sp_without_acting_email_stays_fail_closed():
    ident = ActingIdentity.resolve(
        proxy_email="mcp-sp-id",
        proxy_token="m2m",
        acting_email="",
        acting_token="UT",
        mcp_sp_client_id="mcp-sp-id",
    )
    assert ident.swapped is False
    assert ident.email == "mcp-sp-id"
    assert ident.user_token == "m2m"


def test_empty_mcp_sp_config_never_swaps():
    ident = ActingIdentity.resolve(
        proxy_email="mcp-sp-id",
        proxy_token="m2m",
        acting_email="user@x",
        acting_token="UT",
        mcp_sp_client_id="",
    )
    assert ident.swapped is False
    assert ident.email == "mcp-sp-id"
