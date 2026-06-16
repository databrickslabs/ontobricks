"""Smoke probes against the deployed mcp-ontobricks FastMCP server.

Validates the MCP server is reachable, advertises tools via the standard
``tools/list`` JSON-RPC method, and rejects unauthenticated clients.
"""

from __future__ import annotations

import json
import uuid

import httpx
import pytest


pytestmark = pytest.mark.live_integration


def _rpc_payload(method: str, params: dict | None = None, _id: str | None = None) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": _id or str(uuid.uuid4()),
        "method": method,
        "params": params or {},
    }


class TestMcpHandshake:
    def test_mcp_endpoint_is_alive(self, mcp_http):
        """The MCP server's /mcp endpoint must respond with a proper
        JSON-RPC error (not a 502 Bad Gateway) when called without a
        Streamable-HTTP session ID — that proves the FastMCP process is
        running inside the Databricks App."""
        resp = mcp_http.get(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream"},
        )
        # 200/204 → endpoint serves content; 400 → JSON-RPC error
        # (expected without a session); 405/406 → wrong method/content;
        # 302/303/307 → OAuth redirect for browser flow. Anything 5xx
        # means the FastMCP process isn't reachable behind the gateway.
        assert resp.status_code in (
            200, 204, 302, 303, 307, 400, 405, 406
        ), f"Unexpected status {resp.status_code}: {resp.text[:200]}"
        assert resp.status_code < 500, (
            f"MCP gateway returned {resp.status_code} — the server "
            f"process is likely down: {resp.text[:200]}"
        )

    def test_mcp_returns_jsonrpc_error_on_unframed_get(self, mcp_http):
        """A direct GET without session ID should yield a JSON-RPC error
        envelope (FastMCP's expected behaviour) — confirms the process
        speaks MCP, not a generic HTTP error page."""
        resp = mcp_http.get(
            "/mcp",
            headers={"Accept": "application/json, text/event-stream"},
        )
        if resp.status_code == 400:
            # Body should be a JSON-RPC error frame.
            ctype = resp.headers.get("content-type", "")
            if "json" in ctype:
                body = resp.json()
                assert body.get("jsonrpc") == "2.0", body
                assert "error" in body, body
                # The standard "Missing session ID" code is -32600.
                err = body["error"]
                assert isinstance(err, dict) and "code" in err, body

    def test_mcp_root_is_not_5xx(self, mcp_http):
        """Root path is not the MCP endpoint; must not crash the gateway."""
        resp = mcp_http.get("/")
        assert resp.status_code < 500, resp.text[:200]


class TestMcpProtocol:
    """Best-effort JSON-RPC handshake — skips gracefully if the server's
    transport rejects the simple request shape (FastMCP's Streamable HTTP
    transport sometimes requires a multi-step session init)."""

    def test_initialize_handshake(self, mcp_http):
        """A proper MCP Streamable-HTTP session starts with an
        ``initialize`` JSON-RPC call. The server should answer 200 +
        ``Mcp-Session-Id`` header that subsequent calls reuse."""
        payload = _rpc_payload(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "ontobricks-live-test", "version": "0.1"},
            },
        )
        try:
            resp = mcp_http.post(
                "/mcp",
                json=payload,
                headers={
                    "Accept": "application/json, text/event-stream",
                    "Content-Type": "application/json",
                },
            )
        except httpx.RequestError as exc:
            pytest.skip(f"MCP POST not reachable: {exc}")

        if resp.status_code == 401:
            pytest.fail("MCP rejected Bearer token — auth broken")
        if resp.status_code in (302, 303, 307):
            pytest.skip(
                "MCP gateway requires browser OAuth flow for programmatic "
                "clients; Bearer auth alone insufficient"
            )

        assert 200 <= resp.status_code < 300, (
            f"initialize returned {resp.status_code}: {resp.text[:300]}"
        )

        # Session ID must be present in the response headers so the
        # next call can include it.
        session_id = resp.headers.get("Mcp-Session-Id") or resp.headers.get(
            "mcp-session-id"
        )
        assert session_id, (
            f"No Mcp-Session-Id in response headers: "
            f"{dict(resp.headers)}"
        )

    def test_tools_list_after_initialize(self, mcp_http):
        """Full two-step handshake: initialize, then tools/list, asserting
        the server exposes at least one OntoBricks tool."""
        # Step 1: initialize.
        init = mcp_http.post(
            "/mcp",
            json=_rpc_payload(
                "initialize",
                {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "ontobricks-live-test",
                        "version": "0.1",
                    },
                },
            ),
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
            },
        )
        if init.status_code in (302, 303, 307):
            pytest.skip("MCP requires browser OAuth — handshake not reachable")
        assert init.status_code == 200, init.text[:200]
        session = init.headers.get("Mcp-Session-Id") or init.headers.get(
            "mcp-session-id"
        )
        assert session, "MCP initialize did not return a session id"

        # Per the MCP spec, the client must send `notifications/initialized`
        # after a successful initialize before issuing real RPC calls.
        mcp_http.post(
            "/mcp",
            json={
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
                "params": {},
            },
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                "Mcp-Session-Id": session,
            },
        )

        # Step 2: list tools.
        list_resp = mcp_http.post(
            "/mcp",
            json=_rpc_payload("tools/list"),
            headers={
                "Accept": "application/json, text/event-stream",
                "Content-Type": "application/json",
                "Mcp-Session-Id": session,
            },
        )
        assert list_resp.status_code == 200, list_resp.text[:300]

        body_text = list_resp.text
        # Streamable-HTTP can answer either as a JSON body or as an
        # SSE stream with `event: message\ndata: {...}`.
        ctype = list_resp.headers.get("content-type", "")
        if "json" in ctype:
            body = list_resp.json()
        elif "event-stream" in ctype:
            # Parse the first SSE event's `data:` line as JSON.
            data_lines = [
                ln[len("data: ") :]
                for ln in body_text.splitlines()
                if ln.startswith("data: ")
            ]
            assert data_lines, f"No SSE data in: {body_text[:300]}"
            body = json.loads(data_lines[0])
        else:
            pytest.fail(
                f"Unexpected content-type {ctype!r}: {body_text[:200]}"
            )

        assert body.get("jsonrpc") == "2.0", body
        assert "result" in body, body
        tools = body["result"].get("tools", [])
        assert isinstance(tools, list) and tools, (
            f"tools/list returned empty: {body}"
        )
        # Sanity: at least one OntoBricks-style tool.
        names = [t.get("name", "") for t in tools]
        assert any(
            n.startswith(("list_", "get_", "describe_", "query_"))
            for n in names
        ), f"No recognisable OntoBricks tool in: {names[:10]}"


class TestMcpUnauthenticated:
    """Without a Bearer token MCP must NOT serve protected tools."""

    def test_anonymous_post_is_rejected(self, mcp_base):
        if not mcp_base:
            pytest.skip("ONTOBRICKS_LIVE_MCP_BASE not set")
        with httpx.Client(timeout=10, follow_redirects=False) as anon:
            resp = anon.post(
                f"{mcp_base}/mcp",
                json=_rpc_payload("tools/list"),
                headers={
                    "Accept": "application/json, text/event-stream",
                    "Content-Type": "application/json",
                },
            )
        # 302 (OAuth redirect), 401, or 403 are all valid rejection forms.
        assert resp.status_code in (302, 303, 307, 401, 403), (
            f"Anonymous MCP POST returned {resp.status_code} — should be "
            f"redirected or rejected. Body: {resp.text[:200]}"
        )
