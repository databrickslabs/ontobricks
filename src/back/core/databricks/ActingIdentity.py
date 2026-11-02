"""MCP-forwarded end-user identity after the Apps proxy overwrite.

Databricks Apps is authoritative for ``x-forwarded-email`` /
``x-forwarded-access-token``. On the MCP → UI hop those headers are
stamped from the MCP service principal's M2M bearer, so the UI never
sees the end user if we trust only ``x-forwarded-*``.

The MCP server therefore sends the captured user on
:attr:`ActingIdentity.HEADER_EMAIL` / :attr:`ActingIdentity.HEADER_TOKEN`
(names the proxy does not overwrite). This class applies them **only**
when the proxy identity is the configured MCP app SP.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ActingIdentity:
    """Resolved caller after an optional MCP identity swap."""

    HEADER_EMAIL = "x-ontobricks-acting-email"
    HEADER_TOKEN = "x-ontobricks-acting-token"

    email: str
    user_token: str
    swapped: bool = False

    @classmethod
    def resolve(
        cls,
        *,
        proxy_email: str,
        proxy_token: str,
        acting_email: str,
        acting_token: str,
        mcp_sp_client_id: str,
    ) -> "ActingIdentity":
        """Return the end-user identity, swapping only for the MCP SP.

        Browser users keep the Apps proxy headers. A request whose proxy
        email is the MCP SP *and* that carries an acting email is treated
        as that user. Missing acting email, or a non-MCP caller, leaves
        the proxy identity unchanged (fail-closed / no spoof).
        """
        proxy = (proxy_email or "").strip()
        mcp_sp = (mcp_sp_client_id or "").strip()
        acting = (acting_email or "").strip()
        if mcp_sp and proxy.lower() == mcp_sp.lower() and acting:
            return cls(
                email=acting,
                user_token=(acting_token or "").strip(),
                swapped=True,
            )
        return cls(
            email=proxy,
            user_token=(proxy_token or "").strip(),
            swapped=False,
        )
