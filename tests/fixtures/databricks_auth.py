"""Shared Databricks CLI token-minting helper for the test suites.

Both ``tests/e2e/conftest.py`` and ``tests/live_integration/conftest.py``
need a workspace OAuth token minted from a Databricks CLI profile. This
module is the single home for the ``databricks auth token`` invocation so
the two suites cannot drift.

The CLI honours ``DATABRICKS_CONFIG_PROFILE`` when no explicit profile is
passed, so callers that want the env-driven profile (live_integration
semantics) simply call ``DatabricksAuth.mint_token()`` with no argument.
"""

from __future__ import annotations

import json
import subprocess
from typing import Optional


class DatabricksAuth:
    """Mint workspace OAuth tokens from a Databricks CLI profile.

    All methods are best-effort: any failure (CLI missing, profile absent,
    token endpoint error, malformed JSON) returns ``None`` rather than
    raising, so each caller decides whether to ``pytest.skip`` or fall back.
    """

    @staticmethod
    def mint_token(profile: Optional[str] = None) -> Optional[str]:
        """Return a workspace ``access_token`` or ``None`` on any failure.

        When ``profile`` is ``None`` the CLI uses ``DATABRICKS_CONFIG_PROFILE``
        (or the default profile) — this is the live_integration behaviour.
        """
        cmd = ["databricks", "auth", "token"]
        if profile:
            cmd += ["--profile", profile]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30, check=True
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            return None
        try:
            token = json.loads(result.stdout).get("access_token")
        except json.JSONDecodeError:
            return None
        return token or None

    @staticmethod
    def mint_host_and_token(
        profile: str, host_fallback: str = ""
    ) -> Optional[tuple[str, str]]:
        """Return ``(host, access_token)`` or ``None`` if token minting fails.

        The host is resolved from ``databricks auth describe``; if that fails
        the ``host_fallback`` is used (so the e2e local-subprocess path always
        gets a usable host). Used by the e2e suite, which needs the host to
        point a local uvicorn at the right workspace.
        """
        token = DatabricksAuth.mint_token(profile)
        if not token:
            return None
        host = ""
        try:
            desc = subprocess.run(
                ["databricks", "auth", "describe", "--profile", profile, "-o", "json"],
                capture_output=True, text=True, timeout=15, check=True,
            )
            host = json.loads(desc.stdout).get("details", {}).get("host", "")
        except (subprocess.SubprocessError, json.JSONDecodeError):
            host = ""
        if not host:
            host = host_fallback
        return host.rstrip("/"), token
