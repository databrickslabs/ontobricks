"""HTTP boundary between the MCP server and the OntoBricks REST surface.

Base-URL resolution, M2M OAuth header minting (with a module-level token
cache), and retrying GET/POST wrappers that ride out Databricks Apps
cold-start transients. Tests monkeypatch ``_get_auth_headers`` / ``_base_url``
/ ``_get`` / ``_post`` on this module, so call sites resolve them through the
module object (late binding) rather than importing the names directly.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx

from server.constants import _USER_AGENT

logger = logging.getLogger(__name__)

# Cached M2M OAuth token (module-level to survive across _get_auth_headers calls)
_oauth_cache: dict = {"token": "", "ts": 0.0}
_OAUTH_TOKEN_TTL = 3000  # refresh well before the typical 3600 s expiry


def _base_url(mode: str) -> str:
    """Resolve the OntoBricks REST API base URL for the given mode."""
    if mode == "mounted":
        port = os.getenv("DATABRICKS_APP_PORT", "8000")
        return f"http://localhost:{port}"
    return os.getenv("ONTOBRICKS_URL", "http://localhost:8000")


def _get_auth_headers(mode: str) -> dict:
    """Get authorization headers for the target OntoBricks app.

    In ``databricks`` mode the app's service principal obtains a fresh
    M2M OAuth token.  The token is cached for ``_OAUTH_TOKEN_TTL``
    seconds to avoid hitting the token endpoint on every request.

    Strategy (in order):
    1. Direct OIDC client-credentials grant using ``DATABRICKS_CLIENT_ID``
       / ``DATABRICKS_CLIENT_SECRET`` (most reliable in Apps runtime).
    2. Databricks SDK ``WorkspaceClient().config.authenticate()`` fallback.
    """
    if mode != "databricks":
        logger.debug("Auth: mode=%s, no headers attached", mode)
        return {}

    now = time.time()
    if _oauth_cache["token"] and (now - _oauth_cache["ts"]) < _OAUTH_TOKEN_TTL:
        age = int(now - _oauth_cache["ts"])
        logger.debug(
            "Auth: reusing cached M2M token (age=%ds, ttl=%ds)",
            age,
            _OAUTH_TOKEN_TTL,
        )
        return {"Authorization": f"Bearer {_oauth_cache['token']}"}

    # --- Strategy 1: direct M2M OAuth via OIDC endpoint ---
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "")
    host = os.getenv("DATABRICKS_HOST", "")

    if client_id and client_secret and host:
        try:
            h = host.strip().rstrip("/")
            if not h.startswith("http"):
                h = f"https://{h}"
            token_url = f"{h}/oidc/v1/token"
            logger.info("Requesting M2M OAuth token from %s", token_url)
            with httpx.Client(timeout=10, headers={"User-Agent": _USER_AGENT}) as c:
                resp = c.post(
                    token_url,
                    data={"grant_type": "client_credentials", "scope": "all-apis"},
                    auth=(client_id, client_secret),
                )
                resp.raise_for_status()
                token = resp.json()["access_token"]
            _oauth_cache["token"] = token
            _oauth_cache["ts"] = time.time()
            logger.info("M2M OAuth token obtained and cached (%d chars)", len(token))
            return {"Authorization": f"Bearer {token}"}
        except Exception as exc:
            logger.warning("M2M OAuth token request failed: %s", exc, exc_info=True)
    else:
        logger.info(
            "M2M OAuth env vars not all set (client_id=%s, client_secret=%s, host=%s)",
            bool(client_id),
            bool(client_secret),
            bool(host),
        )

    # --- Strategy 2: Databricks SDK header factory ---
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        result = w.config.authenticate()

        headers: dict = {}
        if isinstance(result, dict) and result:
            headers = result
        elif callable(result):
            try:
                out = result()
                if isinstance(out, dict) and out:
                    headers = out
            except TypeError:
                buf: dict = {}
                result(buf)
                if buf:
                    headers = buf

        if headers:
            logger.info("Auth headers obtained via SDK (%s)", ", ".join(headers.keys()))
            auth_val = headers.get("Authorization", "")
            if auth_val.startswith("Bearer "):
                _oauth_cache["token"] = auth_val[7:]
                _oauth_cache["ts"] = time.time()
            return headers
    except Exception as exc:
        logger.warning("SDK auth fallback failed: %s", exc, exc_info=True)

    logger.error("Could not obtain any Databricks auth token (mode=%s)", mode)
    return {}


_RETRYABLE_STATUSES = {502, 503}
_RETRY_DELAYS = (2, 5, 10)  # seconds between successive attempts (3 retries)


def _retryable(status: int) -> bool:
    return status in _RETRYABLE_STATUSES


def _retry_delays_for(client: httpx.AsyncClient) -> list[int]:
    """Retry schedule for *client*.

    502/503 retries exist to ride out Databricks Apps cold-start / proxy
    transients on the *remote* app hop. When we talk to a same-host app
    (``mounted`` mode, ``localhost``) there is no proxy in front, so a
    5xx is a real error — retrying only stacks latency. Disable retries
    there.
    """
    if "localhost" in str(client.base_url) or "127.0.0.1" in str(client.base_url):
        return []
    return list(_RETRY_DELAYS)


async def _get(
    client: httpx.AsyncClient, path: str, params: dict | None = None
) -> dict:
    """GET *path* on *client* and return the JSON body.

    Logs the full effective URL and response status so deployed-app
    debugging surfaces auth failures, registry overrides, and silent
    empty payloads in the Apps log stream. On non-2xx responses we
    log a body excerpt before re-raising so the caller (and the LLM)
    sees an actionable error instead of a bare ``HTTPStatusError``.

    502/503 responses (Databricks Apps cold-start / proxy transient
    errors) are retried up to 3 times with increasing delays before
    the error is propagated.
    """
    delays = _retry_delays_for(client)
    attempt = 0
    while True:
        logger.info(
            "GET %s%s params=%s (attempt %d)", client.base_url, path, params or {}, attempt + 1
        )
        started = time.monotonic()
        resp = await client.get(path, params=params, timeout=120)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if resp.status_code >= 400:
            body_excerpt = resp.text[:500].replace("\n", " ") if resp.text else ""
            logger.warning(
                "GET %s%s → %s in %dms body=%r",
                client.base_url,
                path,
                resp.status_code,
                elapsed_ms,
                body_excerpt,
            )
            if _retryable(resp.status_code) and delays:
                delay = delays.pop(0)
                logger.info(
                    "Retrying in %ds (status=%s, attempt %d)…",
                    delay,
                    resp.status_code,
                    attempt + 1,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
        else:
            logger.info("GET %s%s → %s in %dms", client.base_url, path, resp.status_code, elapsed_ms)
        resp.raise_for_status()
        return resp.json()


async def _post(
    client: httpx.AsyncClient, path: str, json: dict | None = None
) -> dict:
    """POST *path* on *client* with optional JSON body and return the JSON response.

    502/503 responses are retried up to 3 times with increasing delays.
    """
    delays = _retry_delays_for(client)
    attempt = 0
    while True:
        logger.info("POST %s%s (attempt %d)", client.base_url, path, attempt + 1)
        started = time.monotonic()
        resp = await client.post(path, json=json or {}, timeout=120)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        if resp.status_code >= 400:
            body_excerpt = resp.text[:500].replace("\n", " ") if resp.text else ""
            logger.warning(
                "POST %s%s → %s in %dms body=%r",
                client.base_url,
                path,
                resp.status_code,
                elapsed_ms,
                body_excerpt,
            )
            if _retryable(resp.status_code) and delays:
                delay = delays.pop(0)
                logger.info(
                    "Retrying in %ds (status=%s, attempt %d)…",
                    delay,
                    resp.status_code,
                    attempt + 1,
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue
        else:
            logger.info("POST %s%s → %s in %dms", client.base_url, path, resp.status_code, elapsed_ms)
        resp.raise_for_status()
        return resp.json()
