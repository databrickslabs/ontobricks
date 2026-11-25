"""UC Knowledge Pages (Genie Ontology pages) REST client.

Isolates list/get of account Knowledge Pages and Databricks Domains from
the rest of the Databricks client surface. Used by Context → Pages.
"""

from __future__ import annotations

import copy
import hashlib
import re
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from back.core.errors import InfrastructureError
from back.core.logging import get_logger

from .constants import (
    DOMAINS_PATH,
    KNOWLEDGE_PAGES_PATH,
    _DOMAINS_MAX_PAGES,
    _DOMAINS_PAGE_SIZE,
    _KNOWLEDGE_PAGES_PAGE_SIZE,
    _REQUEST_TIMEOUT,
)
from .DatabricksAuth import DatabricksAuth
from .knowledge_pages import normalize_knowledge_pages

logger = get_logger(__name__)

_PAGE_ID_RE = re.compile(r"^[0-9a-fA-F-]{8,}$")
# Databricks Domains rarely change but a large workspace needs ~10 sequential
# paginated calls (~14 s for ~1000 domains). Cache per (host, identity).
_DOMAINS_CACHE_TTL_S = 600
_domains_cache: Dict[Tuple[str, str], Tuple[float, List[Dict[str, str]]]] = {}
_domains_cache_lock = threading.Lock()
_REMOTE_MESSAGE_MAX = 200
_LIST_HTTP_HINTS = {
    401: (
        "Authentication failed: Databricks rejected the credentials used to "
        "list Knowledge Pages (Genie Pages). Check the app's Databricks authentication."
    ),
    403: (
        "Permission denied: the Databricks identity used by OntoBricks is not "
        "allowed to list Knowledge Pages (Genie Pages) in this workspace. "
        "Ask a workspace admin to grant access."
    ),
    404: (
        "Not found: the Databricks Knowledge Pages API is not available in this "
        "workspace. Genie Pages may not be enabled."
    ),
    429: (
        "Rate limited: Databricks is throttling Knowledge Pages requests. "
        "Retry in a moment."
    ),
}


class KnowledgePageService:
    """List and fetch Unity Catalog Knowledge Pages."""

    def __init__(self, auth: DatabricksAuth) -> None:
        self._auth = auth

    def list_pages(
        self,
        *,
        page_size: int = _KNOWLEDGE_PAGES_PAGE_SIZE,
        page_token: Optional[str] = None,
        filter_str: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return one page of Knowledge Pages from Databricks.

        Response shape: ``{"pages": [...], "next_page_token": str|None}``.
        Each page is a normalized snapshot (no body).
        """
        import requests

        if not self._auth.host:
            logger.warning("No host configured for Knowledge Pages list")
            return {"pages": [], "next_page_token": None}
        if not self._auth.has_valid_auth():
            logger.warning("No valid auth for Knowledge Pages list")
            return {"pages": [], "next_page_token": None}

        size = max(1, min(int(page_size or _KNOWLEDGE_PAGES_PAGE_SIZE), 100))
        params: Dict[str, Any] = {"page_size": size}
        if page_token:
            params["page_token"] = page_token
        if filter_str and str(filter_str).strip():
            params["filter"] = str(filter_str).strip()

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        try:
            response = requests.get(
                f"{host}{KNOWLEDGE_PAGES_PATH}",
                headers=headers,
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json() or {}
        except requests.RequestException as exc:
            logger.warning("Knowledge Pages list failed: %s", exc)
            raise self._list_error(exc) from exc

        pages = normalize_knowledge_pages(data.get("pages") or [])
        token = data.get("next_page_token") or None
        return {"pages": pages, "next_page_token": token}

    @staticmethod
    def _list_error(exc: Exception) -> InfrastructureError:
        """Translate a failed Knowledge Pages request into an explicit message.

        The message names the cause (permissions, auth, missing API, network…)
        because only ``message`` reaches the UI; ``detail`` is stripped outside
        debug mode.
        """
        response = getattr(exc, "response", None)
        if response is None:
            return InfrastructureError(
                "Could not reach Databricks to list Knowledge Pages (Genie Pages). "
                "Check the workspace host and network connectivity.",
                detail=str(exc),
            )
        status = response.status_code
        headline = _LIST_HTTP_HINTS.get(status) or (
            "Databricks failed to serve Knowledge Pages. Retry later."
            if status >= 500
            else "Databricks refused the Knowledge Pages request."
        )
        message = f"{headline} (HTTP {status})"
        try:
            body = response.json()
        except ValueError:
            body = None
        remote = str(body.get("message") or "").strip() if isinstance(body, dict) else ""
        if remote:
            message += f" Databricks said: {remote[:_REMOTE_MESSAGE_MAX]}"
        if status == 403 and "domain filter" in remote.lower():
            message += " Select a Domain in the Domains filter to list its pages."
        return InfrastructureError(message, detail=str(exc))

    def get_page(self, name: str) -> Dict[str, Any]:
        """Fetch a full Knowledge Page by resource name ``pages/{id}``."""
        import requests

        page_id = _page_id_from_name(name)
        if not page_id:
            raise ValueError(f"Invalid Knowledge Page name: {name!r}")
        if not self._auth.host or not self._auth.has_valid_auth():
            raise RuntimeError("Databricks authentication is not configured")

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        path = f"{KNOWLEDGE_PAGES_PATH}/{quote(page_id, safe='')}"
        response = requests.get(
            f"{host}{path}",
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json() or {}
        snap = normalize_knowledge_pages([data])
        out = snap[0] if snap else {"name": f"pages/{page_id}"}
        # Preview needs the body; keep it outside the stored snapshot shape.
        out["body"] = str(data.get("body") or "")
        out["create_time"] = str(data.get("create_time") or "")
        out["owner_ids"] = [
            str(x) for x in (data.get("owner_ids") or []) if x is not None
        ]
        return out

    def list_domains(self) -> List[Dict[str, str]]:
        """Return Databricks Domains for the Pages filter dropdown.

        Each item: ``name`` (``domains/{id}``), ``domain_id``, ``subtitle`` /
        ``tag_key`` as a short label.

        Results are memoised per ``(host, identity)`` for
        ``_DOMAINS_CACHE_TTL_S`` seconds; only successful fetches are cached.
        """
        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
        key = (host, self._identity_fingerprint(headers))
        now = time.monotonic()
        with _domains_cache_lock:
            cached = _domains_cache.get(key)
        if cached and (now - cached[0]) < _DOMAINS_CACHE_TTL_S:
            return copy.deepcopy(cached[1])

        results = self._fetch_domains(host, headers)
        with _domains_cache_lock:
            _domains_cache[key] = (time.monotonic(), results)
        return copy.deepcopy(results)

    @staticmethod
    def clear_domains_cache() -> None:
        """Drop every memoised Databricks Domains list."""
        with _domains_cache_lock:
            _domains_cache.clear()

    @staticmethod
    def _identity_fingerprint(headers: Dict[str, str]) -> str:
        """Short, non-reversible cache key for the caller's credentials."""
        auth = str(headers.get("Authorization") or "")
        return hashlib.sha256(auth.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _fetch_domains(host: str, headers: Dict[str, str]) -> List[Dict[str, str]]:
        """Page through the Databricks Domains API (uncached)."""
        import requests

        results: List[Dict[str, str]] = []
        page_token: Optional[str] = None
        pages = 0
        try:
            while pages < _DOMAINS_MAX_PAGES:
                params: Dict[str, Any] = {"page_size": _DOMAINS_PAGE_SIZE}
                if page_token:
                    params["page_token"] = page_token
                response = requests.get(
                    f"{host}{DOMAINS_PATH}",
                    headers=headers,
                    params=params,
                    timeout=_REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json() or {}
                batch = data.get("domains") or []
                if isinstance(data, list):
                    batch = data
                for d in batch:
                    if not isinstance(d, dict):
                        continue
                    domain_id = str(d.get("domain_id") or "").strip()
                    name = str(d.get("name") or "").strip()
                    if not name and domain_id:
                        name = f"domains/{domain_id}"
                    if not name.startswith("domains/"):
                        continue
                    # tag_key is the governed-tag domain name people recognize;
                    # subtitle is only a short description.
                    label = (
                        str(d.get("tag_key") or "").strip()
                        or str(d.get("subtitle") or "").strip()
                        or domain_id
                        or name
                    )
                    results.append(
                        {
                            "name": name,
                            "domain_id": domain_id or name.split("/", 1)[-1],
                            "label": label,
                        }
                    )
                page_token = data.get("next_page_token") if isinstance(data, dict) else None
                pages += 1
                if not page_token or not batch:
                    break
        except Exception as exc:  # noqa: BLE001
            logger.warning("Databricks Domains list failed: %s", exc)
            raise
        results.sort(key=lambda r: (r.get("label") or "").lower())
        return results


def _page_id_from_name(name: str) -> Optional[str]:
    text = str(name or "").strip()
    if text.startswith("pages/"):
        text = text[len("pages/") :]
    text = text.strip()
    if not text or "/" in text or not _PAGE_ID_RE.match(text):
        return None
    return text
