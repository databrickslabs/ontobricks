"""UC Knowledge Pages (Genie Ontology pages) REST client.

Isolates list/get of account Knowledge Pages and Databricks Domains from
the rest of the Databricks client surface. Used by Context → Pages.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import quote

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
        except Exception as exc:  # noqa: BLE001
            logger.warning("Knowledge Pages list failed: %s", exc)
            raise

        pages = normalize_knowledge_pages(data.get("pages") or [])
        token = data.get("next_page_token") or None
        return {"pages": pages, "next_page_token": token}

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
        """
        import requests

        if not self._auth.host or not self._auth.has_valid_auth():
            return []

        host = self._auth.host.rstrip("/")
        headers = self._auth.get_auth_headers()
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
                    label = (
                        str(d.get("subtitle") or "").strip()
                        or str(d.get("tag_key") or "").strip()
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
