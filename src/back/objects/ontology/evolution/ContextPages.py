"""Genie Knowledge Pages curated on a version, as read-only context for the Evolve agent."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Callable, Dict, List, Optional

from back.core.databricks.knowledge_pages import normalize_knowledge_pages
from back.core.logging import get_logger

logger = get_logger(__name__)

_WORD = re.compile(r"[a-z0-9#]+")
_SPACE = re.compile(r"\s+")
_EMPHASIS = re.compile(r"[*`]+")
_STOP = frozenset(
    "a an and are as at be by does for from in is it of on or that the this to which with".split()
)


def _flat(text: str) -> str:
    """Case, spacing and markdown emphasis do not count when matching a quote."""
    return _SPACE.sub(" ", _EMPHASIS.sub("", str(text or ""))).strip().lower()


class ContextPages:
    """Page text the agent may read and quote, and nothing else.

    Only pages selected on the parent version (``info.knowledge_pages``, the
    same allowlist Generate uses) are fetched, with the caller's identity.
    Page text is business context: it can name a concept or pick between
    two readings of a question, but it is never evidence that a join holds
    or that an answer is right. A quote the agent attributes to a page is
    kept only when it appears in that page verbatim.
    """

    MAX_PAGES = 15
    MAX_BODY_CHARS = 20_000
    MAX_PASSAGE_CHARS = 600
    MIN_QUOTE_CHARS = 8
    MAX_QUOTE_CHARS = 400

    def __init__(
        self,
        pages: List[Dict[str, Any]],
        skipped: Optional[List[Dict[str, str]]] = None,
    ):
        self._pages = {p["name"]: p for p in pages}
        self.skipped = list(skipped or [])

    @staticmethod
    def selected(document: Dict[str, Any]) -> List[str]:
        info = document.get("info") or {}
        return [
            p["name"] for p in normalize_knowledge_pages(info.get("knowledge_pages"))
        ]

    @classmethod
    def load(
        cls, document: Dict[str, Any], fetch: Callable[[str], Dict[str, Any]]
    ) -> "ContextPages":
        """Fetch the selected pages with *fetch* (``name → page``).

        A page that cannot be read is listed in ``skipped``, not raised.
        """
        pages, skipped = [], []
        names = cls.selected(document)
        for name in names[: cls.MAX_PAGES]:
            try:
                page = fetch(name) or {}
            except (
                Exception
            ) as exc:  # noqa: BLE001 — one unreadable page must not stop the run
                logger.warning("Evolve context page %s skipped: %s", name, exc)
                skipped.append({"name": name, "reason": _reason(exc)})
                continue
            body = str(page.get("body") or "")
            pages.append(
                {
                    "name": name,
                    "title": str(page.get("display_name") or name),
                    "description": str(page.get("description") or ""),
                    "synonyms": list(page.get("synonyms") or []),
                    "update_time": str(page.get("update_time") or ""),
                    "body": body[: cls.MAX_BODY_CHARS],
                    "truncated": len(body) > cls.MAX_BODY_CHARS,
                    "body_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                }
            )
        skipped += [
            {"name": n, "reason": "over_page_limit"} for n in names[cls.MAX_PAGES :]
        ]
        return cls(pages, skipped)

    def __bool__(self) -> bool:
        return bool(self._pages)

    def outline(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": p["name"],
                "title": p["title"],
                "description": p["description"][:300],
                "synonyms": p["synonyms"][:20],
                "chars": len(p["body"]),
            }
            for p in self._pages.values()
        ]

    def manifest(self) -> Dict[str, Any]:
        """What the agent could read, for the run record."""
        return {
            "pages": [
                {
                    k: p[k]
                    for k in (
                        "name",
                        "title",
                        "update_time",
                        "body_sha256",
                        "truncated",
                    )
                }
                for p in self._pages.values()
            ],
            "skipped": self.skipped,
        }

    def _passages(self):
        for page in self._pages.values():
            for block in re.split(r"\n\s*\n|\n(?=[-|#])", page["body"]):
                text = block.strip()
                if text:
                    yield page, text[: self.MAX_PASSAGE_CHARS]

    def search(self, query: str, limit: int = 5) -> List[Dict[str, str]]:
        terms = {w for w in _WORD.findall(_flat(query)) if w not in _STOP}
        if not terms:
            return []
        scored = []
        for page, text in self._passages():
            words = set(_WORD.findall(_flat(text)))
            hits = len(terms & words)
            if hits:
                scored.append((hits, page["name"], text))
        scored.sort(key=lambda s: -s[0])
        return [
            {"page": name, "title": self._pages[name]["title"], "passage": text}
            for _hits, name, text in scored[:limit]
        ]

    def read(self, name: str, offset: int = 0, limit: int = 4000) -> Dict[str, Any]:
        page = self._pages.get(name)
        if not page:
            return {"error": f"Page {name!r} is not selected for this version"}
        offset = max(0, int(offset or 0))
        limit = max(1, min(int(limit or 4000), 8000))
        return {
            "page": name,
            "title": page["title"],
            "text": page["body"][offset : offset + limit],
            "next_offset": (
                offset + limit if offset + limit < len(page["body"]) else None
            ),
        }

    def verify(self, citations: Any) -> List[Dict[str, str]]:
        """Keep only ``{page, quote}`` items whose quote appears verbatim in that page."""
        kept = []
        for item in citations if isinstance(citations, list) else []:
            if not isinstance(item, dict):
                continue
            page = self._pages.get(str(item.get("page") or ""))
            quote = str(item.get("quote") or "").strip()
            if (
                page
                and self.MIN_QUOTE_CHARS <= len(quote) <= self.MAX_QUOTE_CHARS
                and _flat(quote) in _flat(page["body"])
            ):
                kept.append(
                    {"page": page["name"], "title": page["title"], "quote": quote}
                )
        return kept


def _reason(exc: Exception) -> str:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    if status in (401, 403):
        return "access_denied"
    if status == 404:
        return "not_found"
    return "unavailable"
