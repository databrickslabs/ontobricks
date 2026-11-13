"""PatternVocabulary -- URI helpers for discovered-pattern materialisation.

Many named patterns can coexist: membership uses a shared ``inPattern``
predicate whose object is the saved pattern URI. Deleting one pattern
removes triples that use that URI as subject or object.
"""

from __future__ import annotations

import re
from typing import Final


class PatternVocabulary:
    """Domain-aware URI builder for pattern triples.

    Pure helpers: ``base_uri`` in, URI string out. No I/O.
    """

    PATTERN_CLASS_FRAGMENT: Final[str] = "Pattern"
    IN_PATTERN_FRAGMENT: Final[str] = "inPattern"
    FROM_DISCOVERY_FRAGMENT: Final[str] = "fromDiscovery"
    PATTERN_SIZE_FRAGMENT: Final[str] = "patternSize"
    PATTERN_PATH_FRAGMENT: Final[str] = "pattern"

    _SAVED_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

    @staticmethod
    def join(base: str, fragment: str) -> str:
        if not base:
            return fragment
        if base.endswith("/") or base.endswith("#"):
            return base + fragment
        return base + "/" + fragment

    @classmethod
    def pattern_class(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.PATTERN_CLASS_FRAGMENT)

    @classmethod
    def in_pattern(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.IN_PATTERN_FRAGMENT)

    @classmethod
    def from_discovery(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.FROM_DISCOVERY_FRAGMENT)

    @classmethod
    def pattern_size(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.PATTERN_SIZE_FRAGMENT)

    @classmethod
    def pattern_prefix(cls, base_uri: str) -> str:
        return cls.join(base_uri, cls.PATTERN_PATH_FRAGMENT) + "/"

    @classmethod
    def pattern(cls, base_uri: str, saved_id: str) -> str:
        """Return ``…/pattern/{saved_id}``. *saved_id* must be ``[A-Za-z0-9_-]+``."""
        sid = (saved_id or "").strip()
        if not sid or not cls._SAVED_ID_RE.match(sid):
            raise ValueError(f"invalid pattern saved_id: {saved_id!r}")
        return f"{cls.pattern_prefix(base_uri)}{sid}"

    @classmethod
    def parse_saved_id(cls, pattern_uri: str) -> str:
        """Return the tail after ``/pattern/``, or ``""``."""
        if not pattern_uri:
            return ""
        marker = f"/{cls.PATTERN_PATH_FRAGMENT}/"
        # Also accept a hash-separator form (``…#pattern/id``).
        hash_marker = f"#{cls.PATTERN_PATH_FRAGMENT}/"
        for sep in (marker, hash_marker):
            idx = pattern_uri.rfind(sep)
            if idx >= 0:
                tail = pattern_uri[idx + len(sep) :].rstrip("/")
                return tail if cls._SAVED_ID_RE.match(tail) else ""
        return ""
