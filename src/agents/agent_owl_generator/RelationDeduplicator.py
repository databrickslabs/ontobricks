"""Semantic deduplication of Stage-3 Generate object-property relations."""

from __future__ import annotations

import re
from typing import Any, Dict, Hashable, Iterable, List, Sequence

_COPULAS = frozenset({"is", "was", "are", "were", "be", "been", "being"})
_INVERSE_MARKERS = frozenset({"by", "of"})
_PASSIVE_PREFIXES = ("is", "was", "are", "were")
_PASSIVE_SUFFIXES = ("by", "of")
_MIN_SEMANTIC_CORE = 4
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


class RelationDeduplicator:
    """Collapse paraphrased, passive, and inverse restatements of one relation.

    Two relations are the same fact when they link the same unordered pair of
    entities and share a verb core once copulas (``is``/``was``/…), trailing
    inverse markers (``by``/``of``) and inflections are stripped:
    ``settles`` (Payment→Contract), ``settledBy`` and ``isSettledBy``
    (Contract→Payment) all key on ``({Contract, Payment}, "settl")``.
    Cores shorter than four characters are too ambiguous to stem safely and
    fall back to an exact, directed label key, so distinct facts are kept.
    """

    @staticmethod
    def tokens(label: str) -> List[str]:
        spaced = _CAMEL_BOUNDARY.sub(" ", label or "")
        return [t for t in re.split(r"[^A-Za-z0-9]+", spaced.casefold()) if t]

    @staticmethod
    def stem(token: str) -> str:
        if len(token) > 4 and token.endswith(("ied", "ies")):
            token = token[:-3] + "y"
        elif len(token) > 5 and token.endswith("ing"):
            token = token[:-3]
        elif len(token) > 3 and token.endswith("es"):
            token = token[:-2]
        elif len(token) > 3 and token.endswith("ed"):
            token = token[:-2]
        elif len(token) > 3 and token.endswith("s") and not token.endswith("ss"):
            token = token[:-1]
        if len(token) > 3 and token.endswith("e"):
            token = token[:-1]
        return token

    @classmethod
    def semantic_key(cls, relation: Dict[str, Any]) -> Hashable:
        domain = str(relation.get("domain") or "")
        range_ = str(relation.get("range") or "")
        raw = cls.tokens(relation.get("label") or "")
        core_tokens = [t for t in raw if t not in _COPULAS]
        if len(core_tokens) > 1 and core_tokens[-1] in _INVERSE_MARKERS:
            core_tokens = core_tokens[:-1]
        core = "".join(cls.stem(t) for t in core_tokens)
        if len(core) >= _MIN_SEMANTIC_CORE:
            return (frozenset((domain, range_)), core)
        return (domain, range_, "".join(raw))

    @staticmethod
    def voice_score(label: str) -> int:
        """Higher is more active-voice (``handles`` beats ``handled`` / ``isHandledBy``)."""
        token = re.sub(r"[^a-z0-9]", "", (label or "").casefold())
        score = 0
        if any(token.startswith(prefix) for prefix in _PASSIVE_PREFIXES):
            score -= 4
        if any(token.endswith(suffix) for suffix in _PASSIVE_SUFFIXES):
            score -= 2
        if len(token) > 3 and token.endswith("ed") and not token.endswith("eed"):
            score -= 3
        return score

    @classmethod
    def dedupe(
        cls,
        relations: Sequence[Dict[str, Any]],
        existing: Iterable[Dict[str, Any]] = (),
    ) -> List[Dict[str, Any]]:
        """Keep one active-voice relation per semantic key, dropping any
        relation that restates one in ``existing``. Order of first appearance
        is preserved."""
        taken = {cls.semantic_key(rel) for rel in existing}
        winners: Dict[Hashable, Dict[str, Any]] = {}
        for rel in relations:
            key = cls.semantic_key(rel)
            if key in taken:
                continue
            current = winners.get(key)
            if current is None or cls.voice_score(rel.get("label") or "") > cls.voice_score(
                current.get("label") or ""
            ):
                winners[key] = rel
        return list(winners.values())
