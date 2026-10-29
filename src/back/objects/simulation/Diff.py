"""Before/after comparison of a simulation sandbox."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set, Tuple

TripleKey = Tuple[str, str, str]


class Diff:
    """Set difference between the seeded slice (S0) and the final sandbox (S2).

    ``added`` triples are tagged ``patch`` when a user patch introduced them
    and ``inferred`` otherwise (reasoning consequence); ``removed`` triples
    are tagged ``patch`` (explicit remove / set overwrite).
    """

    KINDS = ("added", "removed")
    DEFAULT_ROW_CAP = 2000

    @staticmethod
    def key(triple: Dict[str, Any]) -> TripleKey:
        return (
            str(triple.get("subject", "") or ""),
            str(triple.get("predicate", "") or ""),
            str(triple.get("object", "") or ""),
        )

    @classmethod
    def keys(cls, triples: Iterable[Dict[str, Any]]) -> Set[TripleKey]:
        return {cls.key(t) for t in triples}

    @classmethod
    def compute(
        cls,
        before: Iterable[Dict[str, Any]],
        after: Iterable[Dict[str, Any]],
        *,
        patch_added: Set[TripleKey],
        provenance: Dict[TripleKey, str] | None = None,
        row_cap: int = DEFAULT_ROW_CAP,
    ) -> Dict[str, Any]:
        """Return ``{"summary": {...}, "added": [...], "removed": [...]}``.

        Row lists are sorted and capped at *row_cap*; ``summary`` always
        carries the exact counts.
        """
        s0 = cls.keys(before)
        s2 = cls.keys(after)
        prov = provenance or {}
        added_keys = sorted(s2 - s0)
        removed_keys = sorted(s0 - s2)

        added: List[Dict[str, str]] = []
        n_patch = n_inferred = 0
        for k in added_keys:
            origin = "patch" if k in patch_added else "inferred"
            if origin == "patch":
                n_patch += 1
            else:
                n_inferred += 1
            if len(added) < row_cap:
                row = cls._row(k, origin)
                if origin == "inferred" and k in prov:
                    row["provenance"] = prov[k]
                added.append(row)

        removed = [cls._row(k, "patch") for k in removed_keys[:row_cap]]
        touched = {k[0] for k in added_keys} | {k[0] for k in removed_keys}
        return {
            "summary": {
                "before_count": len(s0),
                "after_count": len(s2),
                "added_count": len(added_keys),
                "added_patch_count": n_patch,
                "added_inferred_count": n_inferred,
                "removed_count": len(removed_keys),
                "touched_entity_count": len(touched),
                "capped": len(added_keys) > row_cap or len(removed_keys) > row_cap,
                "row_cap": row_cap,
            },
            "added": added,
            "removed": removed,
        }

    @staticmethod
    def _row(k: TripleKey, origin: str) -> Dict[str, str]:
        return {"subject": k[0], "predicate": k[1], "object": k[2], "origin": origin}

    @classmethod
    def page(
        cls,
        diff: Dict[str, Any],
        kind: str,
        page: int = 1,
        page_size: int = 50,
        search: str = "",
    ) -> Dict[str, Any]:
        """Slice the stored diff for the Results tables."""
        if kind not in cls.KINDS:
            kind = "added"
        rows = list((diff or {}).get(kind) or [])
        needle = (search or "").strip().lower()
        if needle:
            rows = [
                r
                for r in rows
                if needle in r.get("subject", "").lower()
                or needle in r.get("predicate", "").lower()
                or needle in r.get("object", "").lower()
            ]
        page_size = max(1, min(int(page_size), 500))
        page = max(1, int(page))
        start = (page - 1) * page_size
        return {
            "kind": kind,
            "page": page,
            "page_size": page_size,
            "total": len(rows),
            "rows": rows[start : start + page_size],
        }
