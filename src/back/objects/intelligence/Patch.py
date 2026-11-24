"""One hypothetical fact edit applied to a simulation sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from back.core.errors import ValidationError


@dataclass
class Patch:
    """A single ``add`` / ``remove`` / ``set`` edit on a triple.

    ``set`` replaces every ``(subject, predicate, *)`` value with ``object``
    (functional overwrite, e.g. a new credit score); ``add`` and ``remove``
    act on the exact triple.
    """

    OPS = ("add", "remove", "set")

    op: str
    subject: str
    predicate: str
    object: str = ""
    datatype: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Patch":
        if not isinstance(data, dict):
            raise ValidationError("Each patch must be an object")
        patch = cls(
            op=str(data.get("op", "") or "").strip().lower(),
            subject=str(data.get("subject", "") or "").strip(),
            predicate=str(data.get("predicate", "") or "").strip(),
            object=str(data.get("object", "") or ""),
            datatype=str(data.get("datatype", "") or "").strip(),
        )
        patch.validate()
        return patch

    def validate(self) -> None:
        if self.op not in self.OPS:
            raise ValidationError(
                f"Patch op must be one of {', '.join(self.OPS)}", detail=self.op
            )
        if not self.subject or not self.predicate:
            raise ValidationError("Patch subject and predicate are required")
        if self.op in ("add", "set") and self.object == "":
            raise ValidationError(f"Patch '{self.op}' requires an object value")

    def triple(self) -> Dict[str, str]:
        out = {"subject": self.subject, "predicate": self.predicate, "object": self.object}
        if self.datatype:
            out["datatype"] = self.datatype
        return out

    def to_dict(self) -> Dict[str, str]:
        out = {
            "op": self.op,
            "subject": self.subject,
            "predicate": self.predicate,
            "object": self.object,
        }
        if self.datatype:
            out["datatype"] = self.datatype
        return out

    @classmethod
    def append_all(cls, existing: Any, incoming: Any) -> List[Dict[str, str]]:
        """Append *incoming* onto *existing*, dropping duplicates and conflicts."""
        rows = cls._parse_many(existing)
        for patch in cls._parse_many(incoming):
            rows = cls._append_one(rows, patch)
        return [item.to_dict() for item in rows]

    @classmethod
    def _parse_many(cls, raw: Any) -> List["Patch"]:
        rows: List[Patch] = []
        if not isinstance(raw, list):
            return rows
        for item in raw:
            try:
                rows.append(cls.from_dict(item))
            except (ValidationError, TypeError, ValueError):
                continue
        return rows

    @classmethod
    def _append_one(cls, rows: List["Patch"], patch: "Patch") -> List["Patch"]:
        if any(cls._same_fact(item, patch) for item in rows):
            return rows
        if patch.op == "set":
            kept = [item for item in rows if not cls._same_pred(item, patch)]
            kept.append(patch)
            return kept
        if patch.op == "add":
            kept = [
                item
                for item in rows
                if not (item.op == "remove" and cls._same_spo(item, patch))
            ]
            if any(item.op in ("add", "set") and cls._same_spo(item, patch) for item in kept):
                return kept
            kept.append(patch)
            return kept
        if any(
            item.op == "set" and cls._same_pred(item, patch) and item.object != patch.object
            for item in rows
        ):
            return rows
        kept = [
            item
            for item in rows
            if not (item.op in ("add", "set") and cls._same_spo(item, patch))
        ]
        if len(kept) < len(rows):
            return kept
        kept.append(patch)
        return kept

    @staticmethod
    def _same_fact(left: "Patch", right: "Patch") -> bool:
        return (
            left.op == right.op
            and left.datatype == right.datatype
            and Patch._same_spo(left, right)
        )

    @staticmethod
    def _same_spo(left: "Patch", right: "Patch") -> bool:
        return (
            left.subject == right.subject
            and left.predicate == right.predicate
            and left.object == right.object
        )

    @staticmethod
    def _same_pred(left: "Patch", right: "Patch") -> bool:
        return left.subject == right.subject and left.predicate == right.predicate
