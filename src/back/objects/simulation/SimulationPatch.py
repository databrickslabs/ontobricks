"""One hypothetical fact edit applied to a simulation sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from back.core.errors import ValidationError


@dataclass
class SimulationPatch:
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
    def from_dict(cls, data: Dict[str, Any]) -> "SimulationPatch":
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
