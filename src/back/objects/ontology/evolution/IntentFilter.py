"""One filter condition inside an Intent."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from back.core.errors import ValidationError


@dataclass(frozen=True)
class IntentFilter:
    """One ``class.attribute op value`` condition. ``attribute`` ``id`` means the entity id."""

    cls: str
    attribute: str
    op: str
    value: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IntentFilter":
        if not isinstance(data, dict):
            raise ValidationError("Each filter must be an object")
        out = cls(
            cls=str(data.get("class") or "").strip(),
            attribute=str(data.get("attribute") or "").strip(),
            op=str(data.get("op") or "eq").strip().lower(),
            value=str(data.get("value") if data.get("value") is not None else ""),
        )
        if not out.cls or not out.attribute:
            raise ValidationError("A filter needs a class and an attribute")
        return out

    def to_dict(self) -> Dict[str, Any]:
        return {
            "class": self.cls,
            "attribute": self.attribute,
            "op": self.op,
            "value": self.value,
        }
