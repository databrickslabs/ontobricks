"""ShaclShapeFactory — builds SHACL shape config dicts + Turtle strings.

Foundation for T-M1.P1 (SHACL parser/generator/service unit tests).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ShaclShapeFactory:
    """Build SHACL shape dicts in the shape consumed by `SHACLParser`/`SHACLGenerator`.

    Args:
        target_class: IRI of the class the shape applies to.
        min_count: Optional min-cardinality constraint on the focus property.
        max_count: Optional max-cardinality constraint.
        datatype: Optional datatype constraint (xsd IRI).
        pattern: Optional regex pattern (`sh:pattern`).
        path_property: The IRI of the property the shape's PropertyShape constrains.
    """

    target_class: str = "http://test.org/ontology#Customer"
    path_property: str = "http://test.org/ontology#firstName"
    min_count: int | None = 1
    max_count: int | None = None
    datatype: str | None = "http://www.w3.org/2001/XMLSchema#string"
    pattern: str | None = None
    severity: str = "Violation"
    seed: int = 0

    @classmethod
    def build(cls, **overrides: Any) -> dict[str, Any]:
        return cls(**overrides)._build()

    @classmethod
    def build_turtle(cls, **overrides: Any) -> str:
        return cls(**overrides)._turtle()

    def _build(self) -> dict[str, Any]:
        shape: dict[str, Any] = {
            "shape_uri": f"{self.target_class}Shape",
            "target_class": self.target_class,
            "property_shapes": [
                {
                    "path": self.path_property,
                    "constraints": self._constraints(),
                    "severity": self.severity,
                }
            ],
        }
        return shape

    def _constraints(self) -> dict[str, Any]:
        c: dict[str, Any] = {}
        if self.min_count is not None:
            c["minCount"] = self.min_count
        if self.max_count is not None:
            c["maxCount"] = self.max_count
        if self.datatype:
            c["datatype"] = self.datatype
        if self.pattern:
            c["pattern"] = self.pattern
        return c

    def _turtle(self) -> str:
        prefixes = (
            "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
            "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
            "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
            "\n"
        )
        # Each line inside `sh:property [ ... ]` must end with `;` (last may omit
        # both `;` and `.` — Turtle then expects `]` to close the blank node).
        constraints: list[str] = [
            f"    sh:path <{self.path_property}>",
        ]
        if self.min_count is not None:
            constraints.append(f"    sh:minCount {self.min_count}")
        if self.max_count is not None:
            constraints.append(f"    sh:maxCount {self.max_count}")
        if self.datatype:
            constraints.append(f"    sh:datatype <{self.datatype}>")
        if self.pattern:
            constraints.append(f'    sh:pattern "{self.pattern}"')
        constraints.append(f"    sh:severity sh:{self.severity}")
        # Join with `; \n`, no trailing punctuation on the final line.
        property_block = " ;\n".join(constraints)
        body = (
            f"<{self.target_class}Shape> a sh:NodeShape ;\n"
            f"    sh:targetClass <{self.target_class}> ;\n"
            f"    sh:property [\n{property_block}\n    ] .\n"
        )
        return prefixes + body
