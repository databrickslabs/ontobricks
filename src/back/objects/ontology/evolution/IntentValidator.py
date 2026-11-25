"""Check a suggested intent against the parent version before anyone uses it."""

from __future__ import annotations

from typing import Dict, List

from back.objects.ontology.evolution.Intent import Intent
from back.objects.ontology.evolution.OntologyIndex import OntologyIndex

FILTER_OPS = ("eq", "ne", "gt", "lt", "ge", "le")


class IntentValidator:
    """Split problems into *errors* (reject) and *notes* (pass on to coverage).

    A class the ontology does not have is a note: the question may really
    be about a missing concept, and coverage reports it as such. An
    unknown attribute on a known class, an unknown relationship in the path
    hint or an unknown operator is an error, because it can only be a
    misreading or an invention.
    """

    def __init__(self, index: OntologyIndex):
        self._index = index

    def check(self, intent: Intent) -> Dict[str, List[str]]:
        errors: List[str] = []
        notes: List[str] = []
        classes = self._index.classes
        if intent.output_class and intent.output_class not in classes:
            notes.append(f"class_not_in_ontology:{intent.output_class}")
        for flt in intent.filters:
            if flt.op not in FILTER_OPS:
                errors.append(f"unknown_operator:{flt.op}")
            if not str(flt.value).strip():
                errors.append(f"empty_value:{flt.cls}.{flt.attribute}")
            if flt.cls not in classes:
                notes.append(f"class_not_in_ontology:{flt.cls}")
            elif flt.attribute != "id" and not self._index.attribute(
                flt.cls, flt.attribute
            ):
                errors.append(f"unknown_attribute:{flt.cls}.{flt.attribute}")
        for name in intent.path_hint:
            if name not in self._index.relationships:
                errors.append(f"unknown_relationship:{name}")
        return {"errors": errors, "notes": sorted(set(notes))}
