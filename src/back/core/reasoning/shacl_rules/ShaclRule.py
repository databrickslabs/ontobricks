"""Canonical SHACL-AF rule model.

A :class:`ShaclRule` is the intermediate representation the reasoning engine
executes instead of raw IF-THEN text.  It is produced by
:class:`IfThenToShaclTranspiler` from the IF/THEN rules authored in the UI and
carries three views of the same logic:

- ``rule_type`` / ``turtle`` — the canonical SHACL-AF serialisation
  (``sh:TripleRule`` for simple class/property assertions, ``sh:SPARQLRule``
  with a ``CONSTRUCT`` otherwise);
- ``construct_sparql`` — the SPARQL ``CONSTRUCT`` equivalent (also embedded in
  the Turtle for ``sh:SPARQLRule`` shapes);
- ``antecedent`` / ``consequent`` — the executable body, kept so the flat
  triple-store SQL backend can generate identical SQL (capability parity with
  the legacy IF-THEN engine).

The executable body is intentionally preserved: a ``sh:SPARQLRule`` CONSTRUCT
over the flat ``(subject, predicate, object)`` table is semantically the same
graph pattern as the originating IF-THEN rule, so the proven flat-triple SQL
backend can be reused to guarantee identical inference results at scale.
"""

from __future__ import annotations

from dataclasses import dataclass

from back.core.w3c.shacl.constants import IF_THEN_ACTION_ASSERT, IF_THEN_ACTION_RETRACT

#: SHACL-AF rule-shape discriminators.
TRIPLE_RULE = "sh:TripleRule"
SPARQL_RULE = "sh:SPARQLRule"
#: A retraction removes triples, which SHACL-AF cannot express. It runs as a
#: removal; ``construct_sparql`` holds its SPARQL Update equivalent.
RETRACT_RULE = "retract:DeleteWhere"


@dataclass
class ShaclRule:
    """A single SHACL-AF rule transpiled from an IF-THEN rule."""

    id: str
    name: str
    rule_type: str
    target_class_uri: str
    antecedent: str
    consequent: str
    construct_sparql: str
    turtle: str
    enabled: bool = True
    description: str = ""
    action: str = IF_THEN_ACTION_ASSERT

    @property
    def is_sparql_rule(self) -> bool:
        return self.rule_type == SPARQL_RULE

    @property
    def is_retraction(self) -> bool:
        return self.action == IF_THEN_ACTION_RETRACT
