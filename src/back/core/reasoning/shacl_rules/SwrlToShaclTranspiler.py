"""Transpile SWRL IF/THEN rules into canonical SHACL-AF rules.

The UI still authors rules as SWRL (``antecedent`` / ``consequent``); this
transpiler converts each rule into a :class:`ShaclRule` whose canonical form
is SHACL-AF (``sh:TripleRule`` / ``sh:SPARQLRule``).  Classification:

- antecedent with **no** built-ins and **no** negation, and a consequent made
  only of simple class / property assertions -> ``sh:TripleRule``;
- anything else (built-ins, negation, multi-hop) -> ``sh:SPARQLRule`` carrying
  a ``CONSTRUCT { ... } WHERE { ... }`` body.

The SWRL body is preserved on the :class:`ShaclRule` so the flat triple-store
SQL backend can generate byte-for-byte the same inference SQL as before.
"""

from __future__ import annotations

from typing import Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.constants import RDF_TYPE
from back.core.reasoning.shacl_rules.RuleBuiltinRegistry import RuleBuiltinRegistry
from back.core.reasoning.shacl_rules.RuleExpressionParser import RuleExpressionParser
from back.core.reasoning.shacl_rules.rule_uri_map import build_uri_map
from back.core.reasoning.shacl_rules.ShaclRule import (
    ShaclRule,
    SPARQL_RULE,
    TRIPLE_RULE,
)

logger = get_logger(__name__)

#: SWRL comparison built-ins that map cleanly to a SPARQL FILTER operator.
_COMPARISON_OPS = {
    "greaterthan": ">",
    "lessthan": "<",
    "greaterthanorequal": ">=",
    "lessthanorequal": "<=",
    "equal": "=",
    "notequal": "!=",
}


class SwrlToShaclTranspiler:
    """Convert SWRL rule dicts into :class:`ShaclRule` instances."""

    def __init__(self, ontology: Optional[Dict] = None) -> None:
        self._ontology = ontology or {}
        # Canonical URI map so class/property URIs resolve consistently across
        # the whole SHACL rule layer (parity guarantee).
        self._uri_map = build_uri_map(self._ontology)
        self._base_uri = self._ontology.get("base_uri", "")

    # -- Public API -------------------------------------------------------

    def transpile_many(self, rules: List[Dict]) -> List[ShaclRule]:
        return [self.transpile(r) for r in rules]

    def transpile(self, swrl_rule: Dict) -> ShaclRule:
        """Transpile one SWRL rule dict into a :class:`ShaclRule`."""
        name = swrl_rule.get("name", "unnamed")
        antecedent = swrl_rule.get("antecedent", "")
        consequent = swrl_rule.get("consequent", "")

        part = RuleExpressionParser.partition_rule_atoms(swrl_rule)
        rule_type = self._classify(part)
        target_uri = self._target_class_uri(part)
        construct = self._build_construct(antecedent, consequent)
        turtle = self._build_turtle(
            self._shape_id(name), rule_type, target_uri, construct, consequent
        )

        return ShaclRule(
            id=self._shape_id(name),
            name=name,
            rule_type=rule_type,
            target_class_uri=target_uri,
            antecedent=antecedent,
            consequent=consequent,
            construct_sparql=construct,
            turtle=turtle,
            enabled=swrl_rule.get("enabled", True),
            description=swrl_rule.get("description", ""),
        )

    # -- Classification ---------------------------------------------------

    @staticmethod
    def _classify(part) -> str:
        """``sh:TripleRule`` for a simple assertion, else ``sh:SPARQLRule``."""
        if part is None:
            return SPARQL_RULE
        if part.builtin_atoms or part.negated_atoms:
            return SPARQL_RULE
        # A single-head, single-anchor rule maps to one sh:TripleRule.
        if len(part.consequent_atoms) == 1 and len(part.class_atoms) == 1:
            return TRIPLE_RULE
        return SPARQL_RULE

    def _target_class_uri(self, part) -> str:
        if part is None or not part.class_atoms:
            return ""
        primary = part.class_atoms[0]["name"]
        return RuleExpressionParser.resolve_uri(primary, self._base_uri, self._uri_map)

    def _shape_id(self, name: str) -> str:
        safe = "".join(c if c.isalnum() or c == "_" else "_" for c in name) or "rule"
        return f"rule_{safe}"

    # -- SPARQL CONSTRUCT rendering ---------------------------------------

    def _build_construct(self, antecedent: str, consequent: str) -> str:
        """Render a SPARQL ``CONSTRUCT { head } WHERE { body }`` string.

        Built-ins become ``FILTER`` clauses (comparison ops) or SPARQL
        comments (non-comparison), and negated atoms become
        ``FILTER NOT EXISTS`` blocks.  This string is the canonical,
        human-readable SHACL-AF rule body; execution runs through the flat
        SQL backend, not this text.
        """
        head_atoms = RuleExpressionParser.parse_atoms(consequent)
        body_atoms = RuleExpressionParser.parse_atoms(antecedent)

        head = [self._atom_triple(a) for a in head_atoms if not a.get("builtin")]
        body: List[str] = []
        for atom in body_atoms:
            if atom.get("negated"):
                body.append(f"FILTER NOT EXISTS {{ {self._atom_triple(atom)} }}")
            elif atom.get("builtin"):
                flt = self._builtin_filter(atom)
                if flt:
                    body.append(flt)
            else:
                body.append(self._atom_triple(atom))

        head_str = " ".join(t for t in head if t)
        body_str = " ".join(t for t in body if t)
        return f"CONSTRUCT {{ {head_str} }} WHERE {{ {body_str} }}"

    def _atom_triple(self, atom: Dict) -> str:
        """Render a class/property atom as a SPARQL triple pattern."""
        name = atom.get("name", "")
        args = atom.get("args", [])
        if atom.get("arity") == 1 and args:
            return f"{args[0]} a <{self._uri(name)}> ."
        if atom.get("arity") == 2 and len(args) >= 2:
            return f"{args[0]} <{self._uri(name)}> {self._term(args[1])} ."
        return ""

    @staticmethod
    def _builtin_filter(atom: Dict) -> str:
        op = _COMPARISON_OPS.get(atom.get("name", "").lower())
        args = atom.get("args", [])
        if op and len(args) >= 2:
            return (
                f"FILTER ({SwrlToShaclTranspiler._term(args[0])} {op} "
                f"{SwrlToShaclTranspiler._term(args[1])})"
            )
        return f"# builtin {atom.get('name', '')}({', '.join(args)})"

    @staticmethod
    def _term(token: str) -> str:
        """Render a SWRL argument as a SPARQL term (variable or literal)."""
        if not token:
            return '""'
        if token.startswith("?"):
            return token
        if RuleBuiltinRegistry.is_literal(token):
            # Numeric literals pass through; quoted strings stay quoted.
            stripped = token.strip()
            if stripped and (stripped[0].isdigit() or stripped[0] in "+-"):
                return stripped
            return f'"{stripped.strip(chr(34))}"'
        return f'"{token}"'

    def _uri(self, name: str) -> str:
        return RuleExpressionParser.resolve_uri(name, self._base_uri, self._uri_map)

    # -- SHACL-AF Turtle rendering ----------------------------------------

    def _build_turtle(
        self,
        shape_id: str,
        rule_type: str,
        target_uri: str,
        construct: str,
        consequent: str,
    ) -> str:
        """Render the canonical SHACL-AF Turtle for a rule shape."""
        lines = [
            "@prefix sh: <http://www.w3.org/ns/shacl#> .",
            "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .",
            f"@prefix ex: <{self._base_uri or 'http://example.org/ontology#'}> .",
            "",
            f"ex:{shape_id} a sh:NodeShape ;",
        ]
        if target_uri:
            lines.append(f"    sh:targetClass <{target_uri}> ;")

        if rule_type == TRIPLE_RULE:
            head = RuleExpressionParser.parse_atoms(consequent)
            atom = head[0] if head else {}
            if atom.get("arity") == 1:
                lines.append("    sh:rule [")
                lines.append("        a sh:TripleRule ;")
                lines.append("        sh:subject sh:this ;")
                lines.append(f"        sh:predicate <{RDF_TYPE}> ;")
                lines.append(f"        sh:object <{self._uri(atom.get('name', ''))}> ;")
                lines.append("    ] .")
            else:
                lines.append("    sh:rule [")
                lines.append("        a sh:TripleRule ;")
                lines.append("        sh:subject sh:this ;")
                lines.append(f"        sh:predicate <{self._uri(atom.get('name', ''))}> ;")
                lines.append("        sh:object sh:this ;")
                lines.append("    ] .")
        else:
            lines.append("    sh:rule [")
            lines.append("        a sh:SPARQLRule ;")
            lines.append(f'        sh:construct """{construct}""" ;')
            lines.append("    ] .")

        return "\n".join(lines) + "\n"
