"""SWRL expression parser — shared parsing utilities for SWRL translators.

Extracts atoms from SWRL rule strings and provides URI resolution,
violation-subject detection, and variable connectivity analysis used
by both the SQL and Cypher translation paths.
"""

from typing import Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.constants import SWRL_ATOM_RE, NEGATED_ATOM_RE
from back.core.reasoning.models import SWRLAtomPartition

logger = get_logger(__name__)


class SWRLParser:
    """Parse and analyse SWRL rule expressions.

    All methods are static — no instance state is needed.
    """

    @staticmethod
    def partition_rule_atoms(rule: Dict) -> Optional[SWRLAtomPartition]:
        """Parse and partition a SWRL rule's atoms for translation.

        Returns ``None`` if the antecedent or consequent is empty, or if
        the antecedent has no class atoms.
        """
        antecedent = rule.get("antecedent", "")
        consequent = rule.get("consequent", "")
        ante_atoms = SWRLParser.parse_atoms(antecedent)
        cons_atoms = SWRLParser.parse_atoms(consequent)
        if not ante_atoms or not cons_atoms:
            return None

        class_atoms = [
            a
            for a in ante_atoms
            if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")
        ]
        prop_atoms = [
            a
            for a in ante_atoms
            if a["arity"] == 2 and not a.get("builtin") and not a.get("negated")
        ]
        builtin_atoms = [
            a for a in ante_atoms if a.get("builtin") and not a.get("negated")
        ]
        negated_atoms = [a for a in ante_atoms if a.get("negated")]
        if not class_atoms:
            return None

        return SWRLAtomPartition(
            class_atoms=class_atoms,
            prop_atoms=prop_atoms,
            builtin_atoms=builtin_atoms,
            negated_atoms=negated_atoms,
            consequent_atoms=cons_atoms,
        )

    @staticmethod
    def parse_atoms(expression: str) -> List[Dict]:
        """Parse a SWRL expression into a list of atom dicts.

        Supported forms:

        - ``ClassName(?x)`` — class atom (arity 1)
        - ``propName(?x, ?y)`` — property atom (arity 2)
        - ``greaterThan(?x, 18)`` — built-in atom (arity varies)
        - ``not(propName(?x, ?y))`` — negated atom

        Each returned dict has ``name``, ``args``, ``arity``, ``negated``
        (bool), and ``builtin`` (bool).
        """
        from back.core.reasoning.SWRLBuiltinRegistry import SWRLBuiltinRegistry

        atoms: List[Dict] = []
        remaining = expression

        for m in NEGATED_ATOM_RE.finditer(remaining):
            name = m.group(1)
            raw_args = [a.strip() for a in m.group(2).split(",")]
            atoms.append(
                {
                    "name": name,
                    "args": raw_args,
                    "arity": len(raw_args),
                    "negated": True,
                    "builtin": SWRLBuiltinRegistry.is_builtin(name),
                }
            )

        cleaned = NEGATED_ATOM_RE.sub("", remaining)

        for m in SWRL_ATOM_RE.finditer(cleaned):
            name = m.group(1)
            if name.lower() == "not":
                continue
            raw_args = [a.strip() for a in m.group(2).split(",")]
            atoms.append(
                {
                    "name": name,
                    "args": raw_args,
                    "arity": len(raw_args),
                    "negated": False,
                    "builtin": SWRLBuiltinRegistry.is_builtin(name),
                }
            )
        return atoms

    @staticmethod
    def resolve_uri(
        name: str, base_uri: str, uri_map: Optional[Dict[str, str]] = None
    ) -> str:
        """Resolve a short name to a full URI.

        Resolution order:
        1. Already a full URI — return as-is.
        2. Found (case-insensitive) in *uri_map* — return canonical URI.
        3. Fall back to ``base_uri + name``.
        """
        if name.startswith("http://") or name.startswith("https://"):
            return name
        if uri_map:
            resolved = uri_map.get(name.lower())
            if resolved:
                return resolved
        if not base_uri:
            return name
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        return base_uri + sep + name

    @staticmethod
    def determine_violation_subject(
        cons_atoms: List[Dict],
        ante_class_atoms: List[Dict],
    ) -> Optional[str]:
        """Return the SWRL variable whose instances are reported as violations.

        Prefers the subject of the first consequent property atom, then
        the first consequent class atom variable, then the first antecedent
        class atom.
        """
        for atom in cons_atoms:
            if atom["arity"] == 2:
                return atom["args"][0]
        for atom in cons_atoms:
            if atom["arity"] == 1:
                return atom["args"][0]
        if ante_class_atoms:
            return ante_class_atoms[0]["args"][0]
        return None

    @staticmethod
    def find_connected_vars(start_var: str, prop_atoms: List[Dict]) -> set:
        """Return all SWRL variables reachable from *start_var* through property atoms."""
        connected = {start_var}
        changed = True
        while changed:
            changed = False
            for pa in prop_atoms:
                s, o = pa["args"][0], pa["args"][1]
                if s in connected and o not in connected:
                    connected.add(o)
                    changed = True
                elif o in connected and s not in connected:
                    connected.add(s)
                    changed = True
        return connected

    @staticmethod
    def order_connected_props(
        violation_var: str,
        connected_props: List[Dict],
    ) -> List[Dict]:
        """Order property atoms so each has at least one previously-bound variable."""
        ordered: List[Dict] = []
        bound = {violation_var}
        remaining = list(connected_props)
        while remaining:
            found = False
            for i, p in enumerate(remaining):
                s, o = p["args"][0], p["args"][1]
                if s in bound or o in bound:
                    ordered.append(remaining.pop(i))
                    bound.add(s)
                    bound.add(o)
                    found = True
                    break
            if not found:
                ordered.extend(remaining)
                break
        return ordered
