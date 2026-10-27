"""Translate SWRL rules to Cypher for the flat-triple Neo4j model.

**STATUS: scaffolding only.** This class exists so the architecture is in
place (``GraphDBBackend.get_query_translator`` returns this for Cypher
flat-model engines) and so the reasoning UI does not crash when Neo4j is
the active engine. **Actual SWRL → Cypher translation is not implemented
yet** — every method here returns ``None`` and logs a clear warning.

Why this is scaffolded rather than fully implemented:

- The SQL counterpart (:class:`SWRLSQLTranslator`) is ~730 lines of
  careful logic for builtins, negation, variable bindings, arity-1 vs
  arity-2 atoms, IRI resolution, and antecedent-vs-consequent assembly.
  A faithful Cypher port is its own piece of work, deserving a dedicated
  PR with its own test suite — not bundled into the engine-skeleton PR.
- Falling back to ``None`` is what the reasoning engine treats as
  "no work to do", so the UI surfaces "0 violations / 0 inferences"
  cleanly rather than crashing.

When the dedicated PR lands it should mirror the public interface
below — same method names, same return types — so callers do not change.
"""

import re
from typing import Any, Dict, Optional

from back.core.errors import ValidationError
from back.core.graphdb.neo4j.graph_model import _sanitise_ident, label_from_class_uri, reltype_from_predicate
from back.core.logging import get_logger

logger = get_logger(__name__)


class SWRLFlatCypherTranslator:
    """Cypher counterpart of :class:`SWRLSQLTranslator` — scaffolded.

    Parameters
    ----------
    node_label:
        Neo4j label suffix used for the per-store triple nodes, e.g.
        ``"<table_name>"``. The full triple pattern is
        ``(:Triple:{node_label} {subject, predicate, object})``.
    """

    def __init__(self, node_label: str = "") -> None:
        self.node_label = node_label

    _ATOM = re.compile(r"^\s*([:\w./#-]+)\s*\((.*)\)\s*$")

    @classmethod
    def _atoms(cls, expression: str) -> list[tuple[str, list[str]]]:
        """Parse graph-shaped SWRL atoms; reject unsupported builtins clearly."""
        atoms = []
        for raw in re.split(r"\s*(?:∧|AND)\s*", expression or ""):
            match = cls._ATOM.match(raw)
            if not match:
                raise ValidationError(
                    f"Neo4j SWRL does not support atom '{raw}'; "
                    "only class and object-property atoms are supported."
                )
            args = [arg.strip() for arg in match.group(2).split(",")]
            if len(args) not in (1, 2) or any(not arg for arg in args):
                raise ValidationError(f"Neo4j SWRL cannot parse atom '{raw}'")
            atoms.append((match.group(1), args))
        if not atoms:
            raise ValidationError("Neo4j SWRL rule has no antecedent atoms")
        return atoms

    @staticmethod
    def _var(arg: str) -> str:
        if not arg.startswith("?"):
            raise ValidationError(
                f"Neo4j SWRL constant argument '{arg}' is unsupported"
            )
        return "v_" + _sanitise_ident(arg[1:])

    def _build_inference(self, table: str, params: Dict[str, Any]) -> str:
        """Build an injection-safe, read-only Cypher inference query."""
        uri_map = params.get("uri_map") or {}
        label = self.node_label or _sanitise_ident(table)
        nodes: Dict[str, list[str]] = {}
        edges: list[tuple[str, str, str]] = []
        for name, args in self._atoms(params.get("antecedent", "")):
            uri = str(uri_map.get(name.lower(), name))
            if len(args) == 1:
                nodes.setdefault(self._var(args[0]), []).append(label_from_class_uri(uri))
            else:
                edges.append((
                    self._var(args[0]), reltype_from_predicate(uri), self._var(args[1])
                ))
        patterns = [
            f"({var}:`{label}`{''.join(f':`{cls}`' for cls in classes)})"
            for var, classes in nodes.items()
        ]
        known = set(nodes)
        for source, predicate, target in edges:
            for var in (source, target):
                if var not in known:
                    patterns.append(f"({var}:`{label}`)")
                    known.add(var)
            patterns.append(f"({source})-[:`{predicate}`]->({target})")
        consequent = self._atoms(params.get("consequent", ""))
        if len(consequent) != 1 or len(consequent[0][1]) != 2:
            raise ValidationError(
                "Neo4j SWRL supports one object-property consequent per rule"
            )
        name, args = consequent[0]
        predicate = str(uri_map.get(name.lower(), name))
        source, target = self._var(args[0]), self._var(args[1])
        escaped_predicate = predicate.replace("'", "\\'")
        return (
            f"MATCH {', '.join(patterns)} "
            f"WHERE NOT ({source})-[:`{reltype_from_predicate(predicate)}`]->({target}) "
            f"RETURN DISTINCT {source}.uri AS subject, "
            f"'{escaped_predicate}' AS predicate, "
            f"{target}.uri AS object"
        )

    # ------------------------------------------------------------------
    #  Public interface — mirrors SWRLSQLTranslator.
    #  All methods return None for now (graceful no-op).
    # ------------------------------------------------------------------

    def build_violation_cypher(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Build Cypher that finds subjects violating a SWRL rule.

        Returns ``None`` — Cypher SWRL violation queries are not
        translated in this version. Reasoning on Neo4j will report
        zero violations until the dedicated translator PR lands.
        """
        logger.warning(
            "SWRLFlatCypherTranslator.build_violation_cypher: "
            "SWRL→Cypher translation is not implemented yet. "
            "Returning None (rule produces no violations on Neo4j)."
        )
        return None

    def build_antecedent_count_cypher(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Cypher that counts how often a SWRL antecedent matches.

        Returns ``None`` — see class docstring.
        """
        logger.warning(
            "SWRLFlatCypherTranslator.build_antecedent_count_cypher: "
            "not implemented yet. Returning None."
        )
        return None

    def build_materialization_cypher(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Cypher that materialises inferred triples produced by a rule.

        Returns ``None`` — see class docstring.
        """
        logger.warning(
            "SWRLFlatCypherTranslator.build_materialization_cypher: "
            "not implemented yet. Returning None (no inferences materialised)."
        )
        return None

    def build_inference_cypher(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        """Alias / variant of :meth:`build_materialization_cypher`.

        Returns ``None`` — see class docstring.
        """
        return self._build_inference(table, params)

    # ------------------------------------------------------------------
    #  Compatibility shims — the reasoning engine calls the SQL names.
    #  Forward them to the Cypher methods so the engine can use either
    #  translator without branching.
    # ------------------------------------------------------------------

    def build_violation_sql(self, table: str, params: Dict[str, Any]) -> Optional[str]:
        return self.build_violation_cypher(table, params)

    def build_antecedent_count_sql(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        return self.build_antecedent_count_cypher(table, params)

    def build_materialization_sql(
        self, table: str, params: Dict[str, Any]
    ) -> Optional[str]:
        return self.build_materialization_cypher(table, params)

    def build_inference_sql(self, table: str, params: Dict[str, Any]) -> Optional[str]:
        return self.build_inference_cypher(table, params)
