"""Translate SHACL-AF rules into SQL on the flat triple-store table.

A SHACL-AF ``sh:SPARQLRule`` CONSTRUCT (or ``sh:TripleRule``) over the flat
``(subject, predicate, object)`` table is semantically the same graph pattern
as the IF/THEN rule it was transpiled from.  Rather than re-implement the
mature pattern-to-SQL logic (built-ins, closed-world negation, focus
restriction, multi-hop joins, materialisation), this translator delegates to
the backend-provided flat-triple SQL backend, feeding it the rule's preserved
body.  This keeps capability parity while making the SHACL-AF rule the
canonical executable unit.

A future fully SHACL-native translator can replace the delegation here without
touching the engine or its callers.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from back.core.reasoning.shacl_rules.ShaclRule import ShaclRule


class ShaclRuleSqlTranslator:
    """Build SQL for SHACL-AF rules via a flat-triple SQL backend."""

    def __init__(self, flat_translator: Any) -> None:
        #: The flat ``(subject, predicate, object)`` SQL backend
        #: (``ShaclFlatSqlBackend`` or a backend-supplied equivalent).
        self._flat = flat_translator

    @staticmethod
    def _params(rule: ShaclRule, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """Assemble the flat-backend parameter dict from a rule + context."""
        params: Dict[str, Any] = {
            "antecedent": rule.antecedent,
            "consequent": rule.consequent,
            "base_uri": ctx.get("base_uri", ""),
            "uri_map": ctx.get("uri_map") or {},
        }
        if ctx.get("focus"):
            params["focus"] = ctx["focus"]
        return params

    def build_inference_sql(
        self, table: str, rule: ShaclRule, ctx: Dict[str, Any]
    ) -> Optional[str]:
        return self._flat.build_inference_sql(table, self._params(rule, ctx))

    def build_materialization_sql(
        self, table: str, rule: ShaclRule, ctx: Dict[str, Any]
    ) -> Optional[str]:
        return self._flat.build_materialization_sql(table, self._params(rule, ctx))

    def build_violation_sql(
        self, table: str, rule: ShaclRule, ctx: Dict[str, Any]
    ) -> Optional[str]:
        return self._flat.build_violation_sql(table, self._params(rule, ctx))

    def build_antecedent_count_sql(
        self, table: str, rule: ShaclRule, ctx: Dict[str, Any]
    ) -> Optional[str]:
        return self._flat.build_antecedent_count_sql(table, self._params(rule, ctx))
