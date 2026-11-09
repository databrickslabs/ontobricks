"""SHACL-AF rule execution engine.

The execution engine behind the IF/THEN business-rules UI.  Rules are
transpiled to canonical
SHACL-AF rules (:class:`IfThenToShaclTranspiler`) and executed at scale against
the triple-store backend via :class:`ShaclRuleSqlTranslator` (SQL on the flat
``(subject, predicate, object)`` table — Delta views / Lakebase Postgres).

The emitted :class:`InferredTriple` provenance (``if_then:<rule>``) and the result
``stats`` keys (``phase = "if_then"``, ``rules_count`` ...) are intentionally
identical to the legacy engine so the UI, MCP tools, and Data-Quality pipeline
observe no change.
"""

import time
from typing import Any, Dict, List, Optional

from back.core.graphdb.interfaces.GraphBackend import GraphBackend
from back.core.logging import get_logger
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.shacl_rules.ShaclRule import ShaclRule
from back.core.reasoning.shacl_rules.ShaclRuleSqlTranslator import (
    ShaclRuleSqlTranslator,
)
from back.core.reasoning.shacl_rules.IfThenToShaclTranspiler import IfThenToShaclTranspiler

logger = get_logger(__name__)


class ShaclRuleEngine:
    """Execute SHACL-AF rules (transpiled from IF-THEN) against a triple store."""

    def __init__(self, ontology: Optional[Dict[str, Any]] = None) -> None:
        self._ontology = ontology or {}
        self._transpiler = IfThenToShaclTranspiler(ontology=self._ontology)

    def _build_uri_map(self) -> Dict[str, str]:
        """Canonical class/property URI map (reused by the DQ pipeline)."""
        return self._transpiler._uri_map

    def execute_rules(
        self,
        rules: List[Dict],
        store: Any,
        table_name: str,
        materialize: bool = False,
        inference_limit: Optional[int] = None,
        progress_callback: Optional[Any] = None,
        focus: Optional[Dict[str, Any]] = None,
    ) -> ReasoningResult:
        """Run all rules and collect inferred triples.

        Same arguments and result shape as the former IF-THEN engine, so callers
        and the result contract (provenance / stats) are unchanged.
        """
        t0 = time.time()
        result = ReasoningResult()

        base_uri = self._ontology.get("base_uri", "")
        uri_map = self._build_uri_map()
        ctx = {"base_uri": base_uri, "uri_map": uri_map}
        if focus:
            ctx["focus"] = focus

        translator = ShaclRuleSqlTranslator(self._get_flat_translator(store, table_name))
        errors = 0
        failed: List[str] = []

        enabled_rules = [r for r in rules if r.get("enabled", True)]
        total = len(enabled_rules)

        for idx, rule in enumerate(enabled_rules):
            name = rule.get("name", "unnamed")
            if progress_callback:
                try:
                    progress_callback(idx, total, name)
                except Exception:
                    pass
            try:
                shacl_rule = self._transpiler.transpile(rule)
                self._infer_rule(
                    translator,
                    store,
                    table_name,
                    shacl_rule,
                    ctx,
                    result,
                    inference_limit=inference_limit,
                )
                if materialize:
                    self._materialize_rule(
                        translator, store, table_name, shacl_rule, ctx, result
                    )
            except Exception as e:
                logger.error("SHACL rule '%s' failed: %s", name, e)
                errors += 1
                failed.append(f"{name}: {e}")

        duration = time.time() - t0
        result.stats = {
            "phase": "if_then",
            "engine": "shacl-af",
            "rules_count": total,
            "rules_total": len(rules),
            "inferred_count": len(result.inferred_triples),
            "errors": errors,
            "error_details": "; ".join(failed)[:2000],
            "duration_seconds": round(duration, 3),
        }
        logger.info(
            "SHACL rule engine: %d/%d rules enabled, %d inferred, %d errors (%.2fs)",
            total,
            len(rules),
            len(result.inferred_triples),
            errors,
            duration,
        )
        return result

    def _infer_rule(
        self,
        translator: ShaclRuleSqlTranslator,
        store: Any,
        table_name: str,
        rule: ShaclRule,
        ctx: Dict[str, Any],
        result: ReasoningResult,
        inference_limit: Optional[int] = None,
    ) -> None:
        """Execute the inference SELECT for a single rule."""
        query = translator.build_inference_sql(
            store.sql_table_reference(table_name), rule, ctx
        )
        if not query:
            logger.warning("Could not build inference query for rule: %s", rule.name)
            return

        if inference_limit is not None and inference_limit > 0:
            query = query.rstrip().rstrip(";") + f"\nLIMIT {inference_limit}"

        t_rule = time.time()
        count = 0
        execute = store.execute_query
        if getattr(store, "query_dialect", "") == "cypher":
            execute = getattr(store, "execute_reasoning_query", execute)
        rows = execute(query) or []
        for row in rows:
            result.inferred_triples.append(
                InferredTriple(
                    subject=row.get("subject", ""),
                    predicate=row.get("predicate", ""),
                    object=row.get("object", ""),
                    provenance=f"if_then:{rule.name}",
                    rule_name=rule.name,
                )
            )
            count += 1
        logger.info(
            "SHACL rule inference '%s' (%s): %d triples (%.2fs)",
            rule.name,
            rule.rule_type,
            count,
            time.time() - t_rule,
        )

    def _materialize_rule(
        self,
        translator: ShaclRuleSqlTranslator,
        store: Any,
        table_name: str,
        rule: ShaclRule,
        ctx: Dict[str, Any],
        result: ReasoningResult,
    ) -> None:
        """Execute materialisation (INSERT) for a single rule."""
        try:
            t_rule = time.time()
            sql = translator.build_materialization_sql(
                store.sql_table_reference(table_name), rule, ctx
            )
            if sql:
                for stmt in sql.split(";\n"):
                    stmt = stmt.strip()
                    if stmt:
                        store.execute_query(stmt)
                result.inferred_triples.append(
                    InferredTriple(
                        subject="(batch)",
                        predicate="if_then:materialized",
                        object=rule.name,
                        provenance=f"if_then:{rule.name}",
                        rule_name=rule.name,
                    )
                )
            logger.info(
                "SHACL rule materialise '%s': %.2fs", rule.name, time.time() - t_rule
            )
        except Exception as e:
            logger.error("Materialisation for rule '%s' failed: %s", rule.name, e)

    @staticmethod
    def _get_flat_translator(store: Any, table_name: str = "") -> Any:
        """Return the flat-triple SQL backend for *store*.

        Backends override :meth:`GraphBackend.get_query_translator` to plug in
        their own translator (e.g. a future Cypher engine); otherwise the
        default Spark/Postgres flat-triple SQL backend is used.
        """
        if isinstance(store, GraphBackend):
            return store.get_query_translator(table_name)
        from back.core.reasoning.shacl_rules.ShaclFlatSqlBackend import (
            ShaclFlatSqlBackend,
        )

        return ShaclFlatSqlBackend()
