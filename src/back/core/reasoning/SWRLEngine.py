"""SWRL rule execution engine.

Translates SWRL rules to SQL via the backend-provided translator and
executes inference (producing inferred triples) against the active
triple store.  All currently supported backends are SQL-based; a
future Cypher / Gremlin engine would supply its own translator via
:meth:`GraphDBBackend.get_query_translator`.

Violation detection for SWRL rules is handled separately by the
Data Quality runner (see ``run_sql_checks`` in
``back.objects.digitaltwin``).
"""

import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.graphdb.GraphDBBackend import GraphDBBackend
from back.core.w3c.rdf_utils import uri_local_name
from back.core.reasoning.models import InferredTriple, ReasoningResult

logger = get_logger(__name__)


class SWRLEngine:
    """Execute SWRL rules against a triple-store backend."""

    def __init__(self, ontology: Optional[Dict[str, Any]] = None) -> None:
        self._ontology = ontology or {}

    def execute_rules(
        self,
        rules: List[Dict],
        store: Any,
        table_name: str,
        materialize: bool = False,
        inference_limit: Optional[int] = None,
        progress_callback: Optional[Any] = None,
    ) -> ReasoningResult:
        """Run all SWRL rules and collect inferred triples.

        Args:
            rules: List of rule dicts with ``name``, ``antecedent``,
                   ``consequent``, and optionally ``description``.
            store: A :class:`GraphDBBackend` instance.
            table_name: The logical triple-store table/graph name.
            materialize: If True, also insert inferred triples into the store.
            inference_limit: Max inferred triples per rule (None = unlimited).
            progress_callback: Optional ``(idx, total, rule_name)`` callable
                for progress reporting.
        """
        t0 = time.time()
        result = ReasoningResult()

        base_uri = self._ontology.get("base_uri", "")
        uri_map = self._build_uri_map()

        translator = self._get_translator(store, table_name)
        errors = 0

        enabled_rules = [r for r in rules if r.get("enabled", True)]
        total = len(enabled_rules)

        for idx, rule in enumerate(enabled_rules):
            name = rule.get("name", "unnamed")
            if progress_callback:
                try:
                    progress_callback(idx, total, name)
                except Exception:
                    pass
            params = {
                "antecedent": rule.get("antecedent", ""),
                "consequent": rule.get("consequent", ""),
                "base_uri": base_uri,
                "uri_map": uri_map,
            }

            try:
                self._infer_rule(
                    translator,
                    store,
                    table_name,
                    params,
                    name,
                    result,
                    inference_limit=inference_limit,
                )
                if materialize:
                    self._materialize_rule(
                        translator,
                        store,
                        table_name,
                        params,
                        name,
                        result,
                    )
            except Exception as e:
                logger.error("SWRL rule '%s' failed: %s", name, e)
                errors += 1

        duration = time.time() - t0
        result.stats = {
            "phase": "swrl",
            "rules_count": total,
            "rules_total": len(rules),
            "inferred_count": len(result.inferred_triples),
            "errors": errors,
            "duration_seconds": round(duration, 3),
        }
        logger.info(
            "SWRL engine: %d/%d rules enabled, %d inferred, %d errors (%.2fs)",
            total,
            len(rules),
            len(result.inferred_triples),
            errors,
            duration,
        )
        return result

    def _infer_rule(
        self,
        translator,
        store,
        table_name,
        params,
        rule_name,
        result,
        inference_limit: Optional[int] = None,
    ):
        """Execute inference SELECT for a single rule."""
        query = translator.build_inference_sql(
            store.sql_table_reference(table_name), params
        )

        if not query:
            logger.warning(
                "Could not build inference query for SWRL rule: %s", rule_name
            )
            return

        if inference_limit is not None and inference_limit > 0:
            query = query.rstrip().rstrip(";") + f"\nLIMIT {inference_limit}"

        t_rule = time.time()
        count = 0
        rows = store.execute_query(query) or []
        for row in rows:
            result.inferred_triples.append(
                InferredTriple(
                    subject=row.get("subject", ""),
                    predicate=row.get("predicate", ""),
                    object=row.get("object", ""),
                    provenance=f"swrl:{rule_name}",
                    rule_name=rule_name,
                )
            )
            count += 1
        logger.info(
            "SWRL inference '%s': %d triples (%.2fs)",
            rule_name,
            count,
            time.time() - t_rule,
        )

    def _materialize_rule(
        self, translator, store, table_name, params, rule_name, result
    ):
        """Execute materialisation for a single rule."""
        try:
            t_rule = time.time()
            sql = translator.build_materialization_sql(
                store.sql_table_reference(table_name), params
            )
            if sql:
                for stmt in sql.split(";\n"):
                    stmt = stmt.strip()
                    if stmt:
                        store.execute_query(stmt)
                result.inferred_triples.append(
                    InferredTriple(
                        subject="(batch)",
                        predicate="swrl:materialized",
                        object=rule_name,
                        provenance=f"swrl:{rule_name}",
                        rule_name=rule_name,
                    )
                )
            logger.info("SWRL materialise '%s': %.2fs", rule_name, time.time() - t_rule)
        except Exception as e:
            logger.error("Materialisation for rule '%s' failed: %s", rule_name, e)

    def _build_uri_map(self) -> Dict[str, str]:
        """Build a lowercase-name → URI map from ontology classes/properties.

        Property URIs are normalised to the **data namespace** (``base_uri``
        with a trailing ``/``) so they match the predicates written by the
        R2RML generator when syncing data to the triple store.  Class URIs
        keep their original ``#`` separator because ``rdf:type`` objects in
        the store use the ontology class URI as-is.
        """
        uri_map: Dict[str, str] = {}
        base_uri = self._ontology.get("base_uri", "")
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"

        data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""

        for cls in self._ontology.get("classes", []):
            name = cls.get("name", "") or cls.get("localName", "")
            uri = cls.get("uri", "")
            if not uri and name:
                uri = base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        for prop in self._ontology.get("properties", []):
            name = prop.get("name", "") or prop.get("localName", "")
            uri = prop.get("uri", "")
            if data_ns and uri and not uri.startswith(data_ns):
                local = uri_local_name(uri)
                uri = data_ns + local
            elif not uri and name:
                uri = data_ns + name if data_ns else base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        logger.debug(
            "SWRL uri_map (%d entries, data_ns=%s): %s",
            len(uri_map),
            data_ns,
            {k: v for k, v in list(uri_map.items())[:10]},
        )
        return uri_map

    @staticmethod
    def _get_translator(store, table_name: str = ""):
        """Return the SWRL→SQL translator for the backend.

        Backends override :meth:`GraphDBBackend.get_query_translator` to plug
        in their own translator (e.g. a future Cypher engine).
        """
        if isinstance(store, GraphDBBackend):
            return store.get_query_translator(table_name)
        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator

        return SWRLSQLTranslator()
