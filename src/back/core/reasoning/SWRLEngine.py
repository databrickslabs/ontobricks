"""SWRL rule execution engine.

Selects the appropriate translator (SQL or Cypher) based on the
triple-store backend and executes inference (producing inferred triples).

Violation detection for SWRL rules is handled separately by the
Data Quality runner (see ``run_sql_checks`` / ``run_graph_checks``
in ``back.objects.digitaltwin``).
"""
import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
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
    ) -> ReasoningResult:
        """Run all SWRL rules and collect inferred triples.

        Each rule's antecedent is matched against the triple store.
        Where the consequent is missing, the engine returns the would-be
        triples as ``InferredTriple`` objects.  Optionally, these triples
        can also be written into the store (materialised).

        Args:
            rules: List of rule dicts with ``name``, ``antecedent``,
                   ``consequent``, and optionally ``description``.
            store: A :class:`TripleStoreBackend` instance.
            table_name: The logical triple-store table/graph name.
            materialize: If True, also insert inferred triples into the store.

        Returns:
            :class:`ReasoningResult` with inferred triples.
        """
        t0 = time.time()
        result = ReasoningResult()
        is_graph = self._is_graph_backend(store)
        is_ladybug = self._is_ladybug_backend(store)
        uses_cypher = is_graph or is_ladybug

        base_uri = self._ontology.get("base_uri", "")
        uri_map = self._build_uri_map()

        translator = self._get_translator(store, table_name)
        errors = 0

        for rule in rules:
            if not rule.get("enabled", True):
                continue
            name = rule.get("name", "unnamed")
            params = {
                "antecedent": rule.get("antecedent", ""),
                "consequent": rule.get("consequent", ""),
                "base_uri": base_uri,
                "uri_map": uri_map,
            }

            try:
                self._infer_rule(
                    translator, store, table_name, params, name,
                    uses_cypher, result,
                )
                if materialize:
                    self._materialize_rule(
                        translator, store, table_name, params, name,
                        uses_cypher, result,
                    )
            except Exception as e:
                logger.error("SWRL rule '%s' failed: %s", name, e)
                errors += 1

        duration = time.time() - t0
        result.stats = {
            "phase": "swrl",
            "rules_count": len(rules),
            "inferred_count": len(result.inferred_triples),
            "errors": errors,
            "duration_seconds": round(duration, 3),
        }
        logger.info(
            "SWRL engine: %d rules, %d inferred, %d errors (%.2fs)",
            len(rules), len(result.inferred_triples), errors, duration,
        )
        return result

    def _infer_rule(
        self, translator, store, table_name, params, rule_name,
        uses_cypher, result,
    ):
        """Execute inference SELECT for a single rule."""
        if uses_cypher:
            query = translator.build_inference_query(params)
        else:
            query = translator.build_inference_sql(table_name, params)

        if not query:
            logger.warning("Could not build inference query for SWRL rule: %s", rule_name)
            return

        if uses_cypher:
            conn = store._get_connection()
            r = conn.execute(query)
            for row in r:
                result.inferred_triples.append(InferredTriple(
                    subject=str(row[0]) if row[0] else "",
                    predicate=str(row[1]) if row[1] else "",
                    object=str(row[2]) if row[2] else "",
                    provenance=f"swrl:{rule_name}",
                    rule_name=rule_name,
                ))
        else:
            rows = store.execute_query(query)
            for row in rows:
                result.inferred_triples.append(InferredTriple(
                    subject=row.get("subject", ""),
                    predicate=row.get("predicate", ""),
                    object=row.get("object", ""),
                    provenance=f"swrl:{rule_name}",
                    rule_name=rule_name,
                ))

    def _materialize_rule(
        self, translator, store, table_name, params, rule_name, uses_cypher, result
    ):
        """Execute materialisation for a single rule."""
        try:
            if uses_cypher:
                query = translator.build_materialization_query(params)
                if query:
                    conn = store._get_connection()
                    conn.execute(query)
                    result.inferred_triples.append(InferredTriple(
                        subject="(batch)",
                        predicate="swrl:materialized",
                        object=rule_name,
                        provenance=f"swrl:{rule_name}",
                        rule_name=rule_name,
                    ))
            else:
                sql = translator.build_materialization_sql(table_name, params)
                if sql:
                    for stmt in sql.split(";\n"):
                        stmt = stmt.strip()
                        if stmt:
                            store.execute_query(stmt)
                    result.inferred_triples.append(InferredTriple(
                        subject="(batch)",
                        predicate="swrl:materialized",
                        object=rule_name,
                        provenance=f"swrl:{rule_name}",
                        rule_name=rule_name,
                    ))
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
                local = uri.rsplit("#", 1)[-1] if "#" in uri else uri.rsplit("/", 1)[-1]
                uri = data_ns + local
            elif not uri and name:
                uri = data_ns + name if data_ns else base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        logger.debug("SWRL uri_map (%d entries, data_ns=%s): %s",
                     len(uri_map), data_ns,
                     {k: v for k, v in list(uri_map.items())[:10]})
        return uri_map

    @staticmethod
    def _is_ladybug_backend(store) -> bool:
        """Check if the store is any LadybugDB backend (graph or flat)."""
        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            return isinstance(store, LadybugBase)
        except ImportError:
            return False

    @staticmethod
    def _is_graph_backend(store) -> bool:
        """Check if the store is a LadybugDB graph-model backend."""
        try:
            from back.core.triplestore.ladybugdb.LadybugGraphStore import LadybugGraphStore
            return isinstance(store, LadybugGraphStore) and store.use_graph_model
        except ImportError:
            return False

    @staticmethod
    def _get_translator(store, table_name: str = ""):
        """Return the appropriate translator for the backend."""
        try:
            from back.core.triplestore.ladybugdb.LadybugGraphStore import LadybugGraphStore
            if isinstance(store, LadybugGraphStore) and store.use_graph_model:
                from back.core.reasoning.SWRLCypherTranslator import SWRLCypherTranslator
                return SWRLCypherTranslator(graph_schema=store._graph_schema)
        except ImportError:
            pass

        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            if isinstance(store, LadybugBase):
                from back.core.reasoning.SWRLFlatCypherTranslator import SWRLFlatCypherTranslator
                node_table = LadybugBase.safe_table_id(table_name) if table_name else "Triple"
                return SWRLFlatCypherTranslator(node_table=node_table)
        except ImportError:
            pass

        from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator
        return SWRLSQLTranslator()
