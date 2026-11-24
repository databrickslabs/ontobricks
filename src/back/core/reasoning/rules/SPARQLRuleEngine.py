"""SPARQL CONSTRUCT rule engine — execute Complex rules on the triple store.

Each rule is a SPARQL CONSTRUCT query that produces new triples from
existing graph data.  Every supported triple store is SQL-based (Delta
views, Lakebase Postgres), so :class:`SparqlConstructCompiler` turns the
CONSTRUCT into a ``SELECT`` on the flat ``(subject, predicate, object)``
table.  A query outside the supported subset is refused, never run partially.
"""

import time
from typing import Any, Dict, List

from back.core.errors import ValidationError
from back.core.helpers import SQL_DIALECT_SPARK
from back.core.logging import get_logger
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.results.RuleViolation import RuleViolation
from back.core.reasoning.rules.SparqlConstructCompiler import SparqlConstructCompiler
from back.core.w3c.rdf_utils import uri_local_name

logger = get_logger(__name__)


class SPARQLRuleEngine:
    """Execute SPARQL CONSTRUCT rules against a triple store."""

    @staticmethod
    def _build_uri_map(ontology: Dict) -> Dict[str, str]:
        """Build a lowercase name -> URI map from ontology classes/properties."""
        uri_map: Dict[str, str] = {}
        base_uri = ontology.get("base_uri", "")
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""

        for cls in ontology.get("classes", []):
            name = cls.get("name", "") or cls.get("localName", "")
            uri = cls.get("uri", "")
            if not uri and name:
                uri = base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        for prop in ontology.get("properties", []):
            name = prop.get("name", "") or prop.get("localName", "")
            uri = prop.get("uri", "")
            if data_ns and uri and not uri.startswith(data_ns):
                local = uri_local_name(uri)
                uri = data_ns + local
            elif not uri and name:
                uri = data_ns + name if data_ns else base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        return uri_map

    def execute_rules(
        self,
        rules: List[Dict],
        store: Any,
        table_name: str,
        ontology: Dict,
    ) -> ReasoningResult:
        """Run all SPARQL rules and collect results."""
        t0 = time.time()
        result = ReasoningResult()
        failed: List[str] = []

        for rule in rules:
            if not rule.get("enabled", True):
                continue
            name = rule.get("name", "unnamed")
            query_text = rule.get("query", "")
            if not query_text.strip():
                continue
            try:
                result.merge(
                    self._execute_one(name, query_text, store, table_name, ontology)
                )
            except Exception as e:
                logger.error("SPARQL rule '%s' failed: %s", name, e)
                failed.append(f"{name}: {e}")
                result.violations.append(
                    RuleViolation(
                        rule_name=name,
                        subject="",
                        message=f"Execution error: {e}",
                        check_type="sparql_rule",
                        rule_type="sparql",
                    )
                )

        result.stats = {
            "phase": "sparql_rules",
            "rules_count": len(rules),
            "violations_count": len(result.violations),
            "inferred_count": len(result.inferred_triples),
            "errors": len(failed),
            "error_details": "; ".join(failed)[:2000],
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def _execute_one(
        self,
        name: str,
        query: str,
        store: Any,
        table_name: str,
        ontology: Dict,
    ) -> ReasoningResult:
        compiler = SparqlConstructCompiler(
            base_uri=ontology.get("base_uri", ""),
            uri_map=self._build_uri_map(ontology),
            dialect=store.sql_flavor() or SQL_DIALECT_SPARK,
        )
        sql = compiler.compile(query, store.sql_table_reference(table_name))
        logger.debug("SPARQL rule '%s' translated query:\n%s", name, sql)

        result = ReasoningResult()
        for row in store.execute_query(sql) or []:
            result.inferred_triples.append(
                InferredTriple(
                    subject=row.get("s", ""),
                    predicate=row.get("p", ""),
                    object=row.get("o", ""),
                    provenance=f"sparql:{name}",
                    rule_name=name,
                )
            )
        return result

    @staticmethod
    def validate_rule(rule: Dict) -> List[str]:
        """Return validation error messages (empty if valid).

        A query is valid only if it compiles to SQL, so a rule that saves is a
        rule that runs.
        """
        errors: List[str] = []
        if not rule.get("name"):
            errors.append("SPARQL rule must have a name")
        query = rule.get("query", "")
        if not query.strip():
            errors.append("SPARQL rule must have a query")
            return errors
        try:
            SparqlConstructCompiler().compile(query, "triples")
        except ValidationError as exc:
            errors.append(exc.message)
        return errors
