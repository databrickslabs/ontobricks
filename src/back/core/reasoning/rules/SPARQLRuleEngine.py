"""SPARQL CONSTRUCT rule engine — execute Complex rules on the triple store.

Each rule is a SPARQL CONSTRUCT query that produces new triples from
existing graph data.  Every supported triple store is SQL-based (Delta
views, Lakebase Postgres), so :class:`SparqlConstructCompiler` turns the
CONSTRUCT into a ``SELECT`` on the flat ``(subject, predicate, object)``
table.  A query outside the supported subset is refused, never run partially.
"""

import time
from typing import Any, Dict, List, Sequence

from back.core.errors import ValidationError
from back.core.helpers import SQL_DIALECT_SPARK
from back.core.logging import get_logger
from back.core.reasoning.results.InferredTriple import InferredTriple
from back.core.reasoning.results.ReasoningResult import ReasoningResult
from back.core.reasoning.results.RuleViolation import RuleViolation
from back.core.reasoning.rules.SparqlConstructCompiler import SparqlConstructCompiler
from back.core.reasoning.rules.rule_kind import (
    NOT_A_RULE_MESSAGE,
    RULE_KIND_CONSTRUCT,
    RULE_KIND_REMOVAL,
    sparql_rule_kind,
)
from back.core.reasoning.rules.SparqlUpdateCompiler import SparqlUpdateCompiler
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
        # Removal rules run first: a CONSTRUCT rule of the same run reads the graph
        # without the triples they hide. The run itself writes nothing.
        ordered = sorted(
            (r for r in rules if r.get("enabled", True) and (r.get("query") or "").strip()),
            key=lambda r: sparql_rule_kind(r.get("query")) != RULE_KIND_REMOVAL,
        )
        removal_sql: List[str] = []

        for rule in ordered:
            name = rule.get("name", "unnamed")
            try:
                result.merge(self._execute_one(rule, store, table_name, ontology, removal_sql))
                if sparql_rule_kind(rule.get("query")) == RULE_KIND_REMOVAL:
                    removal_sql.append(self._removal_sql(rule, store, table_name, ontology))
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
            "removed_count": len(result.removed_triples),
            "errors": len(failed),
            "error_details": "; ".join(failed)[:2000],
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def count_removals(
        self, rule: Dict, store: Any, table_name: str, ontology: Dict
    ) -> int:
        """Count the distinct triples a removal rule matches, without hiding them.

        The count is the preview shown before a run. It does not apply the D2
        guard (triples with a user add are skipped at write time), so it can
        slightly over-count those.
        """
        sql = self._removal_sql(rule, store, table_name, ontology)
        rows = store.execute_query(
            f"SELECT COUNT(*) AS cnt FROM (SELECT DISTINCT s, p, o FROM ({sql}) u) d"
        ) or []
        return int(rows[0].get("cnt", 0)) if rows else 0

    @staticmethod
    def _options(store: Any, ontology: Dict) -> Dict:
        """Compiler options shared by the CONSTRUCT and removal compilers."""
        return {
            "base_uri": ontology.get("base_uri", ""),
            "uri_map": SPARQLRuleEngine._build_uri_map(ontology),
            "dialect": store.sql_flavor() or SQL_DIALECT_SPARK,
        }

    def _removal_sql(self, rule: Dict, store: Any, table_name: str, ontology: Dict) -> str:
        """SQL selecting the triples a removal rule matches (raises ValidationError)."""
        compiler = SparqlUpdateCompiler(**self._options(store, ontology))
        return compiler.compile_removal(rule.get("query", ""), store.sql_table_reference(table_name))

    @staticmethod
    def _table_after_removals(base: str, removal_sql: List[str]) -> str:
        """The base table minus the triples the earlier removal rules of this run match.

        A user add row keeps its triple when the removal is written (D2), but
        this view does not apply D2, so a CONSTRUCT rule of the same run skips
        such a triple too.
        """
        if not removal_sql:
            return base
        union = "\nUNION ALL\n".join(removal_sql)
        return (
            f"(SELECT t.* FROM {base} t WHERE NOT EXISTS (SELECT 1 FROM ({union}) r "
            "WHERE r.s = t.subject AND r.p = t.predicate AND r.o = t.object))"
        )

    def _execute_one(
        self,
        rule: Dict,
        store: Any,
        table_name: str,
        ontology: Dict,
        removal_sql: Sequence[str] = (),
    ) -> ReasoningResult:
        """Run one rule; its query form selects the CONSTRUCT or the removal path.

        ``removal_sql`` holds the removal rules that ran before this one in the
        same run; a CONSTRUCT rule reads the table without their triples.
        """
        name = rule.get("name", "unnamed")
        query = rule.get("query", "")
        options = self._options(store, ontology)
        base = store.sql_table_reference(table_name)
        kind = sparql_rule_kind(query)
        if kind == RULE_KIND_CONSTRUCT:
            table = self._table_after_removals(base, list(removal_sql))
            return self._execute_construct(name, query, store, table, options)
        if kind == RULE_KIND_REMOVAL:
            return self._execute_removal(name, query, store, base, options)
        raise ValidationError(NOT_A_RULE_MESSAGE)

    @staticmethod
    def _execute_construct(
        name: str, query: str, store: Any, table_sql: str, options: Dict
    ) -> ReasoningResult:
        compiler = SparqlConstructCompiler(**options)
        sql = compiler.compile(query, table_sql)
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
    def _execute_removal(
        name: str, query: str, store: Any, table_sql: str, options: Dict
    ) -> ReasoningResult:
        """Collect the stored triples a removal matches; the caller hides them."""
        compiler = SparqlUpdateCompiler(**options)
        sql = compiler.compile_removal(query, table_sql)
        logger.debug("SPARQL removal '%s' translated query:\n%s", name, sql)

        result = ReasoningResult()
        found = set()
        for row in store.execute_query(sql) or []:
            s, p, o = row.get("s", ""), row.get("p", ""), row.get("o", "")
            found.add((s, p, o))
            result.removed_triples.append({"subject": s, "predicate": p, "object": o})

        # D3: a DELETE DATA triple that is not in the store is reported, not hidden.
        try:
            expected = compiler.data_triples(query)
        except ValidationError:  # not a DELETE DATA: nothing to check
            expected = []
        for s, p, o in expected:
            if (s, p, o) not in found:
                result.violations.append(
                    RuleViolation(
                        rule_name=name,
                        subject=s,
                        message=f"triple not found: {s} {p} {o}",
                        check_type="removal_not_found",
                        rule_type="sparql",
                    )
                )
        result.stats = {"removed_count": len(result.removed_triples)}
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
        kind = sparql_rule_kind(query)
        if kind is None:
            errors.append(NOT_A_RULE_MESSAGE)
            return errors
        try:
            if kind == RULE_KIND_CONSTRUCT:
                SparqlConstructCompiler().compile(query, "triples")
            else:
                SparqlUpdateCompiler().compile_removal(query, "triples")
        except ValidationError as exc:
            errors.append(exc.message)
        return errors
