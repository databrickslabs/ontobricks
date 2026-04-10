"""Aggregate rule engine — rules that use GROUP BY / HAVING for statistical conditions.

Each rule defines:

- *target_class*: the class whose instances are grouped
- *group_by_property*: the relationship used for grouping
- *aggregate_property*: the property to aggregate
- *aggregate_function*: count, sum, avg, min, max
- *operator* + *threshold*: the condition on the aggregate
- *result_class*: the class to assign to matching instances
"""
import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.constants import AGG_FUNCTIONS, AGG_OPERATORS, RDF_TYPE
from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation

logger = get_logger(__name__)


class AggregateRuleEngine:
    """Compile and execute aggregate rules against a triple store."""

    @staticmethod
    def _resolve_rule(rule: Dict, ontology: Dict) -> Dict:
        """Return a copy of *rule* with ``_uri`` fields resolved from names."""
        rule = dict(rule)
        base_uri = ontology.get("base_uri", "")
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""

        uri_map: Dict[str, str] = {}
        for cls in ontology.get("classes", []):
            name = cls.get("name", "") or cls.get("localName", "")
            uri = cls.get("uri", "")
            if not uri and name:
                uri = base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri
            for dp in cls.get("dataProperties", []):
                dp_name = dp.get("name", "") or dp.get("localName", "")
                dp_uri = dp.get("uri", "")
                if data_ns and dp_uri and not dp_uri.startswith(data_ns):
                    local = dp_uri.rsplit("#", 1)[-1] if "#" in dp_uri else dp_uri.rsplit("/", 1)[-1]
                    dp_uri = data_ns + local
                elif not dp_uri and dp_name:
                    dp_uri = data_ns + dp_name if data_ns else base_uri + sep + dp_name
                if dp_name and dp_name.lower() not in uri_map:
                    uri_map[dp_name.lower()] = dp_uri
        for prop in ontology.get("properties", []):
            name = prop.get("name", "") or prop.get("localName", "")
            uri = prop.get("uri", "")
            if data_ns and uri and not uri.startswith(data_ns):
                local = uri.rsplit("#", 1)[-1] if "#" in uri else uri.rsplit("/", 1)[-1]
                uri = data_ns + local
            elif not uri and name:
                uri = data_ns + name if data_ns else base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri

        def _resolve(field_name: str, field_uri: str, is_class: bool = False) -> None:
            if not rule.get(field_uri):
                name = rule.get(field_name, "")
                if name:
                    fallback = (base_uri + sep + name) if is_class else (data_ns + name if data_ns else base_uri + sep + name)
                    rule[field_uri] = uri_map.get(name.lower(), fallback)

        _resolve("target_class", "target_class_uri", is_class=True)
        _resolve("group_by_property", "group_by_property_uri")
        _resolve("aggregate_property", "aggregate_property_uri")
        _resolve("result_class", "result_class_uri", is_class=True)
        return rule

    def execute_rules(
        self,
        rules: List[Dict],
        store: Any,
        table_name: str,
        ontology: Dict,
        materialize: bool = False,
    ) -> ReasoningResult:
        """Run all aggregate rules and collect results."""
        t0 = time.time()
        result = ReasoningResult()
        base_uri = ontology.get("base_uri", "")
        is_cypher = self._is_cypher_backend(store)

        for rule in rules:
            if not rule.get("enabled", True):
                continue
            name = rule.get("name", "unnamed")
            try:
                resolved = self._resolve_rule(rule, ontology)
                rule_result = self._execute_one(
                    resolved, store, table_name, base_uri, is_cypher, materialize,
                )
                result.merge(rule_result)
            except Exception as e:
                logger.error("Aggregate rule '%s' failed: %s", name, e)
                result.violations.append(RuleViolation(
                    rule_name=name,
                    subject="",
                    message=f"Execution error: {e}",
                    check_type="aggregate",
                    rule_type="aggregate",
                ))

        result.stats = {
            "phase": "aggregate_rules",
            "rules_count": len(rules),
            "violations_count": len(result.violations),
            "inferred_count": len(result.inferred_triples),
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def _execute_one(
        self, rule: Dict, store: Any, table_name: str,
        base_uri: str, is_cypher: bool, materialize: bool,
    ) -> ReasoningResult:
        result = ReasoningResult()
        name = rule.get("name", "unnamed")

        if is_cypher:
            query = self.build_cypher(rule, table_name, base_uri)
        else:
            query = self.build_sql(rule, table_name, base_uri)

        if not query:
            return result

        try:
            if is_cypher:
                conn = store._get_connection()
                raw = conn.execute(query)
                for row in raw:
                    subj = row[0] if row else ""
                    agg_val = row[1] if len(row) > 1 else ""
                    result.violations.append(RuleViolation(
                        rule_name=name,
                        subject=subj,
                        message=f"Aggregate rule '{name}': value={agg_val}",
                        check_type="aggregate",
                        rule_type="aggregate",
                    ))
                    if materialize and rule.get("result_class_uri"):
                        result.inferred_triples.append(InferredTriple(
                            subject=subj,
                            predicate=RDF_TYPE,
                            object=rule["result_class_uri"],
                            provenance=f"aggregate:{name}",
                            rule_name=name,
                        ))
            else:
                raw = store.execute_query(query)
                for row in raw:
                    subj = row.get("s", "")
                    agg_val = row.get("agg_val", "")
                    result.violations.append(RuleViolation(
                        rule_name=name,
                        subject=subj,
                        message=f"Aggregate rule '{name}': value={agg_val}",
                        check_type="aggregate",
                        rule_type="aggregate",
                    ))
                    if materialize and rule.get("result_class_uri"):
                        result.inferred_triples.append(InferredTriple(
                            subject=subj,
                            predicate=RDF_TYPE,
                            object=rule["result_class_uri"],
                            provenance=f"aggregate:{name}",
                            rule_name=name,
                        ))
        except Exception as e:
            logger.error("Aggregate rule query failed for '%s': %s", name, e)

        return result

    def build_sql(self, rule: Dict, table: str, base_uri: str) -> Optional[str]:
        """Build SQL with GROUP BY / HAVING for an aggregate rule."""
        target_uri = rule.get("target_class_uri", "")
        group_prop_uri = rule.get("group_by_property_uri", "")
        agg_prop_uri = rule.get("aggregate_property_uri", "")
        agg_func = rule.get("aggregate_function", "count").lower()
        operator = rule.get("operator", "gt")
        threshold = rule.get("threshold", "0")

        if not target_uri or agg_func not in AGG_FUNCTIONS:
            return None
        sql_op = AGG_OPERATORS.get(operator, ">")
        func = agg_func.upper()

        def esc(v: str) -> str:
            return v.replace("'", "''")

        if group_prop_uri and agg_prop_uri:
            return (
                f"SELECT t0.subject AS s, {func}(CAST(t_agg.object AS DOUBLE)) AS agg_val\n"
                f"FROM {table} t0\n"
                f"JOIN {table} t_grp ON t_grp.subject = t0.subject AND t_grp.predicate = '{esc(group_prop_uri)}'\n"
                f"JOIN {table} t_agg ON t_agg.subject = t_grp.object AND t_agg.predicate = '{esc(agg_prop_uri)}'\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{esc(target_uri)}'\n"
                f"GROUP BY t0.subject\n"
                f"HAVING {func}(CAST(t_agg.object AS DOUBLE)) {sql_op} {threshold}"
            )
        elif agg_prop_uri:
            return (
                f"SELECT t0.subject AS s, {func}(CAST(t_agg.object AS DOUBLE)) AS agg_val\n"
                f"FROM {table} t0\n"
                f"JOIN {table} t_agg ON t_agg.subject = t0.subject AND t_agg.predicate = '{esc(agg_prop_uri)}'\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{esc(target_uri)}'\n"
                f"GROUP BY t0.subject\n"
                f"HAVING {func}(CAST(t_agg.object AS DOUBLE)) {sql_op} {threshold}"
            )
        elif group_prop_uri:
            agg_expr = f"{func}(CAST(t_grp.object AS DOUBLE))" if func != "COUNT" else "COUNT(t_grp.object)"
            return (
                f"SELECT t0.subject AS s, {agg_expr} AS agg_val\n"
                f"FROM {table} t0\n"
                f"JOIN {table} t_grp ON t_grp.subject = t0.subject AND t_grp.predicate = '{esc(group_prop_uri)}'\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{esc(target_uri)}'\n"
                f"GROUP BY t0.subject\n"
                f"HAVING {agg_expr} {sql_op} {threshold}"
            )
        else:
            return (
                f"SELECT '{esc(target_uri)}' AS s, COUNT(t0.subject) AS agg_val\n"
                f"FROM {table} t0\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{esc(target_uri)}'\n"
                f"HAVING COUNT(t0.subject) {sql_op} {threshold}"
            )

    def build_cypher(self, rule: Dict, table_name: str, base_uri: str) -> Optional[str]:
        """Build Cypher aggregate query for the flat triple table."""
        target_uri = rule.get("target_class_uri", "")
        group_prop_uri = rule.get("group_by_property_uri", "")
        agg_prop_uri = rule.get("aggregate_property_uri", "")
        agg_func = rule.get("aggregate_function", "count").lower()
        operator = rule.get("operator", "gt")
        threshold = rule.get("threshold", "0")

        if not target_uri or agg_func not in AGG_FUNCTIONS:
            return None
        cypher_op = AGG_OPERATORS.get(operator, ">")

        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            tbl = LadybugBase.safe_table_id(table_name)
        except ImportError:
            tbl = table_name

        if group_prop_uri and agg_prop_uri:
            cypher_agg = agg_func if agg_func != "count" else "count"
            return (
                f"MATCH (t0:{tbl}), (tg:{tbl}), (ta:{tbl})\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{target_uri}'\n"
                f"  AND tg.subject = t0.subject AND tg.predicate = '{group_prop_uri}'\n"
                f"  AND ta.subject = tg.object AND ta.predicate = '{agg_prop_uri}'\n"
                f"WITH t0.subject AS s, {cypher_agg}(CAST(ta.object AS DOUBLE)) AS agg_val\n"
                f"WHERE agg_val {cypher_op} {threshold}\n"
                f"RETURN s, agg_val"
            )
        elif agg_prop_uri:
            cypher_agg = agg_func if agg_func != "count" else "count"
            return (
                f"MATCH (t0:{tbl}), (ta:{tbl})\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{target_uri}'\n"
                f"  AND ta.subject = t0.subject AND ta.predicate = '{agg_prop_uri}'\n"
                f"WITH t0.subject AS s, {cypher_agg}(CAST(ta.object AS DOUBLE)) AS agg_val\n"
                f"WHERE agg_val {cypher_op} {threshold}\n"
                f"RETURN s, agg_val"
            )
        elif group_prop_uri:
            agg_expr = f"{agg_func}(CAST(tg.object AS DOUBLE))" if agg_func != "count" else "count(tg.object)"
            return (
                f"MATCH (t0:{tbl}), (tg:{tbl})\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{target_uri}'\n"
                f"  AND tg.subject = t0.subject AND tg.predicate = '{group_prop_uri}'\n"
                f"WITH t0.subject AS s, {agg_expr} AS agg_val\n"
                f"WHERE agg_val {cypher_op} {threshold}\n"
                f"RETURN s, agg_val"
            )
        else:
            return (
                f"MATCH (t0:{tbl})\n"
                f"WHERE t0.predicate = '{RDF_TYPE}' AND t0.object = '{target_uri}'\n"
                f"WITH '{target_uri}' AS s, count(t0.subject) AS agg_val\n"
                f"WHERE agg_val {cypher_op} {threshold}\n"
                f"RETURN s, agg_val"
            )

    @staticmethod
    def validate_rule(rule: Dict) -> List[str]:
        """Return validation error messages (empty if valid)."""
        errors: List[str] = []
        if not rule.get("name"):
            errors.append("Aggregate rule must have a name")
        if not rule.get("target_class"):
            errors.append("Aggregate rule must have a target class")
        func = rule.get("aggregate_function", "").lower()
        if func and func not in AGG_FUNCTIONS:
            errors.append(f"Invalid aggregate function: {func}")
        op = rule.get("operator", "")
        if op and op not in AGG_OPERATORS:
            errors.append(f"Invalid operator: {op}")
        if not rule.get("group_by_property") and not rule.get("aggregate_property"):
            if func and func != "count":
                errors.append("Must specify at least one of group_by_property or aggregate_property (only COUNT works without them)")
        return errors

    @staticmethod
    def _is_cypher_backend(store) -> bool:
        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            return isinstance(store, LadybugBase)
        except ImportError:
            return False
