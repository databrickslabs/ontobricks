"""SPARQL CONSTRUCT rule engine — execute inference rules as SPARQL queries.

Each rule is a SPARQL CONSTRUCT query that produces new triples from
existing graph data.  For LadybugDB backends the CONSTRUCT is converted
to Cypher; for Delta backends the existing ``translate_sparql_to_spark()``
infrastructure is used.
"""
import re
import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.w3c.rdf_utils import uri_local_name
from back.core.reasoning.constants import CONSTRUCT_RE, NS_PREFIX_MAP, RDF_TYPE, TRIPLE_PATTERN_RE
from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation

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
        materialize: bool = False,
    ) -> ReasoningResult:
        """Run all SPARQL rules and collect results."""
        t0 = time.time()
        result = ReasoningResult()
        is_cypher = self._is_cypher_backend(store)
        self._uri_map = self._build_uri_map(ontology)

        for rule in rules:
            if not rule.get("enabled", True):
                continue
            name = rule.get("name", "unnamed")
            query_text = rule.get("query", "")
            if not query_text.strip():
                continue
            try:
                rule_result = self._execute_one(
                    name, query_text, store, table_name, ontology, is_cypher, materialize,
                )
                result.merge(rule_result)
            except Exception as e:
                logger.error("SPARQL rule '%s' failed: %s", name, e)
                result.violations.append(RuleViolation(
                    rule_name=name,
                    subject="",
                    message=f"Execution error: {e}",
                    check_type="sparql_rule",
                    rule_type="sparql",
                ))

        result.stats = {
            "phase": "sparql_rules",
            "rules_count": len(rules),
            "violations_count": len(result.violations),
            "inferred_count": len(result.inferred_triples),
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def _execute_one(
        self, name: str, query: str, store: Any,
        table_name: str, ontology: Dict, is_cypher: bool,
        materialize: bool,
    ) -> ReasoningResult:
        result = ReasoningResult()

        if is_cypher:
            sql = self._construct_to_flat_cypher_select(query, table_name, ontology)
        else:
            sql = self._construct_to_sql(query, table_name, ontology)

        if not sql:
            logger.warning("Could not translate SPARQL rule '%s'. Input:\n%s", name, query)
            return result

        logger.debug("SPARQL rule '%s' translated query:\n%s", name, sql)

        try:
            if is_cypher:
                conn = store._get_connection()
                raw = conn.execute(sql)
                for row in raw:
                    s = row[0] if len(row) > 0 else ""
                    p = row[1] if len(row) > 1 else ""
                    o = row[2] if len(row) > 2 else ""
                    result.inferred_triples.append(InferredTriple(
                        subject=s, predicate=p, object=o,
                        provenance=f"sparql:{name}",
                        rule_name=name,
                    ))
            else:
                raw = store.execute_query(sql)
                for row in raw:
                    result.inferred_triples.append(InferredTriple(
                        subject=row.get("s", ""),
                        predicate=row.get("p", ""),
                        object=row.get("o", ""),
                        provenance=f"sparql:{name}",
                        rule_name=name,
                    ))
        except Exception as e:
            logger.error("SPARQL rule query failed for '%s': %s", name, e)

        return result

    def _construct_to_sql(
        self, query: str, table: str, ontology: Dict,
    ) -> Optional[str]:
        """Translate a CONSTRUCT query to a SELECT for the flat triple table.

        This is a simplified translator that handles basic triple patterns
        and FILTER expressions.  For full SPARQL support the existing
        ``translate_sparql_to_spark()`` path can be used instead.
        """
        m = CONSTRUCT_RE.search(query)
        if not m:
            return None

        construct_part = m.group(1).strip()
        where_part = m.group(2).strip()
        base_uri = ontology.get("base_uri", "")

        construct_triples = TRIPLE_PATTERN_RE.findall(construct_part)
        if not construct_triples:
            return None

        um = getattr(self, '_uri_map', None)
        s_expr = self._resolve_term(construct_triples[0][0], base_uri, um)
        p_expr = self._resolve_term(construct_triples[0][1], base_uri, um)
        o_expr = self._resolve_term(construct_triples[0][2], base_uri, um)

        where_triples = TRIPLE_PATTERN_RE.findall(where_part)
        if not where_triples:
            return None

        var_bindings: Dict[str, str] = {}
        joins: List[str] = []
        conditions: List[str] = []
        alias_idx = 0

        for ws, wp, wo in where_triples:
            alias = f"w{alias_idx}"
            alias_idx += 1
            joins.append(f"{table} {alias}" if alias_idx == 1 else f"JOIN {table} {alias} ON 1=1")

            wp_resolved = self._resolve_term(wp, base_uri, um)
            if wp_resolved == "a":
                wp_resolved = RDF_TYPE
            conditions.append(f"{alias}.predicate = '{wp_resolved}'")

            if ws.startswith("?"):
                var = ws
                if var in var_bindings:
                    conditions.append(f"{alias}.subject = {var_bindings[var]}")
                else:
                    var_bindings[var] = f"{alias}.subject"
            else:
                conditions.append(f"{alias}.subject = '{self._resolve_term(ws, base_uri, um)}'")

            if wo.startswith("?"):
                var = wo
                if var in var_bindings:
                    conditions.append(f"{alias}.object = {var_bindings[var]}")
                else:
                    var_bindings[var] = f"{alias}.object"
            else:
                conditions.append(f"{alias}.object = '{self._resolve_term(wo, base_uri, um)}'")

        filter_match = re.search(r"FILTER\s*\((.+?)\)", where_part, re.IGNORECASE)
        if filter_match:
            filter_expr = filter_match.group(1).strip()
            sql_filter = self._translate_filter(filter_expr, var_bindings)
            if sql_filter:
                conditions.append(sql_filter)

        s_sql = var_bindings.get(s_expr, f"'{s_expr}'") if s_expr.startswith("?") else f"'{s_expr}'"
        if p_expr == "a":
            p_sql = f"'{RDF_TYPE}'"
        else:
            p_sql = f"'{p_expr}'"
        o_sql = var_bindings.get(o_expr, f"'{o_expr}'") if o_expr.startswith("?") else f"'{o_expr}'"

        from_clause = joins[0] if joins else table
        join_clause = "\n".join(joins[1:]) if len(joins) > 1 else ""

        sql = (
            f"SELECT DISTINCT {s_sql} AS s, {p_sql} AS p, {o_sql} AS o\n"
            f"FROM {from_clause}\n"
        )
        if join_clause:
            sql += join_clause + "\n"
        if conditions:
            sql += f"WHERE {' AND '.join(conditions)}"

        return sql

    def _construct_to_flat_cypher_select(
        self, query: str, table_name: str, ontology: Dict,
    ) -> Optional[str]:
        """Translate CONSTRUCT to a Cypher SELECT on the flat triple table."""
        m = CONSTRUCT_RE.search(query)
        if not m:
            return None

        construct_part = m.group(1).strip()
        where_part = m.group(2).strip()
        base_uri = ontology.get("base_uri", "")

        construct_triples = TRIPLE_PATTERN_RE.findall(construct_part)
        if not construct_triples:
            return None

        where_triples = TRIPLE_PATTERN_RE.findall(where_part)
        if not where_triples:
            return None

        um = getattr(self, '_uri_map', None)
        match_parts: List[str] = []
        where_parts: List[str] = []
        var_alias: Dict[str, str] = {}
        alias_idx = 0

        for ws, wp, wo in where_triples:
            alias = f"w{alias_idx}"
            alias_idx += 1
            match_parts.append(f"MATCH ({alias}:{table_name})")

            wp_resolved = self._resolve_term(wp, base_uri, um)
            if wp_resolved == "a":
                wp_resolved = RDF_TYPE
            where_parts.append(f"{alias}.predicate = '{wp_resolved}'")

            if ws.startswith("?"):
                if ws in var_alias:
                    where_parts.append(f"{alias}.subject = {var_alias[ws]}")
                else:
                    var_alias[ws] = f"{alias}.subject"
            else:
                where_parts.append(f"{alias}.subject = '{self._resolve_term(ws, base_uri, um)}'")

            if wo.startswith("?"):
                if wo in var_alias:
                    where_parts.append(f"{alias}.object = {var_alias[wo]}")
                else:
                    var_alias[wo] = f"{alias}.object"
            else:
                where_parts.append(f"{alias}.object = '{self._resolve_term(wo, base_uri, um)}'")

        s_ref = var_alias.get(construct_triples[0][0], f"'{construct_triples[0][0]}'")
        p_term = self._resolve_term(construct_triples[0][1], base_uri, um)
        if p_term == "a":
            p_term = RDF_TYPE
        o_ref = var_alias.get(construct_triples[0][2], f"'{construct_triples[0][2]}'")

        lines = match_parts + [f"WHERE {' AND '.join(where_parts)}"]
        lines.append(f"RETURN DISTINCT {s_ref} AS s, '{p_term}' AS p, {o_ref} AS o")
        return "\n".join(lines)

    @staticmethod
    def _resolve_term(term: str, base_uri: str, uri_map: Optional[Dict[str, str]] = None) -> str:
        if term.startswith("<") and term.endswith(">"):
            return term[1:-1]
        if term == "a":
            return "a"
        if term.startswith("?"):
            return term
        if ":" in term and not term.startswith("http"):
            prefix, local = term.split(":", 1)
            if prefix in NS_PREFIX_MAP:
                return NS_PREFIX_MAP[prefix] + local
            if uri_map:
                resolved = uri_map.get(local.lower())
                if resolved:
                    return resolved
            if base_uri:
                sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
                return base_uri + sep + local
        if uri_map:
            resolved = uri_map.get(term.lower())
            if resolved:
                return resolved
        if base_uri and not term.startswith("http"):
            sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
            return base_uri + sep + term
        return term

    @staticmethod
    def _translate_filter(expr: str, var_bindings: Dict[str, str]) -> Optional[str]:
        """Translate a simple FILTER expression to SQL."""
        m = re.match(r"\?\w+\s*([><=!]+)\s*(.+)", expr.strip())
        if not m:
            return None
        var_match = re.match(r"(\?\w+)", expr.strip())
        if not var_match:
            return None
        var = var_match.group(1)
        ref = var_bindings.get(var)
        if not ref:
            return None
        op = m.group(1)
        val = m.group(2).strip().strip('"').strip("'")
        try:
            float(val)
            return f"CAST({ref} AS DOUBLE) {op} {val}"
        except ValueError:
            return f"{ref} {op} '{val}'"

    @staticmethod
    def validate_rule(rule: Dict) -> List[str]:
        """Return validation error messages (empty if valid)."""
        errors: List[str] = []
        if not rule.get("name"):
            errors.append("SPARQL rule must have a name")
        query = rule.get("query", "")
        if not query.strip():
            errors.append("SPARQL rule must have a query")
        elif not CONSTRUCT_RE.search(query):
            errors.append("Query must be a SPARQL CONSTRUCT (CONSTRUCT { ... } WHERE { ... })")
        return errors

    @staticmethod
    def _is_cypher_backend(store) -> bool:
        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            return isinstance(store, LadybugBase)
        except ImportError:
            return False
