"""Decision table engine — compile tabular business rules to SQL / Cypher."""
import time
from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation
from back.core.reasoning.constants import (
    RDF_TYPE,
    DT_STRING_OPS,
    DT_NUMERIC_OPS,
    DT_OP_SQL,
    DT_OP_CYPHER,
)

logger = get_logger(__name__)


class DecisionTableEngine:
    """Decision table engine — compile tabular business rules to SQL / Cypher.

    A decision table has:

    - *input_columns*: each maps to a class property (conditions)
    - *output_column*: the inferred predicate or class assignment (action)
    - *rows*: each row has condition cells and an action cell
    - *hit_policy*: ``first`` (first matching row wins), ``all`` (all matching
      rows fire), or ``unique`` (at most one row should match)
    """

    @staticmethod
    def _esc_sql(val: str) -> str:
        return val.replace("'", "''")

    @staticmethod
    def _is_numeric(val: str) -> bool:
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _build_uri_map(ontology: Dict) -> Dict[str, str]:
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
                local = uri.rsplit("#", 1)[-1] if "#" in uri else uri.rsplit("/", 1)[-1]
                uri = data_ns + local
            elif not uri and name:
                uri = data_ns + name if data_ns else base_uri + sep + name
            if name:
                uri_map[name.lower()] = uri
        return uri_map

    @staticmethod
    def _resolve_dt(dt: Dict, uri_map: Dict[str, str], base_uri: str) -> Dict:
        dt = dict(dt)
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""
        if not dt.get("target_class_uri"):
            name = dt.get("target_class", "")
            dt["target_class_uri"] = uri_map.get(name.lower(), base_uri + sep + name if name else "")
        resolved_cols = []
        for col in dt.get("input_columns", []):
            col = dict(col)
            if not col.get("property_uri"):
                name = col.get("property", "")
                col["property_uri"] = uri_map.get(name.lower(), data_ns + name if name else "")
            resolved_cols.append(col)
        dt["input_columns"] = resolved_cols
        out = dict(dt.get("output_column", {}))
        if not out.get("property_uri") and out.get("property"):
            name = out["property"]
            out["property_uri"] = uri_map.get(name.lower(), data_ns + name if name else "")
        dt["output_column"] = out
        return dt

    def execute_tables(self, tables, store, table_name, ontology, materialize=False):
        t0 = time.time()
        result = ReasoningResult()
        base_uri = ontology.get("base_uri", "")
        is_cypher = self._is_cypher_backend(store)
        uri_map = self._build_uri_map(ontology)
        for dt in tables:
            if not dt.get("enabled", True):
                continue
            try:
                resolved = self._resolve_dt(dt, uri_map, base_uri)
                dt_result = self._execute_one(resolved, store, table_name, base_uri, is_cypher, materialize)
                result.merge(dt_result)
            except Exception as e:
                logger.error("Decision table '%s' failed: %s", dt.get("name", "?"), e)
                result.violations.append(RuleViolation(
                    rule_name=dt.get("name", "unknown"), subject="",
                    message=f"Execution error: {e}",
                    check_type="decision_table", rule_type="decision_table",
                ))
        result.stats = {
            "phase": "decision_tables", "tables_count": len(tables),
            "violations_count": len(result.violations),
            "inferred_count": len(result.inferred_triples),
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def _execute_one(self, dt, store, table_name, base_uri, is_cypher, materialize):
        result = ReasoningResult()
        dt_name = dt.get("name", "unnamed")
        rows = dt.get("rows", [])
        if not rows:
            logger.debug("Decision table '%s': no rows defined, skipping", dt_name)
            return result
        logger.debug("Decision table '%s': target_class_uri=%s, inputs=%s",
                      dt_name, dt.get("target_class_uri"),
                      [c.get("property_uri") for c in dt.get("input_columns", [])])
        out_col = dt.get("output_column") or {}
        output_prop_uri = out_col.get("property_uri", "")
        output_prop_name = out_col.get("property", "")
        output_default_val = out_col.get("value", "")
        row_logic = dt.get("row_logic", "or")
        hit_policy = dt.get("hit_policy", "first")
        has_output = bool(output_prop_uri)
        if row_logic == "and" or not has_output:
            self._execute_combined(dt, store, table_name, base_uri, is_cypher,
                                   result, dt_name, output_prop_uri, output_prop_name,
                                   rows, output_default_val)
        else:
            self._execute_per_row(dt, store, table_name, base_uri, is_cypher,
                                  result, dt_name, output_prop_uri, output_prop_name,
                                  rows, hit_policy, output_default_val)
        return result

    def _execute_combined(self, dt, store, table_name, base_uri, is_cypher,
                          result, dt_name, output_prop_uri, output_prop_name,
                          rows, output_default_val=""):
        if is_cypher:
            query = self.build_violation_cypher(dt, table_name, base_uri)
        else:
            query = self.build_violation_sql(dt, table_name, base_uri)
        if not query:
            logger.warning("Decision table '%s': query builder returned None", dt_name)
            return
        logger.debug("Decision table '%s' query:\n%s", dt_name, query)
        action_val = output_default_val or ""
        if not action_val:
            for r in rows:
                if r.get("action_value"):
                    action_val = r["action_value"]
                    break
        for subj in self._run_query(store, query, is_cypher, dt_name):
            msg = f"Matches decision table '{dt_name}'"
            if action_val and output_prop_name:
                msg += f" → {output_prop_name} = {action_val}"
            result.violations.append(RuleViolation(
                rule_name=dt_name, subject=subj, message=msg,
                check_type="decision_table", rule_type="decision_table",
            ))
            if output_prop_uri and action_val:
                result.inferred_triples.append(InferredTriple(
                    subject=subj, predicate=output_prop_uri, object=action_val,
                    provenance=f"decision_table:{dt_name}", rule_name=dt_name,
                ))

    def _execute_per_row(self, dt, store, table_name, base_uri, is_cypher,
                         result, dt_name, output_prop_uri, output_prop_name,
                         rows, hit_policy, output_default_val=""):
        seen: set = set()
        for ri, row in enumerate(rows):
            action_val = output_default_val or row.get("action_value", "")
            single_dt = dict(dt)
            single_dt["rows"] = [row]
            single_dt["row_logic"] = "or"
            if is_cypher:
                query = self.build_violation_cypher(single_dt, table_name, base_uri)
            else:
                query = self.build_violation_sql(single_dt, table_name, base_uri)
            if not query:
                continue
            logger.debug("Decision table '%s' row %d query:\n%s", dt_name, ri + 1, query)
            for subj in self._run_query(store, query, is_cypher, dt_name):
                if hit_policy == "first" and subj in seen:
                    continue
                seen.add(subj)
                msg = f"Row {ri + 1} of '{dt_name}'"
                if action_val and output_prop_name:
                    msg += f" → {output_prop_name} = {action_val}"
                result.violations.append(RuleViolation(
                    rule_name=dt_name, subject=subj, message=msg,
                    check_type="decision_table", rule_type="decision_table",
                ))
                if output_prop_uri and action_val:
                    result.inferred_triples.append(InferredTriple(
                        subject=subj, predicate=output_prop_uri, object=action_val,
                        provenance=f"decision_table:{dt_name}:row{ri + 1}",
                        rule_name=dt_name,
                    ))

    @staticmethod
    def _run_query(store, query, is_cypher, dt_name):
        subjects = []
        try:
            if is_cypher:
                conn = store._get_connection()
                raw = conn.execute(query)
                for row in raw:
                    if row:
                        subjects.append(row[0])
            else:
                raw = store.execute_query(query)
                for row in raw:
                    s = row.get("s", "")
                    if s:
                        subjects.append(s)
        except Exception as e:
            logger.error("Decision table query failed for '%s': %s", dt_name, e)
        return subjects

    def build_violation_sql(self, dt, table, base_uri):
        target_cls_uri = dt.get("target_class_uri", "")
        inputs = dt.get("input_columns", [])
        rows = dt.get("rows", [])
        if not target_cls_uri or not inputs or not rows:
            return None
        joins = []
        base_where = [f"t0.predicate = '{RDF_TYPE}'", f"t0.object = '{self._esc_sql(target_cls_uri)}'"]
        for i, inp in enumerate(inputs):
            alias = f"inp{i}"
            prop_uri = inp.get("property_uri", "")
            if not prop_uri:
                continue
            joins.append(
                f"INNER JOIN {table} {alias} ON {alias}.subject = t0.subject "
                f"AND {alias}.predicate = '{self._esc_sql(prop_uri)}'"
            )
        row_conditions = []
        for row in rows:
            conds = row.get("conditions", [])
            parts = []
            for j, cond in enumerate(conds):
                op = cond.get("op", "any")
                val = cond.get("value", "")
                if op == "any" or not val:
                    continue
                alias = f"inp{j}"
                sql_op = DT_OP_SQL.get(op)
                if sql_op is None:
                    continue
                if self._is_numeric(val):
                    v_expr = val
                    lhs = f"CAST({alias}.object AS DOUBLE)" if op in DT_NUMERIC_OPS else f"{alias}.object"
                else:
                    v_expr = f"'{self._esc_sql(val.lower())}'"
                    lhs = f"LOWER({alias}.object)" if op in DT_STRING_OPS else f"{alias}.object"
                parts.append(f"{lhs} {sql_op.format(v=v_expr)}")
            if parts:
                row_conditions.append("(" + " AND ".join(parts) + ")")
        if not row_conditions:
            return None
        row_joiner = " AND " if dt.get("row_logic") == "and" else " OR "
        sql = (
            f"SELECT DISTINCT t0.subject AS s\n"
            f"FROM {table} t0\n"
            + "\n".join(joins) + "\n"
            f"WHERE {' AND '.join(base_where)}\n"
            f"  AND ({row_joiner.join(row_conditions)})"
        )
        return sql

    def build_violation_cypher(self, dt, table_name, base_uri):
        target_cls_uri = dt.get("target_class_uri", "")
        inputs = dt.get("input_columns", [])
        rows = dt.get("rows", [])
        if not target_cls_uri or not inputs or not rows:
            return None
        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            tbl = LadybugBase.safe_table_id(table_name)
        except ImportError:
            tbl = table_name
        match_parts = [f"MATCH (t0:{tbl})"]
        where_parts = [f"t0.predicate = '{RDF_TYPE}'", f"t0.object = '{target_cls_uri}'"]
        valid_inputs = []
        for i, inp in enumerate(inputs):
            alias = f"inp{i}"
            prop_uri = inp.get("property_uri", "")
            if not prop_uri:
                continue
            match_parts.append(f"MATCH ({alias}:{tbl})")
            where_parts.append(f"{alias}.subject = t0.subject")
            where_parts.append(f"{alias}.predicate = '{prop_uri}'")
            valid_inputs.append(i)
        if not valid_inputs:
            return None
        row_conditions = []
        for row in rows:
            conds = row.get("conditions", [])
            parts = []
            for j, cond in enumerate(conds):
                op = cond.get("op", "any")
                val = cond.get("value", "")
                if op == "any" or not val:
                    continue
                alias = f"inp{j}"
                cypher_op = DT_OP_CYPHER.get(op)
                if cypher_op is None:
                    continue
                if self._is_numeric(val):
                    v_expr = val
                    lhs = f"CAST({alias}.object AS DOUBLE)" if op in DT_NUMERIC_OPS else f"{alias}.object"
                else:
                    v_expr = f"'{val.lower()}'"
                    lhs = f"toLower({alias}.object)" if op in DT_STRING_OPS else f"{alias}.object"
                parts.append(f"{lhs} {cypher_op.format(v=v_expr)}")
            if parts:
                row_conditions.append("(" + " AND ".join(parts) + ")")
        if not row_conditions:
            return None
        row_joiner = " AND " if dt.get("row_logic") == "and" else " OR "
        where_parts.append(f"({row_joiner.join(row_conditions)})")
        lines = match_parts.copy()
        lines.append("WHERE " + " AND ".join(where_parts))
        lines.append("RETURN DISTINCT t0.subject AS s")
        return "\n".join(lines)

    @staticmethod
    def validate_table(dt):
        errors = []
        if not dt.get("name"):
            errors.append("Decision table must have a name")
        if not dt.get("target_class"):
            errors.append("Decision table must have a target class")
        if not dt.get("input_columns"):
            errors.append("Decision table must have at least one input column")
        if not dt.get("rows"):
            errors.append("Decision table must have at least one row")
        for i, row in enumerate(dt.get("rows", [])):
            conds = row.get("conditions", [])
            expected = len(dt.get("input_columns", []))
            if len(conds) != expected:
                errors.append(f"Row {i+1}: expected {expected} conditions, got {len(conds)}")
        return errors

    @staticmethod
    def _is_cypher_backend(store):
        try:
            from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
            return isinstance(store, LadybugBase)
        except ImportError:
            return False
