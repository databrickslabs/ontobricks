"""Decision table engine — compile tabular business rules to SQL.

All currently supported triple stores (Delta views, Lakebase Postgres) are
SQL-based, so the engine emits a SELECT against the flat triple table.  A
future Cypher / Gremlin backend would extend this engine via a translator
seam similar to :class:`back.core.reasoning.SWRLEngine`.
"""

import time
from typing import Dict, List

from back.core.helpers import sql_numeric
from back.core.logging import get_logger
from back.core.w3c.rdf_utils import uri_local_name
from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation
from back.core.reasoning.constants import (
    RDF_TYPE,
    DT_STRING_OPS,
    DT_NUMERIC_OPS,
    DT_OP_SQL,
)

logger = get_logger(__name__)

ASSIGN_CLASS = "assign_class"


class DecisionTableEngine:
    """Decision table engine — compile tabular business rules to SQL.

    A decision table has:

    - *input_columns*: each maps to a class property (conditions)
    - *output_column*: ``set_value`` writes ``property = value``;
      ``assign_class`` types matches as the class named in ``value``
    - *rows*: each row has condition cells and an action cell
    - *hit_policy*: ``first`` (first matching row wins), ``all`` (all matching
      rows fire), or ``unique`` (an instance matching several rows is reported
      as a conflict and gets no output)

    A column only constrains an instance in rows where its cell holds a
    condition: instances missing that attribute still match ``any`` cells.
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
                local = uri_local_name(uri)
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
            dt["target_class_uri"] = uri_map.get(
                name.lower(), base_uri + sep + name if name else ""
            )
        resolved_cols = []
        for col in dt.get("input_columns", []):
            col = dict(col)
            if not col.get("property_uri"):
                name = col.get("property", "")
                col["property_uri"] = uri_map.get(
                    name.lower(), data_ns + name if name else ""
                )
            resolved_cols.append(col)
        dt["input_columns"] = resolved_cols
        out = dict(dt.get("output_column", {}))
        if not out.get("property_uri") and out.get("property"):
            name = out["property"]
            out["property_uri"] = uri_map.get(
                name.lower(), data_ns + name if name else ""
            )
        dt["output_column"] = out
        if out.get("action") == ASSIGN_CLASS:

            def class_uri(name: str) -> str:
                return uri_map.get(name.lower(), base_uri + sep + name) if name else ""

            out["class_uri"] = class_uri(out.get("value", ""))
            dt["rows"] = [
                {**row, "action_uri": class_uri(row.get("action_value", ""))}
                for row in dt.get("rows", [])
            ]
        return dt

    def execute_tables(self, tables, store, table_name, ontology, materialize=False):
        t0 = time.time()
        result = ReasoningResult()
        base_uri = ontology.get("base_uri", "")
        uri_map = self._build_uri_map(ontology)
        for dt in tables:
            if not dt.get("enabled", True):
                continue
            try:
                resolved = self._resolve_dt(dt, uri_map, base_uri)
                dt_result = self._execute_one(
                    resolved, store, table_name, base_uri, materialize
                )
                result.merge(dt_result)
            except Exception as e:
                logger.error("Decision table '%s' failed: %s", dt.get("name", "?"), e)
                result.violations.append(
                    RuleViolation(
                        rule_name=dt.get("name", "unknown"),
                        subject="",
                        message=f"Execution error: {e}",
                        check_type="decision_table",
                        rule_type="decision_table",
                    )
                )
        result.stats = {
            "phase": "decision_tables",
            "tables_count": len(tables),
            "violations_count": len(result.violations),
            "inferred_count": len(result.inferred_triples),
            "duration_seconds": round(time.time() - t0, 3),
        }
        return result

    def _execute_one(self, dt, store, table_name, base_uri, materialize):
        result = ReasoningResult()
        dt_name = dt.get("name", "unnamed")
        rows = dt.get("rows", [])
        if not rows:
            logger.debug("Decision table '%s': no rows defined, skipping", dt_name)
            return result
        logger.debug(
            "Decision table '%s': target_class_uri=%s, inputs=%s",
            dt_name,
            dt.get("target_class_uri"),
            [c.get("property_uri") for c in dt.get("input_columns", [])],
        )
        out_col = dt.get("output_column") or {}
        assign = out_col.get("action") == ASSIGN_CLASS
        if assign:
            predicate = RDF_TYPE
            default_obj = out_col.get("class_uri", "")
            row_obj_key = "action_uri"
        else:
            predicate = out_col.get("property_uri", "")
            default_obj = out_col.get("value", "")
            row_obj_key = "action_value"
        output = {
            "predicate": predicate,
            "label": "" if assign else out_col.get("property", ""),
            "objects": [default_obj or r.get(row_obj_key, "") for r in rows],
        }
        if dt.get("row_logic", "or") == "and" or not predicate:
            self._execute_combined(dt, store, table_name, base_uri, result, dt_name, output)
        else:
            self._execute_per_row(
                dt, store, table_name, base_uri, result, dt_name, output,
                dt.get("hit_policy", "first"),
            )
        return result

    @staticmethod
    def _describe_output(output, obj):
        if not obj:
            return ""
        if output["predicate"] == RDF_TYPE:
            return f" → {uri_local_name(obj)}"
        return f" → {output['label']} = {obj}" if output["label"] else ""

    def _emit(self, result, dt_name, subj, msg, output, obj, provenance):
        result.violations.append(
            RuleViolation(
                rule_name=dt_name,
                subject=subj,
                message=msg + self._describe_output(output, obj),
                check_type="decision_table",
                rule_type="decision_table",
            )
        )
        if output["predicate"] and obj:
            result.inferred_triples.append(
                InferredTriple(
                    subject=subj,
                    predicate=output["predicate"],
                    object=obj,
                    provenance=provenance,
                    rule_name=dt_name,
                )
            )

    def _execute_combined(self, dt, store, table_name, base_uri, result, dt_name, output):
        query = self.build_violation_sql(dt, store.sql_table_reference(table_name), base_uri)
        if not query:
            logger.warning("Decision table '%s': query builder returned None", dt_name)
            return
        logger.debug("Decision table '%s' query:\n%s", dt_name, query)
        obj = next((o for o in output["objects"] if o), "")
        for subj in self._run_query(store, query, dt_name):
            self._emit(
                result, dt_name, subj, f"Matches decision table '{dt_name}'",
                output, obj, f"decision_table:{dt_name}",
            )

    def _execute_per_row(self, dt, store, table_name, base_uri, result, dt_name, output, hit_policy):
        tbl_ref = store.sql_table_reference(table_name)
        matches: Dict[str, List[int]] = {}
        for ri, row in enumerate(dt.get("rows", [])):
            query = self.build_violation_sql(
                {**dt, "rows": [row], "row_logic": "or"}, tbl_ref, base_uri
            )
            if not query:
                continue
            logger.debug("Decision table '%s' row %d query:\n%s", dt_name, ri + 1, query)
            for subj in self._run_query(store, query, dt_name):
                matches.setdefault(subj, []).append(ri)

        for subj, row_idx in matches.items():
            if hit_policy == "unique" and len(row_idx) > 1:
                rows_txt = ", ".join(str(i + 1) for i in row_idx)
                result.violations.append(
                    RuleViolation(
                        rule_name=dt_name,
                        subject=subj,
                        message=(
                            f"Matches rows {rows_txt} of '{dt_name}' — the Unique hit "
                            "policy allows at most one, so no output is applied"
                        ),
                        check_type="decision_table",
                        rule_type="decision_table",
                    )
                )
                continue
            fired = row_idx[:1] if hit_policy == "first" else row_idx
            for ri in fired:
                self._emit(
                    result, dt_name, subj, f"Row {ri + 1} of '{dt_name}'",
                    output, output["objects"][ri], f"decision_table:{dt_name}:row{ri + 1}",
                )

    @staticmethod
    def _run_query(store, query, dt_name):
        subjects = []
        try:
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
        base_where = [
            f"t0.predicate = '{RDF_TYPE}'",
            f"t0.object = '{self._esc_sql(target_cls_uri)}'",
        ]
        used_cols = set()
        row_conditions = []
        for row in rows:
            conds = row.get("conditions", [])
            parts = []
            for j, cond in enumerate(conds):
                op = cond.get("op", "any")
                val = cond.get("value", "")
                if op == "any" or not val:
                    continue
                if j >= len(inputs) or not inputs[j].get("property_uri"):
                    continue
                sql_op = DT_OP_SQL.get(op)
                if sql_op is None:
                    continue
                alias = f"inp{j}"
                used_cols.add(j)
                if self._is_numeric(val):
                    v_expr = val
                    lhs = (
                        sql_numeric(f"{alias}.object")
                        if op in DT_NUMERIC_OPS
                        else f"{alias}.object"
                    )
                else:
                    v_expr = f"'{self._esc_sql(val.lower())}'"
                    lhs = (
                        f"LOWER({alias}.object)"
                        if op in DT_STRING_OPS
                        else f"{alias}.object"
                    )
                parts.append(f"{lhs} {sql_op.format(v=v_expr)}")
            if parts:
                row_conditions.append("(" + " AND ".join(parts) + ")")
        if not row_conditions:
            return None
        # LEFT JOIN: a column only constrains rows whose cell holds a condition.
        joins = [
            f"LEFT JOIN {table} inp{j} ON inp{j}.subject = t0.subject "
            f"AND inp{j}.predicate = '{self._esc_sql(inputs[j]['property_uri'])}'"
            for j in sorted(used_cols)
        ]
        row_joiner = " AND " if dt.get("row_logic") == "and" else " OR "
        sql = (
            f"SELECT DISTINCT t0.subject AS s\n"
            f"FROM {table} t0\n" + "\n".join(joins) + "\n"
            f"WHERE {' AND '.join(base_where)}\n"
            f"  AND ({row_joiner.join(row_conditions)})"
        )
        return sql

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
                errors.append(
                    f"Row {i+1}: expected {expected} conditions, got {len(conds)}"
                )
        out = dt.get("output_column") or {}
        if out.get("action") == ASSIGN_CLASS and not out.get("value"):
            if not any(r.get("action_value") for r in dt.get("rows", [])):
                errors.append("Assign entity needs the entity to assign")
        return errors
