"""SQL data-quality checks against the VIEW triple store.

Extracted from :class:`DigitalTwin` (Fowler Extract Class).
``DigitalTwin`` keeps one-line delegators for callers.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from back.core.helpers import sql_escape as escape_sql_value
from back.core.logging import get_logger
from back.core.w3c.shacl.constants import (
    AGGREGATE_ID_PREFIX,
    DECISION_TABLE_ID_PREFIX,
    SWRL_ID_PREFIX,
    rule_check_id,
)
from back.objects.digitaltwin.constants import RDF_TYPE, RDFS_LABEL

logger = get_logger(__name__)


def _dt():
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    return DigitalTwin


class SqlQualityChecks:
    """Classify predicates and run SHACL / SWRL / decision-table / aggregate SQL checks."""

    #: A failed check used to report only that it failed, which left the reader
    #: no way to tell a broken rule from a warehouse problem without the server
    #: log. Long enough for the engine's error code and its first sentence.
    _SQL_ERROR_MAX_CHARS = 300
    _DQ_PARALLEL_WORKERS = 4
    _MISSING_VIEW = object()

    def __init__(self, domain) -> None:
        self._domain = domain

    def classify_predicates(self, top_predicates: list) -> list:
        """Classify predicates into 'attribute' or 'relationship' kinds."""
        domain = self._domain
        attr_predicates = {
            RDF_TYPE,
            RDFS_LABEL,
            "http://www.w3.org/2000/01/rdf-schema#comment",
            "http://www.w3.org/2000/01/rdf-schema#seeAlso",
        }
        rel_predicates = {"http://www.w3.org/2002/07/owl#sameAs"}

        obj_prop_uris = set()
        data_prop_uris = set()
        for p in domain.get_properties():
            p_uri = p.get("uri", "")
            if p.get("type") == "ObjectProperty":
                obj_prop_uris.add(p_uri)
            else:
                data_prop_uris.add(p_uri)

        classified = []
        for r in top_predicates:
            uri = r["predicate"]
            cnt = int(r["cnt"])
            if uri in attr_predicates or uri in data_prop_uris:
                kind = "attribute"
            elif uri in rel_predicates or uri in obj_prop_uris:
                kind = "relationship"
            else:
                kind = "relationship"
            classified.append({"uri": uri, "count": cnt, "kind": kind})
        return classified

    @staticmethod
    def _count_class_population_sql(
        store, table: str, class_uri: str, cache: dict = None
    ) -> Optional[int]:
        """Count distinct subjects of a given rdf:type class in the triple store."""
        if not class_uri:
            return None
        if cache is None:
            cache = {}
        key = (table, class_uri)
        if key in cache:
            return cache[key]
        try:
            sql = (
                f"SELECT COUNT(DISTINCT subject) AS cnt FROM {table} "
                f"WHERE predicate = '{RDF_TYPE}' AND object = '{escape_sql_value(class_uri)}'"
            )
            rows = store.execute_query(sql) or []
            total = int(rows[0]["cnt"]) if rows else 0
            cache[key] = total
            return total
        except Exception:
            return None

    @staticmethod
    def _enrich_with_population(result: dict, total_population: Optional[int]) -> dict:
        """Add total_population and pass_pct to a check result dict.

        Uses ``violation_total`` (the true, uncapped count) when present,
        falling back to ``len(violations)`` only when the full result set
        was returned without truncation.
        """
        if total_population is not None and total_population > 0:
            vt = result.get("violation_total")
            violation_count = (
                vt if vt is not None else len(result.get("violations") or [])
            )
            pass_pct = max(
                0.0,
                round(
                    ((total_population - violation_count) / total_population) * 100,
                    1,
                ),
            )
            if violation_count > 0:
                pass_pct = min(pass_pct, 99.9)
            result["total_population"] = total_population
            result["pass_pct"] = pass_pct
            if violation_count > 0:
                result["message"] = (
                    f"{violation_count} violations found — "
                    f"{pass_pct}% pass on {total_population} entities"
                )
        return result

    @staticmethod
    def _count_violations_sql(store, sql: str) -> Optional[int]:
        """Run a ``COUNT(*)`` over a violation SQL to get the true total.

        Only called when the ``LIMIT``-ed result hit the cap, so the
        extra round-trip only happens when needed.
        """
        try:
            count_sql = f"SELECT COUNT(*) AS cnt FROM ({sql.rstrip().rstrip(';')})"
            rows = store.execute_query(count_sql)
            if rows:
                return int(rows[0].get("cnt", 0))
        except Exception as exc:
            logger.warning("COUNT(*) fallback failed: %s", exc)
        return None

    @staticmethod
    def _apply_sql_violation_limit(store, sql: str, violation_limit, row_mapper=None):
        """Execute a violation SQL with optional LIMIT, count the true total, and truncate.

        Returns ``(violations, violation_total, status, message)`` or raises
        on unrecoverable query errors.

        *row_mapper* converts raw rows to violation dicts.  Defaults to
        identity (pass rows through unchanged).
        """
        unlimited_sql = sql
        if violation_limit is not None:
            sql = sql.rstrip().rstrip(";") + f" LIMIT {violation_limit + 1}"
        rows = store.execute_query(sql) or []
        violations = [row_mapper(r) for r in rows] if row_mapper else list(rows)
        violation_total = len(violations)
        if violation_limit is not None and violation_total > violation_limit:
            true_count = SqlQualityChecks._count_violations_sql(store, unlimited_sql)
            if true_count is not None:
                violation_total = true_count
            violations = violations[:violation_limit]
        status = "error" if violation_total > 0 else "success"
        msg = (
            f"{violation_total} violations found"
            if violation_total
            else "No violations"
        )
        return violations, violation_total, status, msg

    @staticmethod
    def _load_predicates_from_table(store, table: str) -> set:
        """Query distinct predicates from the triplestore table for URI resolution."""
        try:
            rows = store.execute_query(f"SELECT DISTINCT predicate FROM {table}") or []
            preds = {r.get("predicate", "") for r in rows if r.get("predicate")}
            logger.info("Loaded %d distinct predicates from %s", len(preds), table)
            return preds
        except Exception as exc:
            logger.warning("Could not load predicates from %s: %s", table, exc)
            return set()

    @staticmethod
    def _resolve_shape_uri_for_sql(shape: dict, available_predicates: set) -> dict:
        """Return a shallow copy of *shape* with property_uri resolved against *available_predicates*."""
        from back.core.w3c import resolve_prop_uri

        prop_uri = shape.get("property_uri", "")
        if not prop_uri or not available_predicates:
            return shape

        resolved = resolve_prop_uri(prop_uri, available_predicates)
        if resolved != prop_uri:
            shape = {**shape, "property_uri": resolved}
            logger.info(
                "SQL DQ: resolved property_uri '%s' → '%s' for shape '%s'",
                prop_uri,
                resolved,
                shape.get("label", shape.get("id", "?")),
            )
        return shape

    @staticmethod
    def _sql_error_detail(exc: Exception) -> str:
        """Return the engine's message as a single line fit for a result row."""
        detail = " ".join(str(exc).split())
        if len(detail) > SqlQualityChecks._SQL_ERROR_MAX_CHARS:
            detail = detail[: SqlQualityChecks._SQL_ERROR_MAX_CHARS].rstrip() + "…"
        return detail or exc.__class__.__name__

    @staticmethod
    def _failed_check_result(
        name: str, category: str, check_id, sql: str, exc: Exception
    ) -> dict:
        """Report a check whose query failed, carrying the cause and the SQL."""
        return {
            "name": name,
            "category": category,
            "shape_id": check_id,
            "status": "warning",
            "message": f"Query failed: {SqlQualityChecks._sql_error_detail(exc)}",
            "violations": [],
            "sql": sql,
        }

    @staticmethod
    def _rule_check_id(prefix: str, rule: dict, index: int) -> str:
        """Return the check id for a non-SHACL *rule*.

        A run that selects individual rules hands over a filtered list, which
        renumbers it. The selector stamps ``check_id`` so a rule with no name
        keeps the id it was picked by rather than picking up its neighbour's.
        """
        return rule.get("check_id") or rule_check_id(prefix, rule, index)

    @staticmethod
    def _swrl_target_class_uri(rule, base_uri, uri_map):
        """Return the class URI of the SWRL violation subject."""
        from back.core.reasoning.swrl.SWRLParser import SWRLParser

        ante_atoms = SWRLParser.parse_atoms(rule.get("antecedent", ""))
        cons_atoms = SWRLParser.parse_atoms(rule.get("consequent", ""))
        class_atoms = [
            a
            for a in ante_atoms
            if a["arity"] == 1 and not a.get("builtin") and not a.get("negated")
        ]
        if not class_atoms:
            return None

        viol_var = SWRLParser.determine_violation_subject(cons_atoms, class_atoms)
        for ca in class_atoms:
            if ca["args"][0] == viol_var:
                return SWRLParser.resolve_uri(ca["name"], base_uri, uri_map)
        return SWRLParser.resolve_uri(class_atoms[0]["name"], base_uri, uri_map)

    @staticmethod
    def _swrl_antecedent_population_sql(translator, store, table, params):
        """Count entities matching the SWRL antecedent (the rule's scope)."""
        try:
            count_sql = translator.build_antecedent_count_sql(table, params)
            if not count_sql:
                return None
            rows = store.execute_query(count_sql) or []
            return int(rows[0]["cnt"]) if rows else None
        except Exception:
            return None

    @staticmethod
    def run_sql_checks(
        tm,
        task,
        shapes,
        triplestore_table,
        store,
        t0,
        total,
        swrl_rules=None,
        ontology=None,
        decision_tables=None,
        aggregate_rules=None,
        violation_limit=None,
        parallel=False,
    ):
        """Execute SHACL shapes, SWRL, decision tables and aggregate rules as SQL against the VIEW backend."""
        available_predicates = SqlQualityChecks._load_predicates_from_table(
            store, triplestore_table
        )
        pop_cache = {}
        pop_lock = threading.Lock()
        jobs = SqlQualityChecks._build_check_jobs(
            shapes,
            swrl_rules,
            decision_tables,
            aggregate_rules,
            ontology,
            triplestore_table,
            store,
            available_predicates,
            pop_cache,
            pop_lock,
            violation_limit,
        )
        if parallel and len(jobs) >= 2:
            results = SqlQualityChecks._run_jobs_parallel(
                tm, task, jobs, total, triplestore_table
            )
        else:
            results = SqlQualityChecks._run_jobs_serial(
                tm,
                task,
                jobs,
                total,
                triplestore_table,
                n_shapes=len(shapes or []),
                n_swrl=len(swrl_rules or []),
                n_dt=len(decision_tables or []),
                n_agg=len(aggregate_rules or []),
            )
        if results is None:
            return
        _dt().complete_dq_task(tm, task, results, time.time() - t0)

    @staticmethod
    def _missing_view_message(table: str) -> str:
        return f"View {table} not found. Build first."

    @staticmethod
    def _is_missing_view_error(exc: Exception) -> bool:
        err = str(exc)
        return "TABLE_OR_VIEW_NOT_FOUND" in err or "does not exist" in err.lower()

    @staticmethod
    def _build_check_jobs(
        shapes,
        swrl_rules,
        decision_tables,
        aggregate_rules,
        ontology,
        table,
        store,
        available_predicates,
        pop_cache,
        pop_lock,
        violation_limit,
    ):
        jobs = []
        for idx, shape in enumerate(shapes or []):
            jobs.append(
                (
                    shape.get("label", shape.get("id", f"Shape {idx + 1}")),
                    "shacl",
                    idx,
                    lambda s=shape, i=idx: SqlQualityChecks._eval_shacl_shape(
                        s,
                        i,
                        table,
                        store,
                        available_predicates,
                        pop_cache,
                        pop_lock,
                        violation_limit,
                    ),
                )
            )
        for idx, rule in enumerate(swrl_rules or []):
            if not rule.get("enabled", True):
                continue
            jobs.append(
                (
                    rule.get("name", f"SWRL Rule {idx + 1}"),
                    "swrl",
                    idx,
                    lambda r=rule, i=idx: SqlQualityChecks._eval_swrl_rule(
                        r, i, table, store, ontology, violation_limit
                    ),
                )
            )
        for idx, dt in enumerate(decision_tables or []):
            if not dt.get("enabled", True):
                continue
            jobs.append(
                (
                    dt.get("name", f"Decision Table {idx + 1}"),
                    "dt",
                    idx,
                    lambda d=dt, i=idx: SqlQualityChecks._eval_dt_rule(
                        d,
                        i,
                        table,
                        store,
                        ontology,
                        pop_cache,
                        pop_lock,
                        violation_limit,
                    ),
                )
            )
        for idx, rule in enumerate(aggregate_rules or []):
            if not rule.get("enabled", True):
                continue
            jobs.append(
                (
                    rule.get("name", f"Aggregate Rule {idx + 1}"),
                    "agg",
                    idx,
                    lambda r=rule, i=idx: SqlQualityChecks._eval_agg_rule(
                        r,
                        i,
                        table,
                        store,
                        ontology,
                        pop_cache,
                        pop_lock,
                        violation_limit,
                    ),
                )
            )
        return jobs

    @staticmethod
    def _run_jobs_serial(
        tm, task, jobs, total, table, *, n_shapes, n_swrl, n_dt, n_agg
    ):
        results = []
        for label, kind, idx, fn in jobs:
            if kind == "shacl":
                progress = int((idx / total) * 100) if total else 0
                message = f"Check {idx + 1}/{total}: {label}"
            elif kind == "swrl":
                progress = int(((n_shapes + idx) / total) * 100) if total else 0
                message = f"SWRL {idx + 1}/{n_swrl}: {label}"
            elif kind == "dt":
                progress = int(((n_shapes + n_swrl + idx) / total) * 100) if total else 0
                message = f"DT {idx + 1}/{n_dt}: {label}"
            else:
                progress = (
                    int(((n_shapes + n_swrl + n_dt + idx) / total) * 100) if total else 0
                )
                message = f"Agg {idx + 1}/{n_agg}: {label}"
            tm.update_progress(task.id, progress, message)
            result = fn()
            if result is SqlQualityChecks._MISSING_VIEW:
                tm.fail_task(task.id, SqlQualityChecks._missing_view_message(table))
                return None
            results.append(result)
        return results

    @staticmethod
    def _run_jobs_parallel(tm, task, jobs, total, table):
        workers = min(SqlQualityChecks._DQ_PARALLEL_WORKERS, len(jobs))
        ordered = [None] * len(jobs)
        done = 0
        lock = threading.Lock()
        missing = False
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fn): i
                for i, (_label, _kind, _idx, fn) in enumerate(jobs)
            }
            for fut in as_completed(futures):
                i = futures[fut]
                label = jobs[i][0]
                try:
                    result = fut.result()
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Parallel DQ check '%s' failed: %s", label, exc)
                    result = SqlQualityChecks._failed_check_result(
                        label, "unknown", None, "", exc
                    )
                if result is SqlQualityChecks._MISSING_VIEW:
                    missing = True
                    continue
                ordered[i] = result
                with lock:
                    done += 1
                    progress = int((done / total) * 100) if total else 100
                    tm.update_progress(
                        task.id, progress, f"Check {done}/{total}: {label}"
                    )
        if missing:
            tm.fail_task(task.id, SqlQualityChecks._missing_view_message(table))
            return None
        return ordered

    @staticmethod
    def _eval_shacl_shape(
        shape,
        idx,
        table,
        store,
        available_predicates,
        pop_cache,
        pop_lock,
        violation_limit,
    ):
        from back.core.w3c import SHACLService

        label = shape.get("label", shape.get("id", f"Shape {idx + 1}"))
        cat = shape.get("category", "unknown")
        resolved_shape = SqlQualityChecks._resolve_shape_uri_for_sql(
            shape, available_predicates
        )
        sql = SHACLService.shape_to_sql(resolved_shape, table)
        if not sql:
            return {
                "name": label,
                "category": cat,
                "shape_id": shape.get("id"),
                "status": "info",
                "message": "Cannot translate to SQL",
                "violations": [],
                "sql": "",
            }
        try:
            violations, violation_total, status, msg = (
                SqlQualityChecks._apply_sql_violation_limit(
                    store,
                    sql,
                    violation_limit,
                )
            )
            result = {
                "name": label,
                "category": cat,
                "shape_id": shape.get("id"),
                "status": status,
                "message": msg,
                "violations": violations,
                "sql": sql,
                "violation_total": violation_total,
                "severity": shape.get("severity", "sh:Violation"),
            }
            class_uri = shape.get("target_class_uri", "")
            with pop_lock:
                pop = SqlQualityChecks._count_class_population_sql(
                    store, table, class_uri, pop_cache
                )
            SqlQualityChecks._enrich_with_population(result, pop)
            return result
        except Exception as exc:
            if SqlQualityChecks._is_missing_view_error(exc):
                return SqlQualityChecks._MISSING_VIEW
            logger.exception("SQL DQ check '%s' failed: %s", label, exc)
            return SqlQualityChecks._failed_check_result(
                label, cat, shape.get("id"), sql, exc
            )

    def _eval_swrl_rule(rule, idx, table, store, ontology, violation_limit):
        from back.core.reasoning.swrl.SWRLEngine import SWRLEngine
        from back.core.reasoning.swrl.SWRLSQLTranslator import SWRLSQLTranslator

        label = rule.get("name", f"SWRL Rule {idx + 1}")
        check_id = SqlQualityChecks._rule_check_id(SWRL_ID_PREFIX, rule, idx)
        ontology = ontology or {}
        translator = SWRLSQLTranslator()
        params = {
            "antecedent": rule.get("antecedent", ""),
            "consequent": rule.get("consequent", ""),
            "base_uri": ontology.get("base_uri", ""),
            "uri_map": SWRLEngine(ontology=ontology)._build_uri_map(),
        }
        sql = translator.build_violation_sql(table, params)
        if not sql:
            return {
                "name": label,
                "category": "structural",
                "shape_id": check_id,
                "status": "info",
                "message": "Cannot translate to SQL",
                "violations": [],
                "sql": "",
            }
        try:
            t_rule = time.time()
            violations, violation_total, status, msg = (
                SqlQualityChecks._apply_sql_violation_limit(
                    store,
                    sql,
                    violation_limit,
                    row_mapper=lambda r: {"s": r.get("s", "")},
                )
            )
            result = {
                "name": label,
                "category": "structural",
                "shape_id": check_id,
                "status": status,
                "message": msg,
                "violations": violations,
                "sql": "",
                "severity": "sh:Violation",
                "violation_total": violation_total,
            }
            pop = None
            if violations:
                pop = SqlQualityChecks._swrl_antecedent_population_sql(
                    translator, store, table, params
                )
            SqlQualityChecks._enrich_with_population(result, pop)
            logger.info(
                "SWRL rule '%s': %d violations (%.2fs)",
                label,
                violation_total,
                time.time() - t_rule,
            )
            return result
        except Exception as exc:
            logger.exception("SWRL DQ check '%s' SQL failed: %s", label, exc)
            return SqlQualityChecks._failed_check_result(
                label, "structural", check_id, sql, exc
            )

    @staticmethod
    def _eval_dt_rule(
        dt, idx, table, store, ontology, pop_cache, pop_lock, violation_limit
    ):
        from back.core.reasoning.rules.DecisionTableEngine import DecisionTableEngine

        dt_name = dt.get("name", f"Decision Table {idx + 1}")
        check_id = SqlQualityChecks._rule_check_id(DECISION_TABLE_ID_PREFIX, dt, idx)
        ontology = ontology or {}
        engine = DecisionTableEngine()
        base_uri = ontology.get("base_uri", "")
        resolved = engine._resolve_dt(dt, engine._build_uri_map(ontology), base_uri)
        sql = engine.build_violation_sql(resolved, table, base_uri)
        if not sql:
            return {
                "name": dt_name,
                "category": "conformance",
                "shape_id": check_id,
                "status": "info",
                "message": "Cannot translate to SQL",
                "violations": [],
                "sql": "",
            }
        try:
            t_rule = time.time()
            violations, violation_total, status, msg = (
                SqlQualityChecks._apply_sql_violation_limit(
                    store,
                    sql,
                    violation_limit,
                    row_mapper=lambda r: {"s": r.get("s", "")},
                )
            )
            result = {
                "name": dt_name,
                "category": "conformance",
                "shape_id": check_id,
                "status": status,
                "message": msg,
                "violations": violations,
                "sql": sql,
                "severity": "sh:Violation",
                "violation_total": violation_total,
            }
            if violations:
                with pop_lock:
                    pop = SqlQualityChecks._count_class_population_sql(
                        store, table, resolved.get("target_class_uri", ""), pop_cache
                    )
                SqlQualityChecks._enrich_with_population(result, pop)
            logger.info(
                "DT rule '%s': %d violations (%.2fs)",
                dt_name,
                violation_total,
                time.time() - t_rule,
            )
            return result
        except Exception as exc:
            logger.exception("Decision table DQ check '%s' SQL failed: %s", dt_name, exc)
            return SqlQualityChecks._failed_check_result(
                dt_name, "conformance", check_id, sql, exc
            )

    @staticmethod
    def _eval_agg_rule(
        rule, idx, table, store, ontology, pop_cache, pop_lock, violation_limit
    ):
        from back.core.reasoning.rules.AggregateRuleEngine import AggregateRuleEngine

        agg_name = rule.get("name", f"Aggregate Rule {idx + 1}")
        check_id = SqlQualityChecks._rule_check_id(AGGREGATE_ID_PREFIX, rule, idx)
        ontology = ontology or {}
        engine = AggregateRuleEngine()
        base_uri = ontology.get("base_uri", "")
        resolved = engine._resolve_rule(dict(rule), ontology)
        sql = engine.build_sql(resolved, table, base_uri)
        if not sql:
            return {
                "name": agg_name,
                "category": "conformance",
                "shape_id": check_id,
                "status": "info",
                "message": "Cannot translate to SQL",
                "violations": [],
                "sql": "",
            }
        try:
            t_rule = time.time()
            violations, violation_total, status, msg = (
                SqlQualityChecks._apply_sql_violation_limit(
                    store,
                    sql,
                    violation_limit,
                    row_mapper=lambda r: {
                        "s": r.get("s", ""),
                        "agg_val": r.get("agg_val", ""),
                    },
                )
            )
            result = {
                "name": agg_name,
                "category": "conformance",
                "shape_id": check_id,
                "status": status,
                "message": msg,
                "violations": violations,
                "sql": sql,
                "severity": "sh:Violation",
                "violation_total": violation_total,
            }
            if violations:
                with pop_lock:
                    pop = SqlQualityChecks._count_class_population_sql(
                        store, table, resolved.get("target_class_uri", ""), pop_cache
                    )
                SqlQualityChecks._enrich_with_population(result, pop)
            logger.info(
                "Agg rule '%s': %d violations (%.2fs)",
                agg_name,
                violation_total,
                time.time() - t_rule,
            )
            return result
        except Exception as exc:
            logger.exception(
                "Aggregate rule DQ check '%s' SQL failed: %s", agg_name, exc
            )
            return SqlQualityChecks._failed_check_result(
                agg_name, "conformance", check_id, sql, exc
            )
