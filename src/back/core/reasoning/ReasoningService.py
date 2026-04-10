"""Reasoning service — orchestrates T-Box, SWRL, Graph, Constraint,
Decision Table, SPARQL CONSTRUCT, and Aggregate rule phases."""
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from back.core.reasoning.models import InferredTriple, ReasoningResult, RuleViolation
from back.core.reasoning.constants import RDF_TYPE, RDFS_LABEL, CONSTRAINT_SKIP_TYPES
from shared.config.constants import DEFAULT_GRAPH_NAME

logger = get_logger(__name__)


class ReasoningService:
    """Orchestrate all reasoning phases over a project's ontology and data.

    Phases (in order):

    - **T-Box** — OWL 2 RL closure via owlrl on the ontology graph.
    - **SWRL** — Rule execution (violation and materialisation) on the
      triple-store backend (SQL or Cypher).  Supports built-in predicates
      (comparison, math, string, date) and negated atoms (closed-world).
    - **Graph** — Structural reasoning (transitive closure, symmetric
      expansion, etc.).
    - **Decision Tables** — DMN-style tabular business rules.
    - **SPARQL Rules** — SPARQL CONSTRUCT inference rules.
    - **Aggregate Rules** — GROUP BY / HAVING statistical rules.

    .. note::

        Legacy constraint checks (cardinality, functional, value, global
        rules) have been moved to the **Data Quality** pipeline where they
        are executed as SHACL shapes under the *structural* and
        *consistency* dimensions.
    """

    def __init__(
        self,
        project_session: Any,
        triplestore_backend: Any = None,
    ) -> None:
        self._project = project_session
        self._store = triplestore_backend

    # -- Static helpers -------------------------------------------------------

    @staticmethod
    def _local_name(uri: str) -> str:
        """Extract the local name from a URI (fragment or last path segment)."""
        from back.core.helpers import extract_local_name
        return extract_local_name(uri)

    @staticmethod
    def _namespace_parts(base_uri: str) -> Tuple[str, str]:
        """Derive ``(data_ns, sep)`` from *base_uri*.

        ``data_ns`` is the namespace with a trailing ``/`` used by R2RML
        for data-property predicates.  ``sep`` is the separator (``#`` or
        empty) for building ontology-class URIs.
        """
        sep = "" if base_uri.endswith("#") or base_uri.endswith("/") else "#"
        data_ns = base_uri.rstrip("#").rstrip("/") + "/" if base_uri else ""
        return data_ns, sep

    # -- Public API -------------------------------------------------------

    def _run_phase(
        self,
        result: ReasoningResult,
        phase_name: str,
        runner: Callable[[], ReasoningResult],
        *,
        merge: bool = True,
        extra_stat_keys: Optional[Dict[str, str]] = None,
    ) -> None:
        """Execute a single reasoning phase and fold its output into *result*.

        Only inferred triples are collected; violations are intentionally
        ignored — they belong to the Data Quality pipeline.

        Args:
            phase_name: Prefix for stat keys (e.g. ``"tbox"``).
            runner: Zero-arg callable that returns a :class:`ReasoningResult`.
            merge: If *True*, merge inferred triples from the phase result.
                If *False*, only extend ``inferred_triples``.
            extra_stat_keys: Additional stat keys to copy (source key in phase
                stats -> destination key in result stats).
        """
        try:
            t1 = time.time()
            phase_result = runner()
            ps = phase_result.stats or {}
            result.inferred_triples.extend(phase_result.inferred_triples)
            result.stats[f"{phase_name}_duration_seconds"] = round(time.time() - t1, 3)
            result.stats[f"{phase_name}_inferred_count"] = ps.get(
                "inferred_count", len(phase_result.inferred_triples),
            )
            for src_key, dst_key in (extra_stat_keys or {}).items():
                if src_key in ps:
                    result.stats[dst_key] = ps[src_key]
            if ps.get("skipped"):
                result.stats[f"{phase_name}_skipped"] = True
                result.stats[f"{phase_name}_reason"] = ps.get("reason", "")
        except Exception as e:
            logger.error("%s reasoning failed: %s", phase_name, e)
            result.stats[f"{phase_name}_error"] = str(e)

    def run_full_reasoning(self, options: Optional[Dict] = None) -> ReasoningResult:
        """Run all enabled reasoning phases and merge results."""
        opts = options or {}
        t0 = time.time()
        result = ReasoningResult()
        mat = opts.get("materialize", False)

        phases = [
            ("tbox", "tbox", True, lambda: self.run_tbox_reasoning(), True, {}),
            ("swrl", "swrl", True, lambda: self.run_swrl_rules(materialize=mat), True, {"rules_count": "swrl_rules_count"}),
            ("graph", "graph", True, lambda: self.run_graph_reasoning(opts), False, {}),
            ("decision_tables", "decision_tables", False, lambda: self.run_decision_tables(materialize=mat), False, {"tables_count": "decision_tables_count"}),
            ("sparql_rules", "sparql_rules", False, lambda: self.run_sparql_rules(materialize=mat), True, {"rules_count": "sparql_rules_count"}),
            ("aggregate_rules", "aggregate_rules", False, lambda: self.run_aggregate_rules(materialize=mat), False, {"rules_count": "aggregate_rules_count"}),
        ]

        for opt_key, phase_name, default_on, runner, merge, extra in phases:
            if opts.get(opt_key, default_on):
                self._run_phase(result, phase_name, runner, merge=merge, extra_stat_keys=extra)
            else:
                result.stats[f"{phase_name}_skipped"] = True
                result.stats[f"{phase_name}_reason"] = "Disabled by user"

        duplicates_removed = result.deduplicate()
        if duplicates_removed:
            logger.info("Deduplication removed %d cross-phase duplicate triples", duplicates_removed)

        result.stats["total_duration_seconds"] = round(time.time() - t0, 3)
        result.stats["total_inferred"] = len(result.inferred_triples)
        result.stats["duplicates_removed"] = duplicates_removed
        return result

    def run_tbox_reasoning(self) -> ReasoningResult:
        """Run OWL 2 RL deductive closure on the project ontology."""
        owl_content = self._get_owl_content()
        if not owl_content:
            logger.warning("No OWL content available for T-Box reasoning")
            return ReasoningResult(stats={"phase": "tbox", "skipped": True, "reason": "No OWL content generated yet"})

        try:
            import owlrl as _owlrl  # noqa: F401
        except (ImportError, ModuleNotFoundError):
            logger.warning("owlrl package is not installed — T-Box reasoning unavailable")
            return ReasoningResult(stats={
                "phase": "tbox", "skipped": True,
                "reason": "owlrl package not installed (pip install owlrl)",
            })

        from back.core.reasoning.OWLRLReasoner import OWLRLReasoner
        reasoner = OWLRLReasoner()
        return reasoner.compute_closure(owl_content)

    def run_swrl_rules(self, materialize: bool = False) -> ReasoningResult:
        """Execute SWRL rules against the triple store."""
        rules = self._get_swrl_rules()
        if not rules:
            return ReasoningResult(stats={"phase": "swrl", "skipped": True, "reason": "No SWRL rules defined in ontology"})

        if self._store is None:
            logger.warning("No triple-store backend for SWRL execution")
            return ReasoningResult(stats={"phase": "swrl", "skipped": True, "reason": "No triple-store backend available"})

        from back.core.reasoning.SWRLEngine import SWRLEngine
        ontology = self._get_ontology_dict()
        engine = SWRLEngine(ontology=ontology)
        table_name = self._get_graph_name()
        return engine.execute_rules(
            rules, self._store, table_name, materialize=materialize
        )

    def run_graph_reasoning(self, options: Optional[Dict] = None) -> ReasoningResult:
        """Run graph-structural reasoning (transitive closure, etc.)."""
        if self._store is None:
            return ReasoningResult(stats={"phase": "graph", "skipped": True, "reason": "No triple-store backend available"})

        t0 = time.time()
        result = ReasoningResult()
        table_name = self._get_graph_name()
        ontology = self._get_ontology_dict()

        transitive_props = self._find_properties_by_characteristic(ontology, "transitive")
        logger.info("Graph reasoning: %d transitive properties found%s",
                     len(transitive_props),
                     f" ({', '.join(transitive_props)})" if transitive_props else "")
        for prop_uri in transitive_props:
            try:
                rows = self._store.transitive_closure(table_name, prop_uri)
                logger.info("Transitive closure for %s: %d inferred", prop_uri, len(rows))
                for row in rows:
                    result.inferred_triples.append(InferredTriple(
                        subject=row.get("subject", ""),
                        predicate=row.get("predicate", prop_uri),
                        object=row.get("object", ""),
                        provenance="graph:transitive",
                    ))
            except Exception as e:
                logger.warning("Transitive closure for %s failed: %s", prop_uri, e)

        symmetric_props = self._find_properties_by_characteristic(ontology, "symmetric")
        logger.info("Graph reasoning: %d symmetric properties found%s",
                     len(symmetric_props),
                     f" ({', '.join(symmetric_props)})" if symmetric_props else "")
        for prop_uri in symmetric_props:
            try:
                rows = self._store.symmetric_expand(table_name, prop_uri)
                logger.info("Symmetric expand for %s: %d inferred", prop_uri, len(rows))
                for row in rows:
                    result.inferred_triples.append(InferredTriple(
                        subject=row.get("subject", ""),
                        predicate=row.get("predicate", prop_uri),
                        object=row.get("object", ""),
                        provenance="graph:symmetric",
                    ))
            except Exception as e:
                logger.warning("Symmetric expand for %s failed: %s", prop_uri, e)

        result.stats = {
            "phase": "graph",
            "inferred_count": len(result.inferred_triples),
            "duration_seconds": round(time.time() - t0, 3),
        }
        logger.info("Graph reasoning complete: %d inferred triples (%.3fs)",
                     len(result.inferred_triples), time.time() - t0)
        return result

    def run_constraint_checks(self) -> ReasoningResult:
        """Validate instance data against ontology constraints.

        Supports cardinality, value checks, functional / inverseFunctional,
        and global rules (noOrphans, requireLabels).  Runs Cypher queries
        on the LadybugDB flat store.

        Falls back to legacy ``constraints`` if ``shacl_shapes`` is empty.
        """
        ontology = self._get_ontology_dict()
        constraints = ontology.get("constraints", [])

        shacl_shapes = ontology.get("shacl_shapes", [])
        if shacl_shapes and not constraints:
            try:
                from back.core.w3c.shacl import SHACLService
                svc = SHACLService(base_uri=ontology.get("base_uri", ""))
                constraints = svc.migrate_legacy_constraints([])
                logger.info("Using %d SHACL shapes for constraint checking (via migration path)", len(shacl_shapes))
            except Exception as e:
                logger.warning("Could not migrate SHACL shapes for reasoning: %s", e)

        if not constraints and not shacl_shapes:
            return ReasoningResult(stats={
                "phase": "constraints", "skipped": True,
                "reason": "No constraints or SHACL shapes defined in ontology",
            })
        if self._store is None:
            return ReasoningResult(stats={
                "phase": "constraints", "skipped": True,
                "reason": "No triple-store backend available",
            })

        from back.core.triplestore.ladybugdb.LadybugBase import LadybugBase
        if not isinstance(self._store, LadybugBase):
            return ReasoningResult(stats={
                "phase": "constraints", "skipped": True,
                "reason": "Constraint checks require LadybugDB backend",
            })

        t0 = time.time()
        result = ReasoningResult()
        table_name = self._get_graph_name()
        node = self._store._node_table(table_name)
        conn = self._store._get_connection()
        base_uri = ontology.get("base_uri", "")
        data_ns, sep = self._namespace_parts(base_uri)
        checked = 0

        for c in constraints:
            ctype = (c.get("type") or "").strip()
            if not ctype or ctype in CONSTRAINT_SKIP_TYPES:
                continue
            try:
                violations = self._check_one_constraint(
                    c, ctype, conn, node, data_ns, base_uri, sep,
                )
                result.violations.extend(violations)
                checked += 1
            except Exception as e:
                logger.warning("Constraint check '%s' failed: %s", ctype, e)

        result.stats = {
            "phase": "constraints",
            "violations_count": len(result.violations),
            "constraints_checked": checked,
            "constraints_total": len(constraints),
            "duration_seconds": round(time.time() - t0, 3),
        }
        logger.info("Constraint checks: %d checked, %d violations (%.3fs)",
                     checked, len(result.violations), time.time() - t0)
        return result

    def _check_one_constraint(
        self,
        c: Dict,
        ctype: str,
        conn: Any,
        node: str,
        data_ns: str,
        base_uri: str,
        sep: str,
    ) -> List[RuleViolation]:
        """Dispatch a single constraint to the appropriate checker."""
        cardinality_types = {"minCardinality", "maxCardinality", "exactCardinality"}
        if ctype in cardinality_types:
            return self._check_cardinality(c, ctype, conn, node, data_ns, base_uri, sep, RDF_TYPE)
        if ctype == "functional":
            return self._check_functional(c, conn, node, data_ns, base_uri, sep)
        if ctype == "inverseFunctional":
            return self._check_inverse_functional(c, conn, node, data_ns, base_uri, sep)
        if ctype in ("valueCheck", "entityValueCheck", "entityLabelCheck"):
            return self._check_value(c, conn, node, data_ns, base_uri, sep, RDF_TYPE)
        if ctype == "globalRule":
            rule_name = c.get("ruleName", "")
            if rule_name == "noOrphans":
                return self._check_no_orphans(conn, node, RDF_TYPE)
            if rule_name == "requireLabels":
                return self._check_require_labels(conn, node, RDF_TYPE, RDFS_LABEL)
        return []

    # -- Cardinality -------------------------------------------------------

    def _check_cardinality(
        self, c: Dict, ctype: str, conn: Any, node: str,
        data_ns: str, base_uri: str, sep: str, rdf_type: str,
    ) -> List[RuleViolation]:
        class_uri = c.get("className", "")
        prop_name = c.get("property", "")
        card_val = int(c.get("cardinalityValue", 0))
        prop_uri = self._normalize_property_uri(prop_name, data_ns, base_uri, sep, prop_name)
        if not class_uri or not prop_uri:
            return []

        subjects_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $p AND t.object = $cls "
            f"RETURN DISTINCT t.subject AS s",
            parameters={"p": rdf_type, "cls": class_uri},
        )
        subjects = [row[0] for row in subjects_result]
        violations: List[RuleViolation] = []
        for subj in subjects:
            cnt_result = conn.execute(
                f"MATCH (t:{node}) WHERE t.subject = $s AND t.predicate = $pred "
                f"RETURN COUNT(t) AS cnt",
                parameters={"s": subj, "pred": prop_uri},
            )
            cnt_row = cnt_result.get_next()
            cnt = int(cnt_row[0]) if cnt_row else 0

            violated = False
            if ctype == "minCardinality" and cnt < card_val:
                violated = True
            elif ctype == "maxCardinality" and cnt > card_val:
                violated = True
            elif ctype == "exactCardinality" and cnt != card_val:
                violated = True

            if violated:
                local_subj = self._local_name(subj)
                local_prop = self._local_name(prop_uri)
                violations.append(RuleViolation(
                    rule_name=f"{ctype}({local_prop}={card_val})",
                    subject=subj,
                    message=f"{local_subj} has {cnt} {local_prop} (expected {ctype.replace('Cardinality', '')} {card_val})",
                    check_type="cardinality",
                ))
        return violations

    # -- Functional / Inverse Functional -----------------------------------

    def _check_functional(
        self, c: Dict, conn: Any, node: str,
        data_ns: str, base_uri: str, sep: str,
    ) -> List[RuleViolation]:
        prop_name = c.get("property", "")
        prop_uri = self._normalize_property_uri(prop_name, data_ns, base_uri, sep, prop_name)
        if not prop_uri:
            return []
        rows = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $pred "
            f"RETURN t.subject AS s, COUNT(DISTINCT t.object) AS cnt",
            parameters={"pred": prop_uri},
        )
        rows = [(row[0], int(row[1])) for row in rows]
        violations: List[RuleViolation] = []
        local = self._local_name(prop_uri)
        for subj, cnt in rows:
            if cnt > 1:
                violations.append(RuleViolation(
                    rule_name=f"functional({local})",
                    subject=subj,
                    message=f"Functional property {local} has {cnt} distinct values",
                    check_type="functional",
                ))
        return violations

    def _check_inverse_functional(
        self, c: Dict, conn: Any, node: str,
        data_ns: str, base_uri: str, sep: str,
    ) -> List[RuleViolation]:
        prop_name = c.get("property", "")
        prop_uri = self._normalize_property_uri(prop_name, data_ns, base_uri, sep, prop_name)
        if not prop_uri:
            return []
        rows = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $pred "
            f"RETURN t.object AS o, COUNT(DISTINCT t.subject) AS cnt",
            parameters={"pred": prop_uri},
        )
        rows = [(row[0], int(row[1])) for row in rows]
        violations: List[RuleViolation] = []
        local = self._local_name(prop_uri)
        for obj, cnt in rows:
            if cnt > 1:
                violations.append(RuleViolation(
                    rule_name=f"inverseFunctional({local})",
                    subject=obj,
                    message=f"Inverse-functional property {local}: value mapped by {cnt} subjects",
                    check_type="inverseFunctional",
                ))
        return violations

    # -- Value checks ------------------------------------------------------

    def _check_value(
        self, c: Dict, conn: Any, node: str,
        data_ns: str, base_uri: str, sep: str, rdf_type: str,
    ) -> List[RuleViolation]:
        class_uri = c.get("className", "")
        attr_name = c.get("attributeName", "")
        check_type = c.get("checkType", "")
        check_value = c.get("checkValue", "")
        if not class_uri or not attr_name:
            return []
        attr_uri = self._normalize_property_uri(attr_name, data_ns, base_uri, sep, attr_name)

        subjects_result = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $p AND t.object = $cls "
            f"RETURN DISTINCT t.subject AS s",
            parameters={"p": rdf_type, "cls": class_uri},
        )
        subjects = [row[0] for row in subjects_result]
        if not subjects:
            return []

        violations: List[RuleViolation] = []
        local_attr = self._local_name(attr_uri)

        for subj in subjects:
            vals_result = conn.execute(
                f"MATCH (t:{node}) WHERE t.subject = $s AND t.predicate = $pred "
                f"RETURN t.object AS val",
                parameters={"s": subj, "pred": attr_uri},
            )
            values = [row[0] for row in vals_result]
            local_subj = self._local_name(subj)

            if check_type == "notNull":
                if not values or all(not v for v in values):
                    violations.append(RuleViolation(
                        rule_name=f"notNull({local_attr})",
                        subject=subj,
                        message=f"{local_subj} has no value for {local_attr}",
                        check_type="value",
                    ))
                continue

            for val in values:
                failed = False
                if check_type == "startsWith" and not val.startswith(check_value):
                    failed = True
                elif check_type == "endsWith" and not val.endswith(check_value):
                    failed = True
                elif check_type == "contains" and check_value not in val:
                    failed = True
                elif check_type == "equals" and val != check_value:
                    failed = True
                elif check_type == "notEquals" and val == check_value:
                    failed = True
                elif check_type == "matches":
                    if not re.search(check_value, val):
                        failed = True

                if failed:
                    violations.append(RuleViolation(
                        rule_name=f"{check_type}({local_attr})",
                        subject=subj,
                        message=f"{local_subj}.{local_attr} = '{val}' fails {check_type} '{check_value}'",
                        check_type="value",
                    ))
        return violations

    # -- Global rules ------------------------------------------------------

    def _check_no_orphans(
        self, conn: Any, node: str, rdf_type: str,
    ) -> List[RuleViolation]:
        """Find subjects that appear as subject but have no rdf:type."""
        rows = conn.execute(
            f"MATCH (t:{node}) "
            f"WHERE NOT EXISTS {{ MATCH (t2:{node}) "
            f"WHERE t2.subject = t.subject AND t2.predicate = $p }} "
            f"RETURN DISTINCT t.subject AS s",
            parameters={"p": rdf_type},
        )
        subjects = [row[0] for row in rows]
        return [
            RuleViolation(
                rule_name="noOrphans",
                subject=subj,
                message=f"{self._local_name(subj)} has no rdf:type assertion",
                check_type="global",
            )
            for subj in subjects
        ]

    def _check_require_labels(
        self, conn: Any, node: str, rdf_type: str, rdfs_label: str,
    ) -> List[RuleViolation]:
        """Find typed subjects that have no rdfs:label."""
        rows = conn.execute(
            f"MATCH (t:{node}) WHERE t.predicate = $rdftype "
            f"AND NOT EXISTS {{ MATCH (t2:{node}) "
            f"WHERE t2.subject = t.subject AND t2.predicate = $lbl }} "
            f"RETURN DISTINCT t.subject AS s",
            parameters={"rdftype": rdf_type, "lbl": rdfs_label},
        )
        subjects = [row[0] for row in rows]
        return [
            RuleViolation(
                rule_name="requireLabels",
                subject=subj,
                message=f"{self._local_name(subj)} has no rdfs:label",
                check_type="global",
            )
            for subj in subjects
        ]

    def run_decision_tables(self, materialize: bool = False) -> ReasoningResult:
        """Execute decision tables against the triple store."""
        ontology = self._get_ontology_dict()
        tables = ontology.get("decision_tables", [])
        if not tables:
            return ReasoningResult(stats={
                "phase": "decision_tables", "skipped": True,
                "reason": "No decision tables defined",
            })
        if self._store is None:
            return ReasoningResult(stats={
                "phase": "decision_tables", "skipped": True,
                "reason": "No triple-store backend available",
            })

        from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
        engine = DecisionTableEngine()
        table_name = self._get_graph_name()
        return engine.execute_tables(
            tables, self._store, table_name, ontology,
            materialize=materialize,
        )

    def run_sparql_rules(self, materialize: bool = False) -> ReasoningResult:
        """Execute SPARQL CONSTRUCT rules against the triple store."""
        ontology = self._get_ontology_dict()
        rules = ontology.get("sparql_rules", [])
        if not rules:
            return ReasoningResult(stats={
                "phase": "sparql_rules", "skipped": True,
                "reason": "No SPARQL rules defined",
            })
        if self._store is None:
            return ReasoningResult(stats={
                "phase": "sparql_rules", "skipped": True,
                "reason": "No triple-store backend available",
            })

        from back.core.reasoning.SPARQLRuleEngine import SPARQLRuleEngine
        engine = SPARQLRuleEngine()
        table_name = self._get_graph_name()
        return engine.execute_rules(
            rules, self._store, table_name, ontology,
            materialize=materialize,
        )

    def run_aggregate_rules(self, materialize: bool = False) -> ReasoningResult:
        """Execute aggregate rules against the triple store."""
        ontology = self._get_ontology_dict()
        rules = ontology.get("aggregate_rules", [])
        if not rules:
            return ReasoningResult(stats={
                "phase": "aggregate_rules", "skipped": True,
                "reason": "No aggregate rules defined",
            })
        if self._store is None:
            return ReasoningResult(stats={
                "phase": "aggregate_rules", "skipped": True,
                "reason": "No triple-store backend available",
            })

        from back.core.reasoning.AggregateRuleEngine import AggregateRuleEngine
        engine = AggregateRuleEngine()
        table_name = self._get_graph_name()
        return engine.execute_rules(
            rules, self._store, table_name, ontology,
            materialize=materialize,
        )

    def materialize_inferred(self, result: ReasoningResult) -> int:
        """Insert inferred triples into the triplestore.

        Returns the number of triples inserted.
        """
        if self._store is None or not result.inferred_triples:
            return 0

        table_name = self._get_graph_name()
        triples = [
            {"subject": t.subject, "predicate": t.predicate, "object": t.object}
            for t in result.inferred_triples
            if t.subject and t.subject != "(batch)"
        ]
        if not triples:
            return 0

        count = self._store.insert_triples(table_name, triples)
        logger.info("Materialised %d inferred triples into %s", count, table_name)
        return count

    @staticmethod
    def materialize_to_delta(
        client: Any,
        table_name: str,
        triples: List[Dict[str, str]],
    ) -> int:
        """Materialise inferred triples into a Databricks Delta table.

        Performs CREATE TABLE IF NOT EXISTS, DELETE FROM, then INSERT INTO.

        Args:
            client: A ``DatabricksClient`` instance with SQL Warehouse access.
            table_name: Fully-qualified Delta table (catalog.schema.table).
            triples: List of dicts with ``subject``, ``predicate``, ``object`` keys.

        Returns:
            Number of triples inserted.
        """
        from back.core.triplestore.delta.DeltaTripleStore import DeltaTripleStore

        store = DeltaTripleStore(client)
        logger.info("Materialise to Delta: ensuring table %s exists", table_name)
        store.create_table(table_name)

        logger.info("Materialise to Delta: clearing existing rows in %s", table_name)
        client.execute_statement(f"DELETE FROM {table_name}")

        logger.info("Materialise to Delta: inserting %d triples into %s", len(triples), table_name)
        count = store.insert_triples(table_name, triples)
        logger.info("Materialise to Delta: done — %d triples written to %s", count, table_name)
        return count

    # -- Internal helpers -------------------------------------------------

    def _get_owl_content(self) -> str:
        """Retrieve generated OWL Turtle from the project session."""
        if hasattr(self._project, "generated_owl"):
            return self._project.generated_owl or ""
        data = getattr(self._project, "_data", {})
        return data.get("generated", {}).get("owl", "")

    def _get_swrl_rules(self) -> List[Dict]:
        if hasattr(self._project, "swrl_rules"):
            return self._project.swrl_rules or []
        ontology = getattr(self._project, "ontology", None)
        if isinstance(ontology, dict):
            return ontology.get("swrl_rules", [])
        return []

    def _get_ontology_dict(self) -> Dict:
        ontology = getattr(self._project, "ontology", None)
        if isinstance(ontology, dict):
            return ontology
        if hasattr(self._project, "_data"):
            return self._project._data.get("ontology", {})
        return {}

    def _get_graph_name(self) -> str:
        info = getattr(self._project, "info", None)
        name = info.get("name", DEFAULT_GRAPH_NAME) if isinstance(info, dict) else DEFAULT_GRAPH_NAME
        version = getattr(self._project, "current_version", "1") or "1"
        return f"{name}_V{version}"

    @staticmethod
    def _normalize_property_uri(uri: str, data_ns: str, base_uri: str, sep: str, name: str) -> str:
        """Normalize a property URI to the data namespace used by R2RML.

        Mirrors the logic in ``SWRLEngine._build_uri_map`` so that
        predicate URIs match those stored in the triple store.
        """
        if not uri and name:
            return (data_ns + name) if data_ns else (base_uri + sep + name)
        if data_ns and uri and not uri.startswith(data_ns):
            return data_ns + ReasoningService._local_name(uri)
        return uri

    @staticmethod
    def _find_properties_by_characteristic(ontology: Dict, characteristic: str) -> List[str]:
        """Extract property URIs with *characteristic* (normalized to data namespace)."""
        base_uri = ontology.get("base_uri", "")
        data_ns, sep = ReasoningService._namespace_parts(base_uri)
        result = []
        target = characteristic.lower()
        for prop in ontology.get("properties", []):
            chars = prop.get("characteristics", [])
            if isinstance(chars, list) and target in [c.lower() for c in chars if isinstance(c, str)]:
                name = prop.get("name", "") or prop.get("localName", "")
                uri = ReasoningService._normalize_property_uri(
                    prop.get("uri", ""), data_ns, base_uri, sep, name,
                )
                if uri:
                    result.append(uri)
        return result
