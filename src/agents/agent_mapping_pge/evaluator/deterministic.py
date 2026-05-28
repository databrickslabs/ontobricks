"""Deterministic (stage-1) evaluator for submitted mappings.

This module is pure-Python and has no LLM dependency.  It runs the
submitted mapping's SQL through a caller-supplied ``execute_sql_fn`` and
checks structural invariants (row count, distinct id count, dangling
foreign-key fractions, etc.).

``execute_sql_fn`` contract::

    def execute_sql_fn(sql: str) -> dict
returning ``{"columns": [...], "rows": [{col: value, ...}, ...]}``.

Important: this is the *full* result set, not the 3-row sample emitted by
:func:`agents.tools.sql.tool_execute_sql`.  The orchestrator (Sprint 7) is
responsible for plugging in a runner that returns full rows — typically a
thin wrapper around ``DatabricksClient.execute_query``.

All checks compute every metric even when some fail; the resulting
:class:`~agents.agent_mapping_pge.contracts.EvalReport` lists every failure
so the Generator/Planner can address them in one shot.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple

from back.core.logging import get_logger
from agents.agent_mapping_pge.contracts import EvalFailure, EvalReport
from agents.agent_mapping_pge.evaluator.report import build_report

logger = get_logger(__name__)

# Thresholds for stage-1 checks.  These are intentionally lax — the
# semantic evaluator (stage 2) catches subtler issues.
_DANGLING_FK_FAIL_THRESHOLD = 0.05
_DANGLING_FK_BUBBLE_THRESHOLD = 0.5


SqlFn = Callable[[str], dict]


# =====================================================
# Helpers
# =====================================================


def _resolve_id_col(mapping: dict, fallback: str = "ID") -> str:
    """Return the column name that holds the entity identifier in the row dicts."""
    return mapping.get("id_column") or fallback


def _extract_id_values(rows: List[dict], id_col: str) -> List[Any]:
    """Pull the id_col value from each row; missing key -> ``None``."""
    return [r.get(id_col) for r in rows]


def _attribute_names(ontology_class: dict) -> List[str]:
    """Ontology attributes can come in a few shapes; normalise to a list of names."""
    attrs = ontology_class.get("attributes") or []
    out: List[str] = []
    for a in attrs:
        if isinstance(a, str):
            out.append(a)
        elif isinstance(a, dict):
            name = a.get("name") or a.get("uri") or a.get("label")
            if name:
                out.append(name)
    return out


def _fail(
    *,
    check: str,
    expected: str,
    observed: str,
    hint: str,
    kind: str = "structural",
) -> EvalFailure:
    return EvalFailure(
        kind=kind, check=check, expected=expected, observed=observed, hint=hint
    )


# =====================================================
# Entity evaluator
# =====================================================


def evaluate_entity_mapping(
    *,
    mapping: dict,
    ontology_class: dict,
    execute_sql_fn: SqlFn,
) -> EvalReport:
    """Run the stage-1 deterministic checks on a submitted entity mapping.

    Args:
        mapping: Submitted entity mapping in the shape produced by
            ``tool_submit_entity_mapping``.
        ontology_class: The ontology-class dict the mapping targets; must
            expose an ``attributes`` list (each item being a name string or
            a dict with a ``name`` key).
        execute_sql_fn: Caller-supplied SQL runner — see module docstring.

    Returns:
        An :class:`EvalReport` summarising the metrics and any failures.
        ``bubble_to_planner`` is set when ``row_count == 0`` (typically
        means the mapping is querying the wrong table altogether).
    """
    class_name = mapping.get("class_name") or ontology_class.get("name") or "?"
    sql = mapping.get("sql_query", "")
    id_col = _resolve_id_col(mapping)
    logger.info(
        "evaluate_entity_mapping: class=%s, id_col=%s, sql_len=%d",
        class_name,
        id_col,
        len(sql),
    )

    result = execute_sql_fn(sql)
    rows = result.get("rows", []) or []
    row_count = len(rows)

    id_values = _extract_id_values(rows, id_col)
    null_id_count = sum(1 for v in id_values if v is None)
    distinct_id_count = len({v for v in id_values if v is not None})

    declared_unmapped = set(mapping.get("unmapped_attributes") or [])
    declared_mapped = set((mapping.get("attribute_mappings") or {}).keys())
    all_attrs = _attribute_names(ontology_class)
    unmapped_attrs = [
        a for a in all_attrs if a not in declared_mapped and a not in declared_unmapped
    ]
    unmapped_pct = (len(unmapped_attrs) / len(all_attrs)) if all_attrs else 0.0

    metrics: Dict[str, Any] = {
        "row_count": row_count,
        "distinct_id_count": distinct_id_count,
        "null_id_count": null_id_count,
        "unmapped_attribute_pct": unmapped_pct,
        "unmapped_attributes": unmapped_attrs,
    }

    failures: List[EvalFailure] = []
    bubble = False

    if row_count == 0:
        failures.append(
            _fail(
                check="row_count",
                expected="> 0",
                observed="0",
                hint=(
                    f"Entity '{class_name}' SQL returned 0 rows. Check the FROM "
                    "table is correct and the WHERE clause is not over-filtering."
                ),
            )
        )
        bubble = True

    if row_count > 0 and distinct_id_count != row_count:
        dupes = row_count - distinct_id_count
        failures.append(
            _fail(
                check="distinct_id_count",
                expected=f"== row_count ({row_count})",
                observed=str(distinct_id_count),
                hint=(
                    f"{dupes} duplicate '{id_col}' value(s) in entity '{class_name}'. "
                    "Add DISTINCT or use a stricter id column."
                ),
            )
        )

    if null_id_count > 0:
        failures.append(
            _fail(
                check="null_id_count",
                expected="== 0",
                observed=str(null_id_count),
                hint=(
                    f"{null_id_count} row(s) have NULL '{id_col}' in entity "
                    f"'{class_name}'. Add 'WHERE {id_col} IS NOT NULL' to the SQL."
                ),
            )
        )

    if unmapped_pct > 0:
        failures.append(
            _fail(
                check="unmapped_attribute_pct",
                expected="== 0",
                observed=f"{unmapped_pct:.3f}",
                hint=(
                    f"{len(unmapped_attrs)} attribute(s) of '{class_name}' are "
                    f"neither in attribute_mappings nor declared in "
                    f"unmapped_attributes: {unmapped_attrs}. Map them, or list "
                    "them explicitly under 'unmapped_attributes'."
                ),
            )
        )

    logger.info(
        "evaluate_entity_mapping: class=%s -> %s (%d failure(s), bubble=%s)",
        class_name,
        "PASS" if not failures else "FAIL",
        len(failures),
        bubble,
    )
    return build_report(
        stage="deterministic",
        metrics=metrics,
        failures=failures,
        bubble_to_planner=bubble,
    )


# =====================================================
# Relationship evaluator
# =====================================================


def _distinct_id_set(
    entity_mapping: dict, execute_sql_fn: SqlFn
) -> set:
    """Materialise the set of valid ids for a given entity mapping."""
    sql = entity_mapping.get("sql_query", "")
    id_col = _resolve_id_col(entity_mapping)
    result = execute_sql_fn(sql)
    rows = result.get("rows", []) or []
    return {r.get(id_col) for r in rows if r.get(id_col) is not None}


def _resolve_edge_columns(mapping: dict) -> Tuple[str, str]:
    """Return ``(source_col, target_col)`` for a relationship mapping."""
    return (
        mapping.get("source_id_column") or "source_id",
        mapping.get("target_id_column") or "target_id",
    )


def evaluate_relationship_mapping(
    *,
    mapping: dict,
    source_entity_mapping: dict,
    target_entity_mapping: dict,
    execute_sql_fn: SqlFn,
    expected_cross_source_overlap_band: Optional[Tuple[float, float]] = None,
) -> EvalReport:
    """Run stage-1 deterministic checks on a relationship mapping.

    Checks:

    * ``total_edges > 0``
    * ``dangling_source_pct < 0.05`` — fraction of source ids that do not
      exist in the source entity's id universe.
    * ``dangling_target_pct < 0.05`` — same for targets.
    * If ``expected_cross_source_overlap_band`` is supplied, the realised
      ``overlap_pct`` (fraction of edges whose target id appears in the
      target entity universe) must fall inside the band.

    ``bubble_to_planner`` is set when ``total_edges == 0``, when the source
    dangling fraction exceeds ``0.5``, or when the target dangling fraction
    exceeds ``0.5`` *and* the realised overlap is materially worse than the
    Planner predicted (either no band was supplied, or the band check
    itself failed).  These cases typically indicate the relationship was
    built off the wrong join key.
    """
    name = mapping.get("property_name") or mapping.get("property") or "?"
    sql = mapping.get("sql_query", "")
    src_col, tgt_col = _resolve_edge_columns(mapping)
    logger.info(
        "evaluate_relationship_mapping: property=%s, src_col=%s, tgt_col=%s",
        name,
        src_col,
        tgt_col,
    )

    edges_result = execute_sql_fn(sql)
    edge_rows = edges_result.get("rows", []) or []
    total_edges = len(edge_rows)

    source_universe = _distinct_id_set(source_entity_mapping, execute_sql_fn)
    target_universe = _distinct_id_set(target_entity_mapping, execute_sql_fn)

    src_values = [r.get(src_col) for r in edge_rows]
    tgt_values = [r.get(tgt_col) for r in edge_rows]

    if total_edges > 0:
        dangling_src = sum(
            1 for v in src_values if v is None or v not in source_universe
        )
        dangling_tgt = sum(
            1 for v in tgt_values if v is None or v not in target_universe
        )
        dangling_src_pct = dangling_src / total_edges
        dangling_tgt_pct = dangling_tgt / total_edges
        overlap_pct = 1.0 - dangling_tgt_pct
    else:
        dangling_src_pct = 0.0
        dangling_tgt_pct = 0.0
        overlap_pct = 0.0

    metrics: Dict[str, Any] = {
        "total_edges": total_edges,
        "dangling_source_pct": dangling_src_pct,
        "dangling_target_pct": dangling_tgt_pct,
        "cross_source_overlap_pct": overlap_pct,
        "source_universe_size": len(source_universe),
        "target_universe_size": len(target_universe),
    }

    failures: List[EvalFailure] = []
    bubble = False

    if total_edges == 0:
        failures.append(
            _fail(
                check="total_edges",
                expected="> 0",
                observed="0",
                hint=(
                    f"Relationship '{name}' produced 0 edges. Confirm the join "
                    "predicate is on the right columns and rows are not being "
                    "filtered away."
                ),
            )
        )
        bubble = True

    if total_edges > 0 and dangling_src_pct >= _DANGLING_FK_FAIL_THRESHOLD:
        failures.append(
            _fail(
                check="dangling_source_pct",
                expected=f"< {_DANGLING_FK_FAIL_THRESHOLD}",
                observed=f"{dangling_src_pct:.3f}",
                hint=(
                    f"{dangling_src_pct:.1%} of source_id values in relationship "
                    f"'{name}' are absent from the mapped source entity. "
                    "Confirm the mapping uses the canonical id column (e.g. NHS "
                    "number), not a trust-local patient_id."
                ),
            )
        )
        if dangling_src_pct > _DANGLING_FK_BUBBLE_THRESHOLD:
            bubble = True

    # When an explicit cross-source overlap band is provided the relationship
    # is *expected* to be partial (e.g. trust_a-only IDs vs the cross-trust
    # canonical universe).  In that case we trust the band check and skip
    # the standard ``dangling_target_pct`` strictness — the partiality is
    # the point.  The catastrophic-dangling bubble below still fires, but
    # only when the band itself ALSO fails (i.e. the realised overlap is
    # materially worse than the Planner predicted).
    if (
        total_edges > 0
        and dangling_tgt_pct >= _DANGLING_FK_FAIL_THRESHOLD
        and expected_cross_source_overlap_band is None
    ):
        failures.append(
            _fail(
                check="dangling_target_pct",
                expected=f"< {_DANGLING_FK_FAIL_THRESHOLD}",
                observed=f"{dangling_tgt_pct:.3f}",
                hint=(
                    f"{dangling_tgt_pct:.1%} of target_id values in relationship "
                    f"'{name}' are absent from the mapped target entity. "
                    "Confirm the target join column matches the target entity's "
                    "canonical id."
                ),
            )
        )

    band_failed = False
    if expected_cross_source_overlap_band is not None:
        lo, hi = expected_cross_source_overlap_band
        if not (lo <= overlap_pct <= hi):
            band_failed = True
            failures.append(
                _fail(
                    check="cross_source_overlap_pct",
                    expected=f"in [{lo:.3f}, {hi:.3f}]",
                    observed=f"{overlap_pct:.3f}",
                    hint=(
                        f"Cross-source overlap for '{name}' is {overlap_pct:.1%}, "
                        f"outside the expected band [{lo:.1%}, {hi:.1%}]. "
                        "Check the join key and the source/target trust assignments."
                    ),
                )
            )

    # Bubble-to-planner on catastrophic target-dangling, with a band-aware gate.
    #
    # * Band absent + dangling > 0.5: the strict dangling_target_pct failure
    #   above already fired; we just flip the bubble flag (no new row needed).
    # * Band present + band PASSED: the Planner predicted this overlap and
    #   was right — do NOT bubble, even if dangling > 0.5 (the partiality
    #   was expected).
    # * Band present + band FAILED + dangling > 0.5: the realised overlap
    #   is materially worse than predicted.  Bubble, and emit a dedicated
    #   ``dangling_target_pct_catastrophic`` failure so the FAIL report has
    #   a concrete structural row alongside the band-check failure.
    if total_edges > 0 and dangling_tgt_pct > _DANGLING_FK_BUBBLE_THRESHOLD:
        if expected_cross_source_overlap_band is None:
            bubble = True
        elif band_failed:
            bubble = True
            failures.append(
                _fail(
                    check="dangling_target_pct_catastrophic",
                    expected=f"<= {_DANGLING_FK_BUBBLE_THRESHOLD}",
                    observed=f"{dangling_tgt_pct:.3f}",
                    hint=(
                        f"{dangling_tgt_pct:.1%} of target_id values in "
                        f"relationship '{name}' are absent from the mapped "
                        "target entity AND the realised overlap is outside "
                        "the predicted band.  Re-plan the join key and the "
                        "source/target trust assignments."
                    ),
                )
            )

    logger.info(
        "evaluate_relationship_mapping: %s -> %s (%d failure(s), bubble=%s)",
        name,
        "PASS" if not failures else "FAIL",
        len(failures),
        bubble,
    )
    return build_report(
        stage="deterministic",
        metrics=metrics,
        failures=failures,
        bubble_to_planner=bubble,
    )
