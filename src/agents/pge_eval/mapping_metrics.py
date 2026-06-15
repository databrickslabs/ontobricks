"""Stage-2 — mapping-generation quality (deterministic, no LLM).

Computed from a captured PGE ``AgentResult`` artifact (the JSON dumped by
``scripts/smoke_pge.py``).  Stage-2 reads two artifact fields:

* ``mapping_run_log`` — authoritative per-item ``final_status`` (drives the
  completeness ratios and pass/fail accounting).
* ``mapping_evaluations`` — the per-item deterministic ``EvalReport`` dicts
  the run captured (drives the numeric metrics: dangling fractions, id
  integrity, sql-execution failures).  Defect detection keys off the
  structured ``failures[].check`` field, never on prose.

This makes score-only fully offline: no DB round-trip, no LLM, no network.
Live mode produces the same artifact first, then calls this.  Nothing here
is domain-specific.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

from agents.pge_eval.normalize import (
    NormalizedOntology,
    local_name,
    normalize_name,
    normalize_ontology,
)

_PASS_STATUSES = {"PASS", "PRESEEDED"}
_OUT_OF_SCOPE = {"SKIPPED", "FAIL_BUDGET"}

_IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _ratio(num: int, denom: int) -> float:
    return round(num / denom, 6) if denom else 1.0


def _is_rel_report(metrics: dict) -> bool:
    return "dangling_target_pct" in metrics or "total_edges" in metrics


def _is_entity_report(metrics: dict) -> bool:
    return "row_count" in metrics


def _has_sql_failure(report: dict) -> bool:
    for f in report.get("failures", []) or []:
        if f.get("check") == "sql_execution":
            return True
    return False


def _class_dp_counts(norm: NormalizedOntology) -> Dict[str, int]:
    """Map every class identifier (uri + local + name) -> data-property count."""
    out: Dict[str, int] = {}
    for c in norm.classes:
        n = len(c.get("data_properties", []))
        for key in (c.get("uri"), c.get("name")):
            if key:
                out[key] = n
                out[local_name(key)] = n
    return out


def evaluate_mapping(
    artifact: dict,
    ontology: dict,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Run the deterministic Stage-2 checks.

    Returns ``(metrics, extras)`` where ``extras`` carries the mapped
    column footprint reused by ``pipeline.coverage_loss``.
    """
    norm = normalize_ontology(ontology)
    run_log = artifact.get("mapping_run_log", []) or []
    evaluations = artifact.get("mapping_evaluations", {}) or {}
    entity_mappings = artifact.get("entity_mappings", []) or []

    # ---- completeness from the run log -----------------------------
    ent_inscope = ent_pass = 0
    rel_inscope = rel_pass = 0
    for entry in run_log:
        status = entry.get("final_status", "")
        if status in _OUT_OF_SCOPE:
            continue
        if entry.get("kind") == "entity":
            ent_inscope += 1
            if status in _PASS_STATUSES:
                ent_pass += 1
        elif entry.get("kind") == "relationship":
            rel_inscope += 1
            if status in _PASS_STATUSES:
                rel_pass += 1

    entity_completeness = _ratio(ent_pass, ent_inscope)
    relationship_completeness = _ratio(rel_pass, rel_inscope)

    # ---- numeric metrics from captured eval reports ----------------
    dangling_target_pcts: List[float] = []
    dangling_source_pcts: List[float] = []
    id_ok = id_total = 0
    sql_exec_failures = 0
    band_declared = band_compliant = 0

    for report in evaluations.values():
        metrics = report.get("metrics", {}) or {}
        if _has_sql_failure(report):
            sql_exec_failures += 1
        if _is_rel_report(metrics):
            dangling_target_pcts.append(float(metrics.get("dangling_target_pct", 0.0)))
            dangling_source_pcts.append(float(metrics.get("dangling_source_pct", 0.0)))
            band = metrics.get("expected_cross_source_overlap_band")
            if band and isinstance(band, (list, tuple)) and len(band) == 2:
                band_declared += 1
                lo, hi = float(band[0]), float(band[1])
                overlap = float(metrics.get("cross_source_overlap_pct", 0.0))
                if lo <= overlap <= hi:
                    band_compliant += 1
        if _is_entity_report(metrics):
            row_count = int(metrics.get("row_count", 0))
            # A legitimately empty (0-row) entity is id-vacuous: it has no ids to
            # be (non-)unique, so it neither passes nor fails id-integrity.
            # Counting it as a failure would RED a clean run on empty source data.
            if row_count == 0:
                continue
            id_total += 1
            distinct = int(metrics.get("distinct_id_count", 0))
            null_id = int(metrics.get("null_id_count", 0))
            if distinct == row_count and null_id == 0:
                id_ok += 1

    dangling_target_pct_max = round(max(dangling_target_pcts), 6) if dangling_target_pcts else 0.0
    dangling_source_pct_max = round(max(dangling_source_pcts), 6) if dangling_source_pcts else 0.0
    id_integrity = _ratio(id_ok, id_total)

    # cross_source_band_compliance is conditional: only active when >=1 band
    # was declared.  When inactive it reports 1.0 and is flagged so the gate
    # skips it.
    band_active = band_declared > 0
    cross_source_band_compliance = (
        _ratio(band_compliant, band_declared) if band_active else 1.0
    )

    # ---- attribute coverage + mapped footprint ---------------------
    dp_counts = _class_dp_counts(norm)
    attrs_emitted = 0
    dp_denominator = 0
    mapped_cols: Set[str] = set()
    counted_classes: Set[str] = set()
    for em in entity_mappings:
        am = em.get("attribute_mappings", {}) or {}
        attrs_emitted += len(am)
        cls = em.get("ontology_class") or em.get("class_name") or ""
        if cls and cls not in counted_classes:
            counted_classes.add(cls)
            dp_denominator += dp_counts.get(cls, dp_counts.get(local_name(cls), 0))
        for value in am.values():
            for tok in _IDENT_RE.findall(str(value)):
                k = normalize_name(tok)
                if k:
                    mapped_cols.add(k)

    attribute_coverage = _ratio(attrs_emitted, dp_denominator)

    metrics_out: Dict[str, Any] = {
        "entity_completeness": entity_completeness,
        "relationship_completeness": relationship_completeness,
        "attribute_coverage": attribute_coverage,
        "dangling_target_pct_max": dangling_target_pct_max,
        "dangling_source_pct_max": dangling_source_pct_max,
        "id_integrity": id_integrity,
        "sql_exec_failures": sql_exec_failures,
        "cross_source_band_compliance": cross_source_band_compliance,
    }
    extras = {
        "mapped_cols": mapped_cols,
        "band_active": band_active,
    }
    return metrics_out, extras
