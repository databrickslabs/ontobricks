"""Stage-1 — ontology-generation quality (deterministic, no LLM).

Computed purely from the generated ontology + source metadata.  No mapping
dependency (D2) and no LLM for the deterministic part (§3.2).  The same
checks back the new owl-generator Evaluator stage (§3.5): each issue carries
a concrete ``hint`` that becomes a generator retry_hint.

All metrics are usecase-agnostic: nothing about any particular domain is
hard-coded here.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

from agents.pge_eval.normalize import (
    NormalizedOntology,
    is_surrogate_or_audit,
    local_name,
    normalize_metadata,
    normalize_name,
    normalize_ontology,
)

# Naming conventions (mirror the OWL generator's NAMING RULES, domain-free).
_CLASS_RE = re.compile(r"^[A-Z][A-Za-z0-9]*$")
_PROPERTY_RE = re.compile(r"^[a-z][A-Za-z0-9]*$")


def _issue(check: str, expected: str, observed: str, hint: str) -> Dict[str, str]:
    return {"check": check, "expected": expected, "observed": observed, "hint": hint}


# =====================================================
# Footprint computation (shared with pipeline.coverage_loss)
# =====================================================


def _column_key(table_name: str, column_name: str) -> str:
    return f"{normalize_name(table_name)}::{normalize_name(column_name)}"


def compute_footprint(
    ontology: NormalizedOntology, tables: List[dict]
) -> Dict[str, Any]:
    """Return the ontology footprint over the source metadata.

    A *column* is covered when its normalised name matches some data
    property's normalised name.  A *table* is covered when its name matches
    a class name OR ≥1 of its non-surrogate columns is covered (D3).

    Surrogate/audit columns are excluded from the denominators.
    """
    dp_keys = ontology.all_data_property_keys
    class_keys = ontology.class_name_keys

    total_columns = 0
    covered_columns: Set[str] = set()
    total_tables = len(tables)
    covered_tables: Set[str] = set()

    for t in tables:
        tname = t["name"]
        tkey = normalize_name(local_name(tname))
        table_is_covered = tkey in class_keys
        for col in t["columns"]:
            if is_surrogate_or_audit(col):
                continue
            total_columns += 1
            ckey = _column_key(tname, col)
            if normalize_name(col) in dp_keys:
                covered_columns.add(ckey)
                table_is_covered = True
        if table_is_covered:
            covered_tables.add(tname)

    return {
        "total_tables": total_tables,
        "covered_tables": covered_tables,
        "total_columns": total_columns,
        "covered_columns": covered_columns,
    }


# =====================================================
# Stage-1 metrics + issues
# =====================================================


def evaluate_ontology(
    ontology: dict,
    metadata: dict,
) -> Tuple[Dict[str, Any], List[Dict[str, str]], Dict[str, Any]]:
    """Run the deterministic Stage-1 checks.

    Returns ``(metrics, issues, footprint)``:

    * ``metrics`` — the §3.2 metric block (ratios + absolute counts).
    * ``issues`` — actionable failures (``check/expected/observed/hint``)
      for the owl-gen Evaluator's retry_hints.
    * ``footprint`` — covered tables/columns sets reused by
      ``pipeline.coverage_loss``.
    """
    norm = normalize_ontology(ontology)
    tables = normalize_metadata(metadata)
    footprint = compute_footprint(norm, tables)

    issues: List[Dict[str, str]] = []

    # ---- coverage ratios (Tier-2 warn) -----------------------------
    table_cov = (
        len(footprint["covered_tables"]) / footprint["total_tables"]
        if footprint["total_tables"]
        else 1.0
    )
    column_cov = (
        len(footprint["covered_columns"]) / footprint["total_columns"]
        if footprint["total_columns"]
        else 1.0
    )

    uncovered_tables = [
        t["name"]
        for t in tables
        if t["name"] not in footprint["covered_tables"]
    ]
    for tname in uncovered_tables:
        issues.append(
            _issue(
                "table_footprint_coverage",
                "table maps to a class or contributes >=1 data property",
                "no footprint",
                f"source table '{tname}' has no class and contributes no data "
                "property — model it as a class, attach its columns as data "
                "properties on an existing class, or justify the omission.",
            )
        )

    # ---- orphan classes (Tier-1 absolute = 0) ----------------------
    related: Set[str] = set()
    for op in norm.object_properties:
        for ref in (op.get("domain"), op.get("range")):
            if ref:
                related.add(local_name(ref))
                related.add(str(ref))
    orphan_classes: List[str] = []
    for c in norm.classes:
        has_props = bool(c.get("data_properties"))
        name = c.get("name") or local_name(c.get("uri"))
        in_rel = name in related or local_name(c.get("uri")) in related
        if not has_props and not in_rel:
            orphan_classes.append(name)
            issues.append(
                _issue(
                    "orphan_class_count",
                    "0 orphan classes",
                    name,
                    f"class '{name}' is an orphan (no data properties and no "
                    "object-property domain/range) — attach properties, relate "
                    "it to another class, or remove it.",
                )
            )

    # ---- dangling domain/range (Tier-1 absolute = 0) ---------------
    resolvable = norm.class_resolution_set
    dangling_dr: List[str] = []
    for op in norm.object_properties:
        opname = op.get("name") or local_name(op.get("uri"))
        for role in ("domain", "range"):
            ref = op.get(role)
            if not ref:
                dangling_dr.append(f"{opname}.{role}")
                issues.append(
                    _issue(
                        "dangling_domain_range_count",
                        f"ObjectProperty {role} resolves to a class",
                        f"{opname}.{role}=<missing>",
                        f"ObjectProperty '{opname}' has no {role} — declare an "
                        f"rdfs:{role} pointing at an existing class.",
                    )
                )
                continue
            if ref not in resolvable and local_name(ref) not in resolvable:
                dangling_dr.append(f"{opname}.{role}")
                issues.append(
                    _issue(
                        "dangling_domain_range_count",
                        f"ObjectProperty {role} resolves to a class",
                        f"{opname}.{role}={local_name(ref)}",
                        f"ObjectProperty '{opname}' has {role} "
                        f"'{local_name(ref)}' which resolves to no class — fix "
                        "the reference or add the missing class.",
                    )
                )

    # ---- naming violations (Tier-1 absolute = 0) -------------------
    naming_violations: List[str] = []
    for c in norm.classes:
        nm = local_name(c.get("name") or c.get("uri"))
        if nm and not _CLASS_RE.match(nm):
            naming_violations.append(f"class:{nm}")
            issues.append(
                _issue(
                    "naming_violation_count",
                    "class name is PascalCase [A-Z][A-Za-z0-9]*",
                    nm,
                    f"class '{nm}' violates PascalCase — remove spaces / "
                    "underscores / hyphens and capitalise (e.g. sales_order -> "
                    "SalesOrder).",
                )
            )
    for op in norm.object_properties:
        nm = local_name(op.get("name") or op.get("uri"))
        if nm and not _PROPERTY_RE.match(nm):
            naming_violations.append(f"property:{nm}")
            issues.append(
                _issue(
                    "naming_violation_count",
                    "property name is lowerCamelCase [a-z][A-Za-z0-9]*",
                    nm,
                    f"property '{nm}' violates lowerCamelCase — use "
                    "[a-z][A-Za-z0-9]* with no underscores/hyphens/escapes.",
                )
            )
    # data properties too
    for c in norm.classes:
        for dp in c.get("data_properties", []):
            nm = local_name(dp)
            if nm and not _PROPERTY_RE.match(nm):
                naming_violations.append(f"dataproperty:{nm}")
                issues.append(
                    _issue(
                        "naming_violation_count",
                        "data property name is lowerCamelCase",
                        nm,
                        f"data property '{nm}' violates lowerCamelCase — use "
                        "[a-z][A-Za-z0-9]* with no underscores/hyphens/escapes.",
                    )
                )

    # ---- duplicate classes (Tier-1 absolute = 0) -------------------
    seen: Dict[str, int] = {}
    for c in norm.classes:
        key = normalize_name(local_name(c.get("name") or c.get("uri")))
        if not key:
            continue
        seen[key] = seen.get(key, 0) + 1
    duplicate_class_count = sum(n - 1 for n in seen.values() if n > 1)
    for key, n in seen.items():
        if n > 1:
            issues.append(
                _issue(
                    "duplicate_class_count",
                    "0 duplicate class local names",
                    f"{key} x{n}",
                    f"{n} classes collapse to the local name '{key}' — merge "
                    "them or differentiate their names/definitions.",
                )
            )

    metrics: Dict[str, Any] = {
        "table_footprint_coverage": round(table_cov, 6),
        "column_footprint_coverage": round(column_cov, 6),
        "orphan_class_count": len(orphan_classes),
        "dangling_domain_range_count": len(dangling_dr),
        "naming_violation_count": len(naming_violations),
        "duplicate_class_count": duplicate_class_count,
    }
    return metrics, issues, footprint
