"""SQL builders for the saved-pattern catalog (pure string functions).

Kept apart from :class:`SavedPatternStore` so the store stays small and these
statements can be read (and tested) without a warehouse.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

_SAVED_COLUMNS = (
    "(saved_id, saved_label, pattern_id, focus_pattern_id, "
    "anchor_uri, anchor_label, type_uri, hop_count, support, "
    "type_count, relation_count, pattern_count, seed_label, "
    "shape_json, diagram_json, saved_at, anchor_count, population_count)"
)


def as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def sql_lit(value: str) -> str:
    """Quote *value* as a Spark SQL string literal (escapes ``\\`` and ``'``)."""
    escaped = (value or "").replace("\\", "\\\\").replace("'", "''")
    return f"'{escaped}'"


def kpi_sql(out: str, only: Optional[List[str]] = None) -> str:
    """Per pattern: ``relation_count`` (edge rows) and ``type_count`` (distinct types).

    One definition for the eager rows and the bulk INSERT so both agree.
    """
    scope = (
        "pattern_id IN (" + ", ".join(sql_lit(p) for p in only) + ")"
        if only
        else "1 = 1"
    )
    edges = f"{out}_shape_edges"
    return (
        "SELECT r.pattern_id AS pattern_id, r.relation_count AS relation_count, "
        "COALESCE(t.type_count, 0) AS type_count FROM ("
        f"SELECT pattern_id, COUNT(*) AS relation_count FROM {edges} "
        f"WHERE {scope} GROUP BY pattern_id) r LEFT JOIN ("
        "SELECT pattern_id, COUNT(*) AS type_count FROM ("
        f"SELECT pattern_id, parent_type AS t FROM {edges} "
        f"WHERE {scope} AND parent_type IS NOT NULL AND parent_type <> '' "
        f"UNION SELECT pattern_id, child_type AS t FROM {edges} "
        f"WHERE {scope} AND child_type IS NOT NULL AND child_type <> ''"
        ") u GROUP BY pattern_id) t ON t.pattern_id = r.pattern_id"
    )


def insert_catalog_sql(
    saved: str,
    saved_id: str,
    label: str,
    focus_id: str,
    shapes: List[Dict[str, Any]],
) -> str:
    """One INSERT with one SELECT row per pattern (diagram included)."""
    selects = []
    for item in shapes:
        pid = str(item["pattern_id"])
        metrics = item["metrics"]
        shape = item["shape"]
        diagram = shape.get("diagram")
        diagram_json = json.dumps(diagram, separators=(",", ":")) if diagram else ""
        selects.append(
            f"SELECT {sql_lit(saved_id)} AS saved_id, {sql_lit(label)} AS saved_label, "
            f"{sql_lit(pid)} AS pattern_id, {sql_lit(focus_id)} AS focus_pattern_id, "
            "CAST(NULL AS STRING) AS anchor_uri, "
            "CAST(NULL AS STRING) AS anchor_label, "
            "CAST(NULL AS STRING) AS type_uri, "
            f"{int(metrics['hop_count'])} AS hop_count, "
            f"{int(metrics['support'])} AS support, "
            f"{int(metrics['type_count'])} AS type_count, "
            f"{int(metrics['relation_count'])} AS relation_count, "
            f"{int(metrics['pattern_count'])} AS pattern_count, "
            f"{sql_lit(str(shape.get('seed_label') or ''))} AS seed_label, "
            f"{sql_lit(str(shape.get('shape_json') or '[]'))} AS shape_json, "
            f"{sql_lit(diagram_json)} AS diagram_json, "
            "current_timestamp() AS saved_at, "
            f"{int(metrics.get('anchor_count', 0) or 0)} AS anchor_count, "
            f"{int(metrics.get('population_count', 0) or 0)} AS population_count"
        )
    body = "\nUNION ALL\n".join(selects)
    return f"INSERT INTO {saved} {_SAVED_COLUMNS}\n{body}"


def insert_rest_sql(
    saved: str,
    out: str,
    saved_id: str,
    label: str,
    focus_id: str,
    run_count: int,
    *,
    skip: List[str],
    kpi: Optional[str],
    anchor_count: int = 0,
    population_count: int = 0,
) -> str:
    """Catalog rows for every other pattern, computed inside the warehouse.

    No diagram is stored (``diagram_json`` empty): ``SavedPatternStore.shape``
    draws it from the live Detect tables the first time the pattern is opened.
    *kpi* is :func:`kpi_sql`, or ``None`` when the edges table is missing.
    """
    kpi = kpi or (
        "SELECT CAST(NULL AS STRING) AS pattern_id, 0 AS relation_count, "
        "0 AS type_count WHERE 1 = 0"
    )
    exclude = (
        " AND s.pattern_id NOT IN (" + ", ".join(sql_lit(p) for p in skip) + ")"
        if skip
        else ""
    )
    return (
        f"INSERT INTO {saved} {_SAVED_COLUMNS}\n"
        f"SELECT {sql_lit(saved_id)}, {sql_lit(label)}, s.pattern_id, "
        f"{sql_lit(focus_id)}, CAST(NULL AS STRING), CAST(NULL AS STRING), "
        "CAST(NULL AS STRING), CAST(COALESCE(s.hop_count, 0) AS INT), "
        "CAST(s.support AS INT), "
        "CAST(CASE WHEN COALESCE(k.type_count, 0) > 0 "
        "THEN k.type_count ELSE 1 END AS INT), "
        "CAST(COALESCE(k.relation_count, 0) AS INT), "
        f"{int(run_count)}, COALESCE(s.seed_label, ''), "
        f"COALESCE(s.shape_json, '[]'), '', current_timestamp(), "
        f"{int(anchor_count)}, {int(population_count)} "
        f"FROM {out}_shapes s "
        f"LEFT JOIN ({kpi}) k ON k.pattern_id = s.pattern_id "
        f"WHERE s.support > 0{exclude}"
    )
