"""Pure builders for Unity Catalog metric-view SQL.

A metric view cannot be queried with a flat ``SELECT *``: dimensions are
projected directly, measures must be wrapped in ``MEASURE()``, and any query
that projects a measure must ``GROUP BY`` its dimensions. This module builds
that base query from a data-source metadata entry. No I/O.
"""

from typing import Any, Dict, List, Optional

from .identifiers import quote_uc_fqn, quote_uc_identifier


def build_metric_view_base_sql(
    entry: Dict[str, Any], *, selected_columns: Optional[List[str]] = None
) -> str:
    """Build the base ``SELECT`` for a metric view.

    Args:
        entry: A data-source metadata entry with ``full_name`` and
            ``columns`` (each ``{"name", "role"}``; ``role`` defaults to
            ``"dimension"`` when absent).
        selected_columns: Optional subset of column names to project. When
            ``None`` every column is projected.

    Returns:
        A ``SELECT <dims>, MEASURE(<m>) AS <m> ... FROM <fqn> [GROUP BY <dims>]``
        string. Dimension-only projections omit ``GROUP BY``.
    """
    cols = entry.get("columns", []) or []
    if selected_columns is not None:
        wanted = set(selected_columns)
        cols = [c for c in cols if c.get("name") in wanted]

    dims = [c["name"] for c in cols if c.get("role", "dimension") != "measure"]
    measures = [c["name"] for c in cols if c.get("role") == "measure"]

    parts = str(entry.get("full_name", "")).split(".")
    if len(parts) == 3:
        fqn = quote_uc_fqn(*parts)
    else:
        fqn = quote_uc_identifier(entry.get("full_name", ""), role="table")

    select_items = [quote_uc_identifier(d, role="column") for d in dims]
    for m in measures:
        q = quote_uc_identifier(m, role="column")
        select_items.append(f"MEASURE({q}) AS {q}")

    sql = f"SELECT {', '.join(select_items)}\nFROM {fqn}"
    if measures and dims:
        sql += "\nGROUP BY " + ", ".join(
            quote_uc_identifier(d, role="column") for d in dims
        )
    return sql
