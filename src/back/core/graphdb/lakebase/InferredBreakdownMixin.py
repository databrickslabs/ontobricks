"""Lakebase mixin: per-category breakdown of materialized inferred triples.

Mixed into :class:`FlatStore`, which supplies ``execute_query``,
``_readable_table_id`` and ``_sql_escape``. Kept out of ``FlatStore`` so that
class stays within its size budget.
"""

from __future__ import annotations

from typing import Any, Dict, List

from back.core.graphdb.inferred_breakdown import build_breakdown_sql, normalise_breakdown
from back.core.graphdb.lakebase import _companion_ddl
from back.core.helpers import validate_table_name


class InferredBreakdownMixin:
    """Per-category triples / entities / groups over the ``*__app`` companion."""

    supports_inferred_breakdown = True

    def get_inferred_breakdown(self, table_name: str, base_uri: str) -> List[Dict[str, Any]]:
        companion = _companion_ddl.companion_phy(table_name)
        validate_table_name(companion)
        sql = build_breakdown_sql(self._readable_table_id(companion), base_uri, self._sql_escape)
        return normalise_breakdown(self.execute_query(sql))
