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
    """Per-category triples / entities / groups over the derived ``*__overrides`` rows."""

    supports_inferred_breakdown = True

    def get_inferred_breakdown(self, table_name: str, base_uri: str) -> List[Dict[str, Any]]:
        companion = _companion_ddl.companion_phy(table_name)
        validate_table_name(companion)
        relation = self._readable_table_id(companion)
        # The overrides table is created by the first Build or edit. Until then
        # nothing is materialized, so report zero rows instead of failing.
        with self._cursor() as cur:
            if not _companion_ddl.relation_exists(cur, relation):
                return normalise_breakdown([])
        sql = build_breakdown_sql(
            relation,
            base_uri,
            self._sql_escape,
            has_origin=True,
        )
        return normalise_breakdown(self.execute_query(sql))
