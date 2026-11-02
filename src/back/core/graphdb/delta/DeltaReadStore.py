"""Shared read core for the Delta (Unity Catalog) graph engine.

Mixin holding the SQL **read** surface common to both Delta backends:

* :class:`back.core.graphdb.delta.DeltaMaterializedStore.DeltaMaterializedStore`
  (the ``table``/CTAS copy, a :class:`MaterializedGraphBackend`), and
* :class:`back.core.graphdb.delta.DeltaViewStore.DeltaViewStore`
  (the no-copy ``view`` pass-through, a :class:`VirtualBackend`).

It is a plain mixin (not a hat on its own); concrete classes combine it with the
appropriate hat so the write/build surface stays on the materialized side only.
Companion reads (adjacency / entity-search / props) remain available here and are
governed per request by the search-cache option, so a no-copy view can still read
companions a build pipeline materialized alongside it.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.graphdb.adjacency import expand_and_fetch_sql
from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL
from back.core.graphdb.delta import _table_naming
from back.core.graphdb.delta.DeltaOverviewStats import DeltaOverviewStats
from back.core.graphdb.entity_search import (
    is_missing_relation_error,
    is_view_not_table_error,
)
from back.core.graphdb.props import execute_expand_with_props_fallback
from back.core.helpers import validate_table_name


class DeltaReadStore:
    """SQL read surface shared by the Delta materialized and view stores."""

    def __init__(
        self,
        client: Any,
        domain: Any = None,
        settings: Any = None,
    ) -> None:
        self._client = client
        self._domain = domain
        self._settings = settings

    @property
    def query_dialect(self) -> str:
        return "sql"

    def _distinct_count_expr(self, column: str) -> str:
        """Use Spark's HyperLogLog counter for the stats overview.

        ``approx_count_distinct`` is O(1) memory and avoids the full distinct
        shuffle an exact ``COUNT(DISTINCT ...)`` triggers over multi-million-row
        triple tables — turning a multi-second stats query into sub-second. The
        default ~5% relative error is acceptable for an at-a-glance overview.
        """
        return f"approx_count_distinct({column})"

    def get_connection(self) -> Any:
        return self._client

    def close(self) -> None:
        return

    def physical_table_id(self, graph_name: str) -> str:
        """Resolve logical graph name (``Domain_V5``) to a UC FQN for SQL."""
        return self._sql_relation(graph_name)

    def _readable_table_fqn(self) -> str:
        """UC object for read queries (union VIEW, or ``_data`` if VIEW not built yet)."""
        if self._domain is None:
            return ""
        graph = _table_naming.graph_view_fqn(self._domain, self._settings)
        data = _table_naming.data_table_fqn(self._domain, self._settings)
        if graph:
            try:
                if self.table_exists(graph):
                    return graph
            except Exception:  # noqa: BLE001
                pass
        return data or graph or ""

    def _sql_relation(self, table_name: str) -> str:
        """Map Lakebase-style logical names to UC ``…_graph`` / ``…_data`` FQNs."""
        if "." in table_name and table_name.count(".") == 2:
            return table_name
        resolved = self._readable_table_fqn()
        if resolved:
            return resolved
        return table_name

    def sql_flavor(self) -> str:
        return "spark"

    def adjacency_table_ids(self, table_name: str) -> tuple[str, str]:
        if self._domain is not None:
            adj_out = _table_naming.adj_out_fqn(self._domain, self._settings)
            adj_in = _table_naming.adj_in_fqn(self._domain, self._settings)
            if adj_out and adj_in:
                return (adj_out, adj_in)
        if "." not in table_name or table_name.count(".") != 2:
            return ("", "")
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return (
            f"{cat}.{sch}.{base}{_table_naming.adj_out_suffix()}",
            f"{cat}.{sch}.{base}{_table_naming.adj_in_suffix()}",
        )

    def entity_search_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            search = _table_naming.entity_search_fqn(self._domain, self._settings)
            if search:
                return search
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.entity_search_suffix()}"

    def entity_search_asserted_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            search = _table_naming.entity_search_asserted_fqn(
                self._domain, self._settings
            )
            if search:
                return search
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.entity_search_asserted_suffix()}"

    def props_table_id(self, table_name: str) -> str:
        if self._domain is not None:
            props = _table_naming.props_fqn(self._domain, self._settings)
            if props:
                return props
        if "." not in table_name or table_name.count(".") != 2:
            return ""
        cat, sch, base = table_name.split(".", 2)
        base = _table_naming.strip_graph_leaf_suffix(base)
        return f"{cat}.{sch}.{base}{_table_naming.props_suffix()}"

    def synced_table_name(self, table_name: str) -> str:
        """Base data table without inferred companion rows."""
        if table_name.endswith(_table_naming.data_suffix()):
            return table_name
        if self._domain is not None:
            data = _table_naming.data_table_fqn(self._domain, self._settings)
            if data:
                return data
        if "." in table_name and table_name.count(".") == 2:
            cat, sch, base = table_name.split(".", 2)
            if base.endswith(_table_naming.inferred_suffix()):
                base = base[: -len(_table_naming.inferred_suffix())]
            elif base.endswith(_table_naming.graph_suffix()):
                base = base[: -len(_table_naming.graph_suffix())]
            return f"{cat}.{sch}.{base}{_table_naming.data_suffix()}"
        return table_name

    def query_triples(self, table_name: str) -> List[Dict[str, str]]:
        """SELECT all triples."""
        validate_table_name(table_name)
        return self._client.execute_query(
            f"SELECT subject, predicate, object FROM {table_name}"
        )

    def count_triples(self, table_name: str) -> int:
        """Count triples."""
        validate_table_name(table_name)
        rows = self._client.execute_query(f"SELECT COUNT(*) AS cnt FROM {table_name}")
        if rows and len(rows) > 0:
            return int(rows[0].get("cnt", 0))
        return 0

    def describe_detail(self, table_name: str) -> Dict[str, Any]:
        """Return the first ``DESCRIBE DETAIL`` row, or ``{}`` if it fails.

        A view (zero-copy ``…_data``) has no Delta detail, so it also yields ``{}``.
        """
        if not table_name or not table_name.strip():
            return {}
        validate_table_name(table_name)
        try:
            rows = self._client.execute_query(f"DESCRIBE DETAIL {table_name}")
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc) or is_view_not_table_error(exc):
                return {}
            raise
        return rows[0] if rows else {}

    def _graph_index_probe_fqn(self, table_name: str) -> str:
        """Prefer ``…_entity_search`` when *table_name* is the union ``…_graph`` VIEW."""
        leaf = table_name.rsplit(".", 1)[-1]
        if leaf.endswith(_table_naming.graph_suffix()):
            search = self.entity_search_table_id(table_name)
            if search:
                return search
        return table_name

    def table_exists(self, table_name: str) -> bool:
        """Check existence with catalog metadata, not a row scan.

        For the union ``…_graph`` VIEW, describe ``…_entity_search`` first.
        ``DESCRIBE DETAIL`` on a pass-through mapping view can fail the same
        way a ``COUNT(*)`` does (timeout / cloud I/O), which the KG badge
        treats as unknown.
        """
        if not table_name or not table_name.strip():
            return False
        validate_table_name(table_name)
        probe = self._graph_index_probe_fqn(table_name)
        if probe != table_name and self._describe_exists(probe) is True:
            return True
        exists = self._describe_exists(table_name)
        if exists is not None:
            return exists
        return False

    def _describe_exists(self, table_name: str) -> Optional[bool]:
        try:
            self._client.execute_query(f"DESCRIBE DETAIL {table_name}")
            return True
        except Exception as exc:
            if is_view_not_table_error(exc):
                return True
            if is_missing_relation_error(exc):
                return False
            raise

    def get_status(self, table_name: str) -> Dict[str, Any]:
        """Return count/metadata without touching a live mapping VIEW."""
        validate_table_name(table_name)
        probe = self._graph_index_probe_fqn(table_name)
        status: Dict[str, Any] = {"count": 0, "last_modified": None}
        row = self.describe_detail(probe)
        if row:
            status["last_modified"] = row.get("lastModified")
            status["path"] = row.get("path")
            status["format"] = row.get("format")
        total = (
            DeltaOverviewStats(self).triple_count(table_name)
            if probe != table_name
            else None
        )
        status["count"] = total if total else self._companion_has_row_count(probe)
        return status

    def _companion_has_row_count(self, table_name: str) -> int:
        """``1``/``0`` from ``LIMIT 1`` on *table_name* (already a probe FQN)."""
        try:
            rows = self._client.execute_query(f"SELECT 1 FROM {table_name} LIMIT 1")
            return 1 if rows else 0
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc):
                return 0
            raise

    def get_aggregate_stats(self, table_name: str) -> Dict[str, int]:
        return DeltaOverviewStats(self).aggregate(table_name)

    def get_type_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return DeltaOverviewStats(self).type_distribution(table_name)

    def get_predicate_distribution(self, table_name: str) -> List[Dict[str, Any]]:
        return DeltaOverviewStats(self).predicate_distribution(table_name)

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a graph read query, bounded by the graph statement timeout.

        Routed through :meth:`SQLWarehouse.execute_query` with a
        ``statement_timeout_s`` so a runaway traversal (e.g. the recursive BFS
        CTE) is cancelled server-side instead of pinning a warehouse session.
        The unbounded ``client.execute_query`` is intentionally reserved for
        full-graph dumps (``query_triples``) and the build pipeline.
        """
        from back.core.query_limits import get_graph_query_timeout_s

        timeout_s = get_graph_query_timeout_s()
        sql_service = getattr(self._client, "sql", None)
        if sql_service is not None and hasattr(sql_service, "execute_query"):
            return sql_service.execute_query(query, statement_timeout_s=timeout_s)
        return self._client.execute_query(query)

    @staticmethod
    def _expansion_level_ctes(
        relation: str, depth: int, max_entities: int
    ) -> list[str]:
        """Build fixed-depth, de-duplicated BFS levels for one SQL statement."""
        ctes: list[str] = []
        for level in range(1, depth + 1):
            previous = f"level_{level - 1}"
            visited = f"visited_{level}"
            visited_union = " UNION ALL ".join(
                f"SELECT entity FROM level_{prior}" for prior in range(level)
            )
            ctes.extend(
                [
                    f"{visited} AS ({visited_union})",
                    (
                        f"level_{level}_candidates AS ("
                        f"SELECT t.object AS entity FROM {relation} t "
                        f"JOIN {previous} frontier "
                        f"ON t.subject = frontier.entity "
                        f"WHERE t.object LIKE 'http%' "
                        f"AND t.predicate != '{RDF_TYPE}' "
                        f"AND t.predicate != '{RDFS_LABEL}' "
                        f"UNION ALL "
                        f"SELECT t.subject AS entity FROM {relation} t "
                        f"JOIN {previous} frontier "
                        f"ON t.object = frontier.entity "
                        f"WHERE t.predicate != '{RDF_TYPE}' "
                        f"AND t.predicate != '{RDFS_LABEL}')"
                    ),
                    (
                        f"level_{level} AS ("
                        f"SELECT DISTINCT candidate.entity "
                        f"FROM level_{level}_candidates candidate "
                        f"JOIN {relation} typed "
                        f"ON typed.subject = candidate.entity "
                        f"AND typed.predicate = '{RDF_TYPE}' "
                        f"LEFT ANTI JOIN {visited} "
                        f"ON {visited}.entity = candidate.entity "
                        f"LIMIT {max_entities})"
                    ),
                ]
            )
        return ctes

    def expand_and_fetch_subgraph(
        self,
        table_name: str,
        selected_uris: list[str],
        depth: int,
        max_entities: int,
        max_triples: int,
    ) -> dict[str, Any]:
        """Expand and fetch a bounded Delta subgraph in one SQL statement."""
        if not selected_uris:
            raise ValueError("At least one selected URI is required")
        depth = max(0, int(depth))
        max_entities = max(1, int(max_entities))
        max_triples = max(1, int(max_triples))

        relation = self._sql_relation(table_name)
        props = ""
        if self.adjacency_ready(table_name):
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            props = self._props_table_for_read(table_name)
            sql = expand_and_fetch_sql(
                flavor=self.sql_flavor(),
                adj_out=self._sql_relation(adj_out),
                adj_in=self._sql_relation(adj_in),
                spo=relation,
                props=self._sql_relation(props) if props else None,
                selected_uris=selected_uris,
                depth=depth,
                max_entities=max_entities,
                max_triples=max_triples,
                escape=self._sql_escape,
            )
        else:
            seed_values = ", ".join(
                f"('{self._sql_escape(uri)}')" for uri in dict.fromkeys(selected_uris)
            )
            ctes = [f"level_0(entity) AS (VALUES {seed_values})"]
            ctes.extend(self._expansion_level_ctes(relation, depth, max_entities))

            levels = " UNION ALL ".join(
                f"SELECT entity FROM level_{level}" for level in range(depth + 1)
            )
            ctes.extend(
                [
                    (
                        "entity_probe AS ("
                        f"SELECT DISTINCT entity FROM ({levels}) discovered "
                        f"LIMIT {max_entities + 1})"
                    ),
                    (
                        "entities AS ("
                        f"SELECT entity FROM entity_probe LIMIT {max_entities})"
                    ),
                    (
                        "entity_stats AS ("
                        "SELECT COUNT(*) AS _ob_expanded_count FROM entity_probe)"
                    ),
                ]
            )
            sql = (
                "WITH "
                + ", ".join(ctes)
                + " "
                + "SELECT triples.subject, triples.predicate, triples.object, "
                + "stats._ob_expanded_count "
                + f"FROM {relation} triples "
                + "JOIN entities ON entities.entity = triples.subject "
                + "CROSS JOIN entity_stats stats "
                + f"LIMIT {max_triples + 1}"
            )

        if props:
            adj_out, adj_in = self.adjacency_table_ids(table_name)
            fallback_sql = expand_and_fetch_sql(
                flavor=self.sql_flavor(),
                adj_out=self._sql_relation(adj_out),
                adj_in=self._sql_relation(adj_in),
                spo=relation,
                selected_uris=selected_uris,
                depth=depth,
                max_entities=max_entities,
                max_triples=max_triples,
                escape=self._sql_escape,
            )
            rows = execute_expand_with_props_fallback(
                execute_query=self.execute_query,
                sql=sql,
                props_table=props,
                fallback_sql=fallback_sql,
            )
        else:
            rows = self.execute_query(sql) or []
        discovered_count = (
            int(rows[0].get("_ob_expanded_count") or 0)
            if rows
            else min(len(set(selected_uris)), max_entities)
        )
        triple_capped = len(rows) > max_triples
        triples = [
            {
                "subject": row.get("subject", ""),
                "predicate": row.get("predicate", ""),
                "object": row.get("object", ""),
            }
            for row in rows[:max_triples]
        ]
        entity_capped = discovered_count > max_entities
        return {
            "results": triples,
            "count": len(triples),
            "expanded_count": min(discovered_count, max_entities),
            "capped": entity_capped or triple_capped,
            "timeout_capped": False,
        }
