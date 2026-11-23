"""Companion-readiness probing and search-cache mode handling.

Implementation detail of :class:`~back.core.graphdb.interfaces.GraphBackend`;
mixed into it, not a public interface. Governs whether a read uses the
adjacency / entity-search / property companions or falls back to the live SPO
relation, honouring the per-request ``auto`` / ``force_on`` / ``force_off`` mode.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from back.core.logging import get_logger
from back.core.graphdb.entity_search import (
    is_asserted_only_relation,
    is_missing_relation_error,
)

logger = get_logger(__name__)

COMPANION_INVENTORY_KEYS = (
    ("adj_out", "Adjacency out"),
    ("adj_in", "Adjacency in"),
    ("props", "Properties"),
    ("entity_search", "Entity search"),
    ("entity_search_asserted", "Entity search (asserted)"),
)


class SearchCacheMixin:
    """Search-cache mode + companion existence probing (adjacency / entity
    search / props). The companion-id accessors default to empty; copy-owning
    backends override them with real identifiers."""

    def search_cache_mode(self) -> str:
        """Return ``auto``, ``force_on``, or ``force_off`` for this request."""
        return getattr(self, "_search_cache_mode", "auto") or "auto"

    def _companion_tables_exist(self, *table_names: str) -> bool:
        """Return whether every companion exists.

        ``auto`` treats a failed existence probe as ``False`` so Explorer,
        Graph Chat, and MCP fall back to the live SPO relation. ``force_on``
        still surfaces the probe error (or the missing-cache ValidationError).
        """
        try:
            return all(self.table_exists(name) for name in table_names if name)
        except Exception as exc:  # noqa: BLE001
            if self.search_cache_mode() == "force_on":
                raise
            logger.info(
                "Companion existence probe failed; using live graph reads: %s",
                exc,
            )
            return False

    def set_search_cache_mode(self, mode: str) -> None:
        """Pin companion usage for the lifetime of this store instance."""
        if mode not in ("auto", "force_on", "force_off"):
            from back.core.errors import ValidationError

            raise ValidationError("Invalid search cache mode")
        self._search_cache_mode = mode

    def _raise_if_force_on_missing(self, ready: bool) -> None:
        if ready or self.search_cache_mode() != "force_on":
            return
        from back.core.errors import ValidationError
        from back.core.graphdb.search_cache import CACHE_MISSING_MESSAGE

        raise ValidationError(CACHE_MISSING_MESSAGE)

    def _props_table_for_read(self, table_name: str) -> str:
        """Property companion id, or ``\"\"`` when this request must use SPO."""
        mode = self.search_cache_mode()
        if mode == "force_off" or not self.supports_props:
            if mode == "force_on" and not self.supports_props:
                from back.core.errors import ValidationError
                from back.core.graphdb.search_cache import CACHE_BACKEND_MESSAGE

                raise ValidationError(CACHE_BACKEND_MESSAGE)
            return ""
        props = self.props_table_id(table_name)
        if not props:
            self._raise_if_force_on_missing(False)
            return ""
        if mode == "force_on" and not self.table_exists(props):
            self._raise_if_force_on_missing(False)
        return props

    def adjacency_table_ids(self, table_name: str) -> tuple[str, str]:
        """Return ``(adj_out, adj_in)`` table identifiers for *table_name*."""
        return ("", "")

    def rebuild_adjacency(self, table_name: str) -> None:
        """Materialise adj, ``_entity_search``, asserted search, and ``_props``.

        Default is a no-op. SQL backends rebuild companions from the
        reader-facing SPO relation plus an asserted-only entity-search table.
        """

    def adjacency_ready(self, table_name: str) -> bool:
        """Whether adjacency tables exist and neighbour expansion can use them."""
        mode = self.search_cache_mode()
        if mode == "force_off":
            return False
        if not self.supports_adjacency:
            self._raise_if_force_on_missing(False)
            return False
        adj_out, adj_in = self.adjacency_table_ids(table_name)
        if not adj_out or not adj_in:
            self._raise_if_force_on_missing(False)
            return False
        ready = self._companion_tables_exist(adj_out, adj_in)
        self._raise_if_force_on_missing(ready)
        return ready

    def entity_search_table_id(self, table_name: str) -> str:
        """Return the union entity-search table identifier for *table_name*."""
        return ""

    def entity_search_asserted_table_id(self, table_name: str) -> str:
        """Return the asserted-only entity-search table identifier."""
        return ""

    def props_table_id(self, table_name: str) -> str:
        """Return the property-companion table identifier for *table_name*."""
        return ""

    def entity_search_ready(self, table_name: str) -> bool:
        """Whether Preview can use the matching entity-search snapshot."""
        mode = self.search_cache_mode()
        if mode == "force_off":
            return False
        if not self.supports_entity_search:
            self._raise_if_force_on_missing(False)
            return False
        if is_asserted_only_relation(table_name):
            search_table = self.entity_search_asserted_table_id(table_name)
        else:
            search_table = self.entity_search_table_id(table_name)
        ready = bool(search_table) and self._companion_tables_exist(search_table)
        self._raise_if_force_on_missing(ready)
        return ready

    def companion_inventory(self, table_name: str) -> List[Dict[str, Any]]:
        """Row counts of adjacency / props / entity-search companions.

        Used by Graph → Overview. Returns an empty list when this backend
        has no search-cache companions (Neo4j, graph-less domains).
        """
        if not getattr(self, "supports_adjacency", False):
            return []
        adj_out, adj_in = self.adjacency_table_ids(table_name)
        ids = {
            "adj_out": adj_out,
            "adj_in": adj_in,
            "props": self.props_table_id(table_name),
            "entity_search": self.entity_search_table_id(table_name),
            "entity_search_asserted": self.entity_search_asserted_table_id(
                table_name
            ),
        }
        inventory: List[Dict[str, Any]] = []
        for key, label in COMPANION_INVENTORY_KEYS:
            fqn = str(ids.get(key) or "")
            if not fqn:
                inventory.append(
                    {
                        "key": key,
                        "label": label,
                        "table": "",
                        "location": "",
                        "ready": False,
                        "row_count": None,
                    }
                )
                continue
            ready = self._companion_tables_exist(fqn)
            inventory.append(
                {
                    "key": key,
                    "label": label,
                    "table": fqn.rsplit(".", 1)[-1],
                    "location": self.companion_location(fqn),
                    "ready": ready,
                    "row_count": self._count_companion_rows(fqn) if ready else None,
                }
            )
        return inventory

    def companion_location(self, table_fqn: str) -> str:
        """Where a companion lives: its qualifier (``catalog.schema`` on the Lakehouse)."""
        return table_fqn.rsplit(".", 1)[0] if "." in table_fqn else ""

    def _count_companion_rows(self, table_fqn: str) -> Optional[int]:
        """``COUNT(*)`` of a companion, or ``None`` when the relation is missing."""
        if not table_fqn:
            return None
        relation = self._sql_relation(table_fqn)
        try:
            rows = self.execute_query(f"SELECT COUNT(*) AS cnt FROM {relation}") or []
        except Exception as exc:  # noqa: BLE001
            if is_missing_relation_error(exc):
                return None
            logger.info("Companion row count failed for %s: %s", table_fqn, exc)
            return None
        if not rows:
            return None
        return int(rows[0].get("cnt") or 0)
