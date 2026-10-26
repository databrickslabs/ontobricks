"""Per-domain search-cache policy for Lakehouse and Lakebase companions.

Companions (adjacency, entity-search, props) are snapshots rebuilt on
Knowledge Graph build / Refresh cache. This class decides whether a read
may use them (``auto``), must use them (``force_on``), or must hit live SPO
(``force_off``). Request ``cache`` always wins over the domain switch.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from back.core.errors import ValidationError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.logging import get_logger

logger = get_logger(__name__)

SearchCacheMode = Literal["auto", "force_on", "force_off"]

GRAPH_CACHE_STATE_READY = "ready"
GRAPH_CACHE_STATE_DISABLED = "disabled"
GRAPH_CACHE_STATE_PENDING = "pending_refresh"
GRAPH_CACHE_STATES = frozenset(
    {
        GRAPH_CACHE_STATE_READY,
        GRAPH_CACHE_STATE_DISABLED,
        GRAPH_CACHE_STATE_PENDING,
    }
)

SEARCH_CACHE_SQL_BACKENDS = frozenset({"lakebase", "databricks"})

CACHE_MISSING_MESSAGE = (
    "Search cache is not available; rebuild the Knowledge Graph or omit cache=true."
)
CACHE_BACKEND_MESSAGE = (
    "Search cache is only available for Lakehouse and Lakebase."
)
CACHE_DISABLED_REFRESH_MESSAGE = "Search cache is disabled for this domain."


class SearchCachePolicy:
    """Resolve, apply, and persist graph companion cache policy for a domain."""

    @staticmethod
    def _domain_info(domain: Any) -> dict:
        info = getattr(domain, "info", None)
        return info if isinstance(info, dict) else {}

    @staticmethod
    def normalize_graph_cache_enabled(raw: Any, *, default: bool = True) -> bool:
        """Coerce a stored/submitted flag; missing values use *default*."""
        if raw is None:
            return default
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            text = raw.strip().lower()
            if text in {"1", "true", "yes", "on"}:
                return True
            if text in {"0", "false", "no", "off"}:
                return False
            return default
        return default

    @staticmethod
    def normalize_graph_cache_state(
        raw: Any, *, enabled: bool, default_when_missing: str
    ) -> str:
        """Return a valid state. Unknown strings follow enabled/disabled."""
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return default_when_missing
        text = str(raw).strip().lower()
        if text in GRAPH_CACHE_STATES:
            if not enabled and text != GRAPH_CACHE_STATE_DISABLED:
                return GRAPH_CACHE_STATE_DISABLED
            if enabled and text == GRAPH_CACHE_STATE_DISABLED:
                return GRAPH_CACHE_STATE_PENDING
            return text
        return GRAPH_CACHE_STATE_PENDING if enabled else GRAPH_CACHE_STATE_DISABLED

    @staticmethod
    def parse_cache_param(raw: Any) -> Optional[bool]:
        """Parse request ``cache``. ``None`` means omit (domain policy)."""
        if raw is None:
            return None
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)) and raw in (0, 1):
            return bool(raw)
        if isinstance(raw, str):
            text = raw.strip().lower()
            if text == "":
                return None
            if text in {"true", "1", "yes", "on"}:
                return True
            if text in {"false", "0", "no", "off"}:
                return False
            raise ValidationError("cache must be true or false")
        # FastAPI ``Query()`` objects (and other non-scalars) mean "omitted"
        # when the route is invoked outside the ASGI stack.
        return None

    @staticmethod
    def resolve_search_cache_mode(
        domain: Any, request_cache: Optional[bool]
    ) -> SearchCacheMode:
        """Map domain policy + optional request override to a store mode."""
        if request_cache is True:
            return "force_on"
        if request_cache is False:
            return "force_off"

        backend = GraphDBFactory._resolve_graph_backend(domain)
        if backend not in SEARCH_CACHE_SQL_BACKENDS:
            return "force_off"

        info = SearchCachePolicy._domain_info(domain)
        enabled = SearchCachePolicy.normalize_graph_cache_enabled(
            info.get("graph_cache_enabled")
        )
        state = SearchCachePolicy.normalize_graph_cache_state(
            info.get("graph_cache_state"),
            enabled=enabled,
            default_when_missing=GRAPH_CACHE_STATE_READY,
        )
        if enabled and state == GRAPH_CACHE_STATE_READY:
            return "auto"
        return "force_off"

    @staticmethod
    def search_cache_usage(store: Any, table_name: str) -> tuple[str, bool]:
        """Return ``(mode, cache_used)`` after a find-style companion walk."""
        getter = getattr(store, "search_cache_mode", None)
        mode = getter() if callable(getter) else "auto"
        if mode not in ("auto", "force_on", "force_off"):
            mode = "auto"
        if mode == "force_off":
            return mode, False
        if mode == "force_on":
            return mode, True
        adj = getattr(store, "adjacency_ready", None)
        search = getattr(store, "entity_search_ready", None)
        used = False
        if callable(adj) and callable(search):
            used = bool(adj(table_name) and search(table_name))
        return mode, used

    @staticmethod
    def apply_search_cache_to_store(
        store: Any, domain: Any, request_cache: Optional[bool]
    ) -> SearchCacheMode:
        """Set the store mode for this request. Raises on ``cache=true`` + Neo4j."""
        mode = SearchCachePolicy.resolve_search_cache_mode(domain, request_cache)
        if mode == "force_on" and not getattr(store, "supports_adjacency", False):
            raise ValidationError(CACHE_BACKEND_MESSAGE)
        setter = getattr(store, "set_search_cache_mode", None)
        if callable(setter):
            setter(mode)
        return mode

    @staticmethod
    def graph_cache_rebuild_allowed(domain: Any) -> bool:
        """Whether companions should be rebuilt for *domain*."""
        info = SearchCachePolicy._domain_info(domain)
        return SearchCachePolicy.normalize_graph_cache_enabled(
            info.get("graph_cache_enabled")
        )

    @staticmethod
    def mark_graph_cache_ready(domain: Any) -> None:
        """Record a successful companion rebuild on *domain.info* when enabled."""
        info = SearchCachePolicy._domain_info(domain)
        if not info:
            return
        if not SearchCachePolicy.normalize_graph_cache_enabled(
            info.get("graph_cache_enabled")
        ):
            return
        info["graph_cache_state"] = GRAPH_CACHE_STATE_READY
        save = getattr(domain, "save", None)
        if callable(save):
            try:
                save()
            except Exception as exc:  # noqa: BLE001
                logger.debug("Could not persist graph_cache_state=ready: %s", exc)

    @staticmethod
    def mark_graph_cache_pending(domain: Any) -> None:
        """Record a failed rebuild while cache is enabled."""
        info = SearchCachePolicy._domain_info(domain)
        if not info:
            return
        if not SearchCachePolicy.normalize_graph_cache_enabled(
            info.get("graph_cache_enabled")
        ):
            return
        info["graph_cache_state"] = GRAPH_CACHE_STATE_PENDING

    @staticmethod
    def rebuild_graph_cache_if_enabled(
        store: Any, table_name: str, domain: Any
    ) -> bool:
        """Run ``rebuild_adjacency`` when the domain switch is on.

        Returns True when a rebuild ran. Raises if rebuild fails (after marking
        pending). Refresh callers that must fail when disabled should check
        :meth:`graph_cache_rebuild_allowed` first.
        """
        if store is None:
            return False
        if not getattr(store, "supports_adjacency", False):
            return False
        if not SearchCachePolicy.graph_cache_rebuild_allowed(domain):
            logger.info(
                "Skipping graph-index rebuild for %s (search cache disabled)",
                table_name,
            )
            return False
        try:
            store.rebuild_adjacency(table_name)
        except Exception:
            SearchCachePolicy.mark_graph_cache_pending(domain)
            raise
        SearchCachePolicy.mark_graph_cache_ready(domain)
        return True

    @staticmethod
    def state_after_enabled_change(*, previous_enabled: bool, enabled: bool) -> str:
        """State to persist when the operator toggles Search cache."""
        if not enabled:
            return GRAPH_CACHE_STATE_DISABLED
        if previous_enabled:
            return GRAPH_CACHE_STATE_PENDING
        return GRAPH_CACHE_STATE_PENDING
