"""Backward-compatible aliases for :class:`SearchCachePolicy`.

Callers may keep importing names from this module. New code should use
:class:`back.core.graphdb.SearchCachePolicy.SearchCachePolicy`.
"""

from back.core.graphdb.SearchCachePolicy import (
    CACHE_BACKEND_MESSAGE,
    CACHE_DISABLED_REFRESH_MESSAGE,
    CACHE_MISSING_MESSAGE,
    GRAPH_CACHE_STATE_DISABLED,
    GRAPH_CACHE_STATE_PENDING,
    GRAPH_CACHE_STATE_READY,
    GRAPH_CACHE_STATES,
    SEARCH_CACHE_SQL_BACKENDS,
    SearchCacheMode,
    SearchCachePolicy,
)

normalize_graph_cache_enabled = SearchCachePolicy.normalize_graph_cache_enabled
normalize_graph_cache_state = SearchCachePolicy.normalize_graph_cache_state
parse_cache_param = SearchCachePolicy.parse_cache_param
resolve_search_cache_mode = SearchCachePolicy.resolve_search_cache_mode
search_cache_usage = SearchCachePolicy.search_cache_usage
apply_search_cache_to_store = SearchCachePolicy.apply_search_cache_to_store
graph_cache_rebuild_allowed = SearchCachePolicy.graph_cache_rebuild_allowed
mark_graph_cache_ready = SearchCachePolicy.mark_graph_cache_ready
mark_graph_cache_pending = SearchCachePolicy.mark_graph_cache_pending
rebuild_graph_cache_if_enabled = SearchCachePolicy.rebuild_graph_cache_if_enabled
state_after_enabled_change = SearchCachePolicy.state_after_enabled_change

__all__ = [
    "CACHE_BACKEND_MESSAGE",
    "CACHE_DISABLED_REFRESH_MESSAGE",
    "CACHE_MISSING_MESSAGE",
    "GRAPH_CACHE_STATE_DISABLED",
    "GRAPH_CACHE_STATE_PENDING",
    "GRAPH_CACHE_STATE_READY",
    "GRAPH_CACHE_STATES",
    "SEARCH_CACHE_SQL_BACKENDS",
    "SearchCacheMode",
    "SearchCachePolicy",
    "apply_search_cache_to_store",
    "graph_cache_rebuild_allowed",
    "mark_graph_cache_pending",
    "mark_graph_cache_ready",
    "normalize_graph_cache_enabled",
    "normalize_graph_cache_state",
    "parse_cache_param",
    "rebuild_graph_cache_if_enabled",
    "resolve_search_cache_mode",
    "search_cache_usage",
    "state_after_enabled_change",
]
