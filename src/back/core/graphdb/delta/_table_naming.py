"""Unity Catalog FQN helpers for the Databricks (Delta) triple store."""

from __future__ import annotations

import re
from typing import Any

from back.core.helpers.SQLHelpers import SQLHelpers

_SUFFIX_DATA = "_data"
_SUFFIX_INFERRED = "_inferred"
_SUFFIX_GRAPH = "_graph"
_SUFFIX_ANALYTICS = "_analytics"
_SUFFIX_DQ = "_dq"
_SUFFIX_ADJ_OUT = "_adj_out"
_SUFFIX_ADJ_IN = "_adj_in"
_SUFFIX_ENTITY_SEARCH = "_entity_search"
_SUFFIX_ENTITY_SEARCH_ASSERTED = "_entity_search_asserted"
_SUFFIX_PROPS = "_props"
_SUFFIX_CERT_BASE = "_cert_base"
_SUFFIX_CERT_ENRICHED = "_cert_enriched"
_SUFFIX_CERTIFIED = "_certified"
_SUFFIX_CERT_DATA = "_cert_data"
_SUFFIX_CERT_SYNC = "_cert_sync"
# Infix of the Certified Graph's own search-cache companions
# (``<base>_cert_adj_out``, ...): the Original Graph keeps ``<base>_adj_out``.
_INFIX_CERT_CACHE = "_cert"
_CACHE_SUFFIXES = (
    "_entity_search_asserted",
    "_entity_search",
    "_adj_out",
    "_adj_in",
    "_props",
)


def graph_leaf_suffixes() -> tuple[str, ...]:
    """Longest-first suffixes stripped when deriving the mapping-view stem."""
    return tuple(_INFIX_CERT_CACHE + c for c in _CACHE_SUFFIXES) + (
        _SUFFIX_CERT_ENRICHED,
        _SUFFIX_CERT_BASE,
        _SUFFIX_CERTIFIED,
        _SUFFIX_CERT_DATA,
        _SUFFIX_CERT_SYNC,
        _SUFFIX_ENTITY_SEARCH_ASSERTED,
        _SUFFIX_ENTITY_SEARCH,
        _SUFFIX_ADJ_OUT,
        _SUFFIX_ADJ_IN,
        _SUFFIX_PROPS,
        _SUFFIX_ANALYTICS,
        _SUFFIX_INFERRED,
        _SUFFIX_GRAPH,
        _SUFFIX_DATA,
    )


def strip_graph_leaf_suffix(leaf: str) -> str:
    for suffix in graph_leaf_suffixes():
        if leaf.endswith(suffix):
            return leaf[: -len(suffix)]
    return leaf


def view_fqn(domain: Any, settings: Any = None) -> str:
    """R2RML SQL VIEW FQN (``triplestore_<safe>_V<n>``)."""
    return SQLHelpers.effective_view_table(domain, settings)


def _suffixed_fqn(domain: Any, settings: Any, suffix: str) -> str:
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    return f"{view}{suffix}"


def data_table_fqn(domain: Any, settings: Any = None) -> str:
    """Materialized base triples Delta TABLE (``…_data``)."""
    return _suffixed_fqn(domain, settings, _SUFFIX_DATA)


def inferred_table_fqn(domain: Any, settings: Any = None) -> str:
    """Companion table for reasoning / app-written triples."""
    return _suffixed_fqn(domain, settings, _SUFFIX_INFERRED)


def graph_view_fqn(domain: Any, settings: Any = None) -> str:
    """Union VIEW (``…_data`` UNION ALL ``…_inferred``) for graph read queries."""
    return _suffixed_fqn(domain, settings, _SUFFIX_GRAPH)


def cert_base_fqn(domain: Any, settings: Any = None) -> str:
    """Certified Graph base VIEW over the Original ``…_data`` (no copy)."""
    return _suffixed_fqn(domain, settings, _SUFFIX_CERT_BASE)


def cert_enriched_fqn(domain: Any, settings: Any = None) -> str:
    """Certified Graph enrichments TABLE (inferences, rules, cohorts, communities)."""
    return _suffixed_fqn(domain, settings, _SUFFIX_CERT_ENRICHED)


def certified_view_fqn(domain: Any, settings: Any = None) -> str:
    """Certified Graph union VIEW (base ∪ enrichments)."""
    return _suffixed_fqn(domain, settings, _SUFFIX_CERTIFIED)


def cert_data_fqn(domain: Any, settings: Any = None) -> str:
    """Copy of the certified base for a materialized Lakehouse Certified Graph."""
    return _suffixed_fqn(domain, settings, _SUFFIX_CERT_DATA)


def cert_sync_source_fqn(domain: Any, settings: Any = None) -> str:
    """Certified base with ``object_hash``: the Lakeflow source of a synced load."""
    return _suffixed_fqn(domain, settings, _SUFFIX_CERT_SYNC)


def analytics_snapshot_fqn(domain: Any, settings: Any = None) -> str:
    """Disposable Delta TABLE the analytics job reads in view-only mode.

    Only ever exists for the duration of one run: the graph analytics job
    scans its source repeatedly (iterative BFS), which a pass-through view
    would answer by re-running the whole R2RML query every time.
    """
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ANALYTICS}"


def _sanitize_uc_token(value: str) -> str:
    """Keep a Unity Catalog identifier fragment alphanumeric-plus-underscore."""
    token = re.sub(r"[^a-z0-9_]+", "_", (value or "").strip().lower())
    return token.strip("_")


def dq_snapshot_fqn(domain: Any, settings: Any = None, task_id: str = "") -> str:
    """Disposable Delta TABLE for one bulk Data Quality run.

    Named off the R2RML gateway stem plus a sanitized *task_id* so two
    concurrent runs never share a table. Exists only for the duration of
    the run: checks scan this copy instead of re-expanding the VIEW.
    """
    try:
        view = view_fqn(domain, settings)
    except Exception:  # noqa: BLE001
        return ""
    if not view or view.count(".") != 2:
        return ""
    token = _sanitize_uc_token(task_id)
    if not token:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_DQ}_{token}"


def is_certified_relation(table_name: str) -> bool:
    """True when *table_name* is the Certified Graph read relation (``..._certified``)."""
    return str(table_name or "").endswith(_SUFFIX_CERTIFIED)


def cache_suffix(suffix: str, *, certified: bool = False) -> str:
    """Companion suffix of the Original (``_adj_out``) or Certified (``_cert_adj_out``) cache."""
    return (_INFIX_CERT_CACHE + suffix) if certified else suffix


def _companion_fqn(domain: Any, settings: Any, suffix: str, certified: bool) -> str:
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    return f"{view}{cache_suffix(suffix, certified=certified)}"


def adj_out_fqn(domain: Any, settings: Any = None, *, certified: bool = False) -> str:
    """Outgoing adjacency table FQN (``..._adj_out`` / ``..._cert_adj_out``)."""
    return _companion_fqn(domain, settings, _SUFFIX_ADJ_OUT, certified)


def adj_in_fqn(domain: Any, settings: Any = None, *, certified: bool = False) -> str:
    """Incoming adjacency table FQN (``..._adj_in`` / ``..._cert_adj_in``)."""
    return _companion_fqn(domain, settings, _SUFFIX_ADJ_IN, certified)


def entity_search_fqn(domain: Any, settings: Any = None, *, certified: bool = False) -> str:
    """Entity-search table FQN (``..._entity_search`` / ``..._cert_entity_search``)."""
    return _companion_fqn(domain, settings, _SUFFIX_ENTITY_SEARCH, certified)


def entity_search_asserted_fqn(
    domain: Any, settings: Any = None, *, certified: bool = False
) -> str:
    """Asserted-only entity-search table FQN (``..._entity_search_asserted``)."""
    return _companion_fqn(domain, settings, _SUFFIX_ENTITY_SEARCH_ASSERTED, certified)


def props_fqn(domain: Any, settings: Any = None, *, certified: bool = False) -> str:
    """Property-companion table FQN (``..._props`` / ``..._cert_props``)."""
    return _companion_fqn(domain, settings, _SUFFIX_PROPS, certified)


def graph_suffix() -> str:
    return _SUFFIX_GRAPH


def data_suffix() -> str:
    return _SUFFIX_DATA


def inferred_suffix() -> str:
    return _SUFFIX_INFERRED


def adj_out_suffix() -> str:
    return _SUFFIX_ADJ_OUT


def adj_in_suffix() -> str:
    return _SUFFIX_ADJ_IN


def entity_search_suffix() -> str:
    return _SUFFIX_ENTITY_SEARCH


def entity_search_asserted_suffix() -> str:
    return _SUFFIX_ENTITY_SEARCH_ASSERTED


def props_suffix() -> str:
    return _SUFFIX_PROPS
