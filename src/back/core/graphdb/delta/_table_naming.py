"""Unity Catalog FQN helpers for the Databricks (Delta) triple store."""

from __future__ import annotations

import re
from typing import Any

from back.core.helpers.SQLHelpers import SQLHelpers

_SUFFIX_DATA = "_data"
_SUFFIX_INFERRED = "_inferred"  # legacy companion, read only by the migration
_SUFFIX_OVERRIDES = "_overrides"
_SUFFIX_ASSERTED = "_asserted"
_SUFFIX_GRAPH = "_graph"
_SUFFIX_ANALYTICS = "_analytics"
_SUFFIX_DQ = "_dq"
_SUFFIX_ADJ_OUT = "_adj_out"
_SUFFIX_ADJ_IN = "_adj_in"
_SUFFIX_ENTITY_SEARCH = "_entity_search"
_SUFFIX_ENTITY_SEARCH_ASSERTED = "_entity_search_asserted"
_SUFFIX_PROPS = "_props"
# Asserted-graph companions: adjacency / props built from the ``…_asserted`` view
# so the Original Graph can use the search cache without derived triples.
_SUFFIX_ASSERTED_ADJ_OUT = "_asserted_adj_out"
_SUFFIX_ASSERTED_ADJ_IN = "_asserted_adj_in"
_SUFFIX_ASSERTED_PROPS = "_asserted_props"


def graph_leaf_suffixes() -> tuple[str, ...]:
    """Longest-first suffixes stripped when deriving the mapping-view stem."""
    return (
        _SUFFIX_ASSERTED_ADJ_OUT,
        _SUFFIX_ASSERTED_ADJ_IN,
        _SUFFIX_ASSERTED_PROPS,
        _SUFFIX_ENTITY_SEARCH_ASSERTED,
        _SUFFIX_ENTITY_SEARCH,
        _SUFFIX_ADJ_OUT,
        _SUFFIX_ADJ_IN,
        _SUFFIX_PROPS,
        _SUFFIX_ANALYTICS,
        _SUFFIX_OVERRIDES,
        _SUFFIX_ASSERTED,
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


def data_table_fqn(domain: Any, settings: Any = None) -> str:
    """Materialized base triples Delta TABLE (``…_data``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_DATA}"


def inferred_table_fqn(domain: Any, settings: Any = None) -> str:
    """Legacy ``…_inferred`` companion, superseded by ``…_overrides``.

    Only the one-shot migration reads it; nothing writes to it any more.
    """
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_INFERRED}"


def overrides_fqn(domain: Any, settings: Any = None) -> str:
    """Writable overrides TABLE (``…_overrides``): add / del rows over ``…_data``."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_OVERRIDES}"


def asserted_view_fqn(domain: Any, settings: Any = None) -> str:
    """Asserted VIEW: mapped triples plus user overrides, without derived triples."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ASSERTED}"


def graph_view_fqn(domain: Any, settings: Any = None) -> str:
    """Effective-graph VIEW: ``…_data`` plus ``…_overrides`` adds, minus dels."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_GRAPH}"


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


def adj_out_fqn(domain: Any, settings: Any = None) -> str:
    """Outgoing adjacency table FQN (``..._adj_out``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ADJ_OUT}"


def adj_in_fqn(domain: Any, settings: Any = None) -> str:
    """Incoming adjacency table FQN (``..._adj_in``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ADJ_IN}"


def entity_search_fqn(domain: Any, settings: Any = None) -> str:
    """Entity-search table FQN (``..._entity_search``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ENTITY_SEARCH}"


def entity_search_asserted_fqn(domain: Any, settings: Any = None) -> str:
    """Asserted-only entity-search table FQN (``..._entity_search_asserted``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ENTITY_SEARCH_ASSERTED}"


def props_fqn(domain: Any, settings: Any = None) -> str:
    """Property-companion table FQN (``..._props``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_PROPS}"


def asserted_adj_out_fqn(domain: Any, settings: Any = None) -> str:
    """Asserted-graph outgoing adjacency FQN (``..._asserted_adj_out``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ASSERTED_ADJ_OUT}"


def asserted_adj_in_fqn(domain: Any, settings: Any = None) -> str:
    """Asserted-graph incoming adjacency FQN (``..._asserted_adj_in``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ASSERTED_ADJ_IN}"


def asserted_props_fqn(domain: Any, settings: Any = None) -> str:
    """Asserted-graph property-companion FQN (``..._asserted_props``)."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_ASSERTED_PROPS}"


def asserted_adj_out_suffix() -> str:
    return _SUFFIX_ASSERTED_ADJ_OUT


def asserted_adj_in_suffix() -> str:
    return _SUFFIX_ASSERTED_ADJ_IN


def asserted_props_suffix() -> str:
    return _SUFFIX_ASSERTED_PROPS


def graph_suffix() -> str:
    return _SUFFIX_GRAPH


def data_suffix() -> str:
    return _SUFFIX_DATA


def inferred_suffix() -> str:
    return _SUFFIX_INFERRED


def overrides_suffix() -> str:
    return _SUFFIX_OVERRIDES


def asserted_suffix() -> str:
    return _SUFFIX_ASSERTED


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
