"""Unity Catalog FQN helpers for the Databricks (Delta) triple store."""

from __future__ import annotations

import re
from typing import Any

from back.core.helpers.SQLHelpers import SQLHelpers

_SUFFIX_DATA = "_data"
_SUFFIX_INFERRED = "_inferred"
_SUFFIX_GRAPH = "_graph"


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
    """Companion table for reasoning / app-written triples."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_INFERRED}"


def graph_view_fqn(domain: Any, settings: Any = None) -> str:
    """Union VIEW (``…_data`` UNION ALL ``…_inferred``) for graph read queries."""
    view = view_fqn(domain, settings)
    if not view or view.count(".") != 2:
        return ""
    cat, sch, base = view.split(".", 2)
    return f"{cat}.{sch}.{base}{_SUFFIX_GRAPH}"


def graph_suffix() -> str:
    return _SUFFIX_GRAPH


def data_suffix() -> str:
    return _SUFFIX_DATA


def inferred_suffix() -> str:
    return _SUFFIX_INFERRED
