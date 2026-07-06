"""Lazy-import gate for ``psycopg``.

Both the registry store and the graph engine are import-safe without
``psycopg`` installed — the dependency is only required when a Lakebase
connection is actually opened. This module centralises the import so the
error message (and the extra install hint) lives in exactly one place.
"""

from __future__ import annotations

from typing import Any, Tuple

_INSTALL_HINT = (
    "psycopg is required for the Lakebase backend. Install with "
    "``uv sync --extra lakebase`` (or ``pip install .[lakebase]``)."
)


def require_psycopg() -> Tuple[Any, Any]:
    """Return ``(psycopg, psycopg.rows.dict_row)`` or raise ``ImportError``.

    Raising :class:`ImportError` keeps the behaviour compatible with the
    graph package ``__init__`` which probes availability with a bare
    ``except ImportError``.
    """
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise ImportError(_INSTALL_HINT) from exc
    return psycopg, dict_row
