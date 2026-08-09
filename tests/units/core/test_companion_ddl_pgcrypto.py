"""Unit tests for Lakebase companion DDL pgcrypto handling.

Regression cover for the stranded-extension bug: ``CREATE EXTENSION`` without a
SCHEMA clause lands in the first ``search_path`` entry (a graph schema), and
``IF NOT EXISTS`` then never repairs it when the graph schema changes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError
from back.core.graphdb.lakebase._companion_ddl import ensure_pgcrypto


def _statements(cur: MagicMock) -> list[str]:
    return [str(c.args[0]) for c in cur.execute.call_args_list]


def test_ensure_pgcrypto_installs_into_public_and_verifies_digest() -> None:
    cur = MagicMock()
    cur.fetchone.return_value = (1,)

    ensure_pgcrypto(cur)

    sql = " ".join(_statements(cur))
    assert "CREATE EXTENSION pgcrypto WITH SCHEMA public" in sql
    assert "proname = 'digest'" in sql


def test_ensure_pgcrypto_relocates_extension_off_the_search_path() -> None:
    """A stranded extension must be moved, not skipped by IF NOT EXISTS."""
    cur = MagicMock()
    cur.fetchone.return_value = (1,)

    ensure_pgcrypto(cur)

    sql = " ".join(_statements(cur))
    assert "ALTER EXTENSION pgcrypto SET SCHEMA public" in sql
    # Only relocate when the extension is unreachable from this connection.
    assert "current_schemas(true)" in sql
    # Fall back to the current schema when public is not writable.
    assert "ALTER EXTENSION pgcrypto SET SCHEMA %I" in sql


def test_ensure_pgcrypto_raises_when_digest_still_unresolvable() -> None:
    cur = MagicMock()
    cur.fetchone.return_value = None

    with pytest.raises(InfrastructureError, match="digest\\(\\) is unavailable"):
        ensure_pgcrypto(cur)
