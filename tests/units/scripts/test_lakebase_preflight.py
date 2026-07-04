"""Unit tests for scripts/_lakebase_preflight.py migration expectations."""

from __future__ import annotations

from scripts._lakebase_preflight import (
    EXPECTED_COLUMNS,
    EXPECTED_TABLES,
    STALE_COLUMNS,
)


def test_registry_migration_expectations_cover_bootstrap_ddl() -> None:
    """Keep preflight objects aligned with bootstrap-lakebase-perms Step 2b."""
    assert ("domain_versions", "status") in EXPECTED_COLUMNS
    assert ("domains", "review_quorum") in EXPECTED_COLUMNS
    assert "domain_edit_locks" in EXPECTED_TABLES
    assert "domain_change_events" in EXPECTED_TABLES
    assert ("domain_comments", "anchor_type") in STALE_COLUMNS
