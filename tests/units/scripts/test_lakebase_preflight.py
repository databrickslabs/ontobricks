"""Unit tests for scripts/_lakebase_preflight.py migration expectations."""

from __future__ import annotations

from pathlib import Path

from scripts._internal._lakebase_preflight import (
    EXPECTED_COLUMNS,
    EXPECTED_TABLES,
    STALE_COLUMNS,
)

ROOT = Path(__file__).resolve().parents[3]
LAKEBASE_PERMS = ROOT / "scripts" / "bootstrap" / "lakebase-perms.sh"


def test_registry_migration_expectations_cover_bootstrap_ddl() -> None:
    """Keep preflight objects aligned with bootstrap-lakebase-perms Step 2b."""
    assert ("domain_versions", "status") in EXPECTED_COLUMNS
    assert ("domains", "review_quorum") in EXPECTED_COLUMNS
    assert ("domains", "mcp_policy") in EXPECTED_COLUMNS
    assert ("schedules", "task_type") in EXPECTED_COLUMNS
    assert ("schedules", "target_key") in EXPECTED_COLUMNS
    assert ("schedules", "config") in EXPECTED_COLUMNS
    assert ("schedules", "last_count") in EXPECTED_COLUMNS
    assert ("schedule_runs", "task_type") in EXPECTED_COLUMNS
    assert ("schedule_runs", "target_key") in EXPECTED_COLUMNS
    assert ("schedule_runs", "detail") in EXPECTED_COLUMNS
    assert "domain_edit_locks" in EXPECTED_TABLES
    assert "domain_change_events" in EXPECTED_TABLES
    assert ("domain_comments", "anchor_type") in STALE_COLUMNS


def test_lakebase_perms_pins_pgcrypto_to_public() -> None:
    """pgcrypto must land in public — a graph-schema install is unreachable later."""
    body = LAKEBASE_PERMS.read_text()
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public" in body
    assert "ALTER EXTENSION pgcrypto SET SCHEMA public" in body
    # Must run before the schema-missing early exit so pgcrypto is still installed.
    assert body.index("CREATE EXTENSION IF NOT EXISTS pgcrypto") < body.index(
        "Schema '${SCHEMA}' does not exist"
    )


def test_lakebase_perms_does_not_abort_when_relocation_is_denied() -> None:
    """An app-owned extension cannot be relocated by an admin — warn, don't exit."""
    body = LAKEBASE_PERMS.read_text()
    step = body[body.index("Step 1b") : body.index("Step 2: Postgres schema grants")]
    assert "EXCEPTION WHEN OTHERS THEN" in step
    assert "exit 1" not in step.split("Ensuring pgcrypto")[1]


def test_lakebase_perms_skips_ddl_when_registry_migrations_are_current() -> None:
    """A current app-owned registry must not execute owner-only no-op DDL."""
    body = LAKEBASE_PERMS.read_text()
    migration_step = body[
        body.index("Step 2b: Registry schema migrations") : body.index(
            "for app in", body.index("Step 2b: Registry schema migrations")
        )
    ]

    assert "inspect_migrations" in migration_step
    assert "Registry schema migrations already current; skipping DDL." in migration_step
    assert migration_step.index("inspect_migrations") < migration_step.index(
        'ALTER TABLE "${SCHEMA}".domain_versions'
    )
