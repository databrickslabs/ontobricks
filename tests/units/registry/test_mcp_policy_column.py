"""``domains.mcp_policy`` migration and round-trip contracts.

The realistic failure mode for a new registry column is not bad SQL — it is
forgetting one of the six places a column has to be declared, so the app
self-heals in dev and a fresh deploy silently lacks the column. These tests
pin all six, plus the read/write round-trip through the Lakebase store.
"""

from __future__ import annotations

from pathlib import Path

from back.objects.registry.store.base import DomainSummary
from back.objects.registry.store.lakebase.store import LakebaseRegistryStore

ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = ROOT / "src/back/objects/registry/store/lakebase/schema.sql"
STORE_PY = ROOT / "src/back/objects/registry/store/lakebase/store.py"
MIGRATION = ROOT / "scripts/migrations/upgrade_0.7_to_0.8.sql"
BOOTSTRAP = ROOT / "scripts/bootstrap/lakebase-perms.sh"
DEPLOY_PREFLIGHT = ROOT / "scripts/_internal/_deploy-preflight.sh"


def test_canonical_schema_declares_the_column() -> None:
    body = SCHEMA_SQL.read_text()
    assert "mcp_policy      jsonb NOT NULL DEFAULT '{}'::jsonb" in body


def test_store_self_heals_the_column() -> None:
    """A workspace upgraded without running the migration must recover."""
    assert hasattr(LakebaseRegistryStore, "_ensure_domains_mcp_policy_column")
    body = STORE_PY.read_text()
    assert "ADD COLUMN IF NOT EXISTS mcp_policy jsonb" in body
    # Memoised, like every other lazy column guard.
    assert "self._mcp_policy_column_ready = False" in body


def test_self_heal_runs_at_every_call_site_that_reads_the_column() -> None:
    """initialize + the three runtime paths that touch domains.mcp_policy.

    Namely list_domains_with_metadata, read_version and write_version.
    """
    body = STORE_PY.read_text()
    assert body.count("self._ensure_domains_mcp_policy_column()") == 4


def test_migration_script_is_idempotent_and_checked() -> None:
    body = MIGRATION.read_text()
    assert "ADD COLUMN IF NOT EXISTS mcp_policy jsonb" in body
    assert "ON_ERROR_STOP on" in body
    assert "('domains', 'mcp_policy')" in body
    assert "RAISE EXCEPTION" in body


def test_bootstrap_provisions_the_column_as_schema_owner() -> None:
    body = BOOTSTRAP.read_text()
    assert "ADD COLUMN IF NOT EXISTS mcp_policy jsonb" in body
    assert "domains.mcp_policy" in body


def test_deploy_preflight_requires_the_migration_file() -> None:
    body = DEPLOY_PREFLIGHT.read_text()
    assert "scripts/migrations/upgrade_0.7_to_0.8.sql" in body


def test_domain_summary_carries_the_policy() -> None:
    assert "mcp_policy" in DomainSummary.__annotations__


def test_write_path_persists_and_read_path_restores_the_policy() -> None:
    """The column is written in the domains UPSERT and re-injected on read."""
    body = STORE_PY.read_text()
    write = body[body.index("def write_version") : body.index("def delete_version")]
    assert "review_quorum, mcp_policy)" in write
    assert "json.dumps(mcp_policy)" in write
    assert "mcp_policy    = EXCLUDED.mcp_policy" in write

    read = body[body.index("def read_version") : body.index("def write_version")]
    assert "d.mcp_policy" in read
    assert 'info["mcp_policy"] = coerce_mcp_policy(row.get("mcp_policy"))' in read
