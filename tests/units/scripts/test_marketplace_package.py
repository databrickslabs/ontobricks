"""Contract tests for the Marketplace app package (marketplace/*.yaml).

Marketplace runs ``app.yaml`` verbatim — no render step — so it must be
static, must agree with ``manifest.yaml`` on resource names, and must not
drift from ``app.yaml.template`` (the DAB deploy path).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MARKETPLACE = REPO_ROOT / "marketplace"
TEMPLATE = REPO_ROOT / "app.yaml.template"

#: Manifest resources whose env vars the Apps platform injects, so they have no
#: ``valueFrom`` in app.yaml: Lakebase injects PGHOST/PGPORT/PGDATABASE/PGUSER.
AUTO_INJECTED_RESOURCES = {"postgres"}

#: Template env vars intentionally NOT shipped to Marketplace installs.
#: Adding a var here is a deliberate decision — review it.
DEPLOY_ONLY_ENV = {
    "DATABRICKS_SQL_WAREHOUSE_ID_DEFAULT",  # MCP/session-less fallback
    "DATABRICKS_TRIPLESTORE_TABLE",  # session-less fallback
    # No install-time source exists for these (Apps can't bind a UC schema and
    # Settings has no input for them); see PLAN.md Q7.
    "REGISTRY_CATALOG",
    "REGISTRY_SCHEMA",
    "MCP_APP_NAME",  # MCP companion out of scope for v1 (Q5)
    "MCP_SP_CLIENT_ID",
    "LAKEBASE_PROJECT",  # only read when PGHOST is absent
    "LAKEBASE_DATABASE",
    "LAKEBASE_BRANCH",
    "ONTOBRICKS_SYNC_UC_CATALOG",  # optional, managed-synced only
}


#: User scopes only Marketplace installs need: they can't run the
#: app-permissions.sh grant, so the first-run setup does it as the admin (Q4).
MARKETPLACE_ONLY_SCOPES = {"apps"}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _env(doc: dict) -> dict[str, dict]:
    return {e["name"]: e for e in doc.get("env", [])}


def test_manifest_schema_version_is_1():
    assert _load(MARKETPLACE / "manifest.yaml")["version"] == 1


def test_app_yaml_has_no_template_placeholders():
    assert "${" not in (MARKETPLACE / "app.yaml").read_text()


def test_every_value_from_is_declared_in_manifest():
    app = _load(MARKETPLACE / "app.yaml")
    declared = {r["name"] for r in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]}
    used = {e["valueFrom"] for e in _env(app).values() if "valueFrom" in e}
    assert used <= declared, f"valueFrom without manifest resource: {used - declared}"


def test_every_manifest_resource_is_consumed():
    app = _load(MARKETPLACE / "app.yaml")
    declared = {r["name"] for r in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]}
    used = {e["valueFrom"] for e in _env(app).values() if "valueFrom" in e}
    assert declared - used <= AUTO_INJECTED_RESOURCES


def test_command_matches_deploy_template():
    assert _load(MARKETPLACE / "app.yaml")["command"] == _load(TEMPLATE)["command"]


def test_marketplace_env_names_exist_in_template():
    unknown = set(_env(_load(MARKETPLACE / "app.yaml"))) - set(_env(_load(TEMPLATE)))
    assert not unknown, f"typo or undocumented var: {unknown}"


def test_every_template_env_is_shipped_or_allowlisted():
    template_env = set(_env(_load(TEMPLATE)))
    shipped = set(_env(_load(MARKETPLACE / "app.yaml")))
    missing = template_env - shipped - DEPLOY_ONLY_ENV
    assert not missing, f"add to marketplace/app.yaml or DEPLOY_ONLY_ENV: {missing}"


def test_literal_values_agree_with_template():
    """Vars the template hardcodes (no ${...}) must ship the same value."""
    template_env = _env(_load(TEMPLATE))
    for name, entry in _env(_load(MARKETPLACE / "app.yaml")).items():
        tmpl = template_env[name]
        if "value" in tmpl and not re.search(r"\$\{", str(tmpl["value"])):
            assert entry.get("value") == tmpl["value"], name


def _scopes(app_yaml: dict) -> set:
    return set(app_yaml["authorization"]["user"]["scopes"])


def test_manifest_user_scopes_are_the_template_scopes_plus_marketplace_only():
    """Marketplace installs take user scopes from the manifest, not app.yaml."""
    manifest_scopes = set(_load(MARKETPLACE / "manifest.yaml")["user_api_scopes"])
    assert manifest_scopes == _scopes(_load(TEMPLATE)) | MARKETPLACE_ONLY_SCOPES


def test_app_yaml_scopes_mirror_the_manifest():
    manifest_scopes = set(_load(MARKETPLACE / "manifest.yaml")["user_api_scopes"])
    assert _scopes(_load(MARKETPLACE / "app.yaml")) == manifest_scopes


def test_manifest_binds_lakebase_at_install():
    """The registry is Lakebase-only in 0.10.x, so the install must bind it."""
    specs = {r["name"]: r for r in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]}
    assert specs["postgres"]["postgres_spec"] == {"permission": "CAN_CONNECT_AND_CREATE"}
