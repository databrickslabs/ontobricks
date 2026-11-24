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
#: ``valueFrom`` in app.yaml. Empty since 0.10.0: the manifest can't declare a
#: Lakebase ``postgres`` resource (no such spec in the SDK's AppManifest model),
#: so Lakebase is bound after install instead (PLAN.md Q1).
AUTO_INJECTED_RESOURCES: set[str] = set()

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


def test_user_authorization_matches_template():
    """The ``sql`` user scope (admin group checks, MCP OBO) must ship too."""
    assert _load(MARKETPLACE / "app.yaml").get("authorization") == _load(TEMPLATE).get("authorization")


def test_manifest_user_scopes_match_template():
    """Marketplace installs take user scopes from the manifest, not app.yaml."""
    template_scopes = _load(TEMPLATE)["authorization"]["user"]["scopes"]
    assert _load(MARKETPLACE / "manifest.yaml").get("user_api_scopes") == template_scopes


def test_manifest_has_no_resource_type_the_sdk_cannot_express():
    """Only resource specs the SDK's AppManifest model knows about."""
    import dataclasses

    from databricks.sdk.service import apps

    allowed = {f.name for f in dataclasses.fields(apps.AppManifestAppResourceSpec)} - {"name", "description"}
    for spec in _load(MARKETPLACE / "manifest.yaml")["resource_specs"]:
        kinds = set(spec) - {"name", "description"}
        assert kinds <= allowed, f"{spec['name']}: {kinds - allowed} not in AppManifest"
