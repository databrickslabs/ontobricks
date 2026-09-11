"""An app rename is a delete, so the deploy has to say so before it happens.

A Databricks app name is immutable. The DAB resource key is static while the
name is derived from ``DEFAULT_INSTANCE_ID``, so editing the id *on the same
DAB target* makes Terraform destroy the running app and create a new one.
(Changing INSTANCE_ID under the default config also switches target, which
keeps the old app — that is the safe path.) On 2026-07-30 a same-target
rename turned a routine ``make deploy`` into the deletion of ``ontobricks-060``:
the destroy succeeded and the create failed on a secret scope that did not
exist, leaving no app at all.

These tests drive the real bash functions, because the value of a guard is
entirely in whether it exits non-zero at the right moment.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
PREFLIGHT = ROOT / "scripts/_internal/_deploy-preflight.sh"
DEPLOY = ROOT / "scripts/deploy.sh"
CONFIG = ROOT / "scripts/deploy.config.sh"
BUNDLE = ROOT / "databricks.yml"
APP_TEMPLATE = ROOT / "app.yaml.template"
MAKEFILE = ROOT / "Makefile"

TARGET = "dev-lakebase"
KEY = "ontobricks_dev_app"


def _write_state(tmp_path: Path, app_name: str | None, *, resource_key: str = KEY):
    """Fabricate a bundle tfstate, optionally with an app resource in it."""
    state_dir = tmp_path / ".databricks" / "bundle" / TARGET / "terraform"
    state_dir.mkdir(parents=True, exist_ok=True)
    resources = []
    if app_name is not None:
        resources.append(
            {
                "type": "databricks_app",
                "name": resource_key,
                "instances": [{"attributes": {"name": app_name}}],
            }
        )
    resources.append({"type": "databricks_job", "name": "graph_analytics_job"})
    (state_dir / "terraform.tfstate").write_text(
        json.dumps({"serial": 1, "resources": resources})
    )


def _run_guard(cwd: Path, desired: str, env: dict | None = None):
    script = (
        f"source '{PREFLIGHT}'\n"
        f"_preflight_check_app_rename '{TARGET}' '{KEY}' '{desired}'\n"
    )
    return subprocess.run(
        ["bash", "-c", script],
        cwd=cwd,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,  # never a TTY, so the guard cannot prompt
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", **(env or {})},
    )


class TestGuardAllowsSafeDeploys:
    def test_unchanged_name_passes(self, tmp_path):
        _write_state(tmp_path, "ontobricks-07x")
        assert _run_guard(tmp_path, "ontobricks-07x").returncode == 0

    def test_missing_state_passes_as_a_first_deploy(self, tmp_path):
        assert _run_guard(tmp_path, "ontobricks-07x").returncode == 0

    def test_state_without_the_app_resource_passes(self, tmp_path):
        """The situation right after the failed replacement — nothing to destroy."""
        _write_state(tmp_path, None)
        assert _run_guard(tmp_path, "ontobricks-07x").returncode == 0

    def test_a_different_resource_key_is_not_mistaken_for_ours(self, tmp_path):
        _write_state(tmp_path, "mcp-ontobricks-07x", resource_key="mcp_ontobricks_app")
        assert _run_guard(tmp_path, "ontobricks-07x").returncode == 0

    def test_unreadable_state_does_not_block_the_deploy(self, tmp_path):
        state_dir = tmp_path / ".databricks" / "bundle" / TARGET / "terraform"
        state_dir.mkdir(parents=True)
        (state_dir / "terraform.tfstate").write_text("{not json")
        assert _run_guard(tmp_path, "ontobricks-07x").returncode == 0


class TestGuardBlocksTheRename:
    def test_changed_name_aborts(self, tmp_path):
        """The exact 060 -> 07x edit that deleted the app."""
        _write_state(tmp_path, "ontobricks-060")
        result = _run_guard(tmp_path, "ontobricks-07x")
        assert result.returncode != 0

    def test_it_names_the_app_it_would_destroy(self, tmp_path):
        _write_state(tmp_path, "ontobricks-060")
        out = _run_guard(tmp_path, "ontobricks-07x")
        combined = out.stdout + out.stderr
        assert "ontobricks-060" in combined
        assert "DESTROY" in combined

    def test_it_says_how_to_keep_the_old_app(self, tmp_path):
        _write_state(tmp_path, "ontobricks-060")
        out = _run_guard(tmp_path, "ontobricks-07x")
        assert "DEFAULT_INSTANCE_ID" in out.stdout + out.stderr

    def test_it_reassures_about_the_data(self, tmp_path):
        """The registry Volume and Lakebase schema survive; say so."""
        _write_state(tmp_path, "ontobricks-060")
        combined = _run_guard(tmp_path, "ontobricks-07x")
        assert "Lakebase" in combined.stdout + combined.stderr

    def test_explicit_override_proceeds(self, tmp_path):
        _write_state(tmp_path, "ontobricks-060")
        result = _run_guard(tmp_path, "ontobricks-07x", env={"ALLOW_APP_RENAME": "1"})
        assert result.returncode == 0

    def test_a_wrong_override_value_does_not_count_as_consent(self, tmp_path):
        _write_state(tmp_path, "ontobricks-060")
        for value in ("0", "true", "yes", ""):
            result = _run_guard(
                tmp_path, "ontobricks-07x", env={"ALLOW_APP_RENAME": value}
            )
            assert result.returncode != 0, f"ALLOW_APP_RENAME={value!r} must not pass"


class TestDeployWiring:
    def test_the_guard_runs_before_the_bundle_is_deployed(self):
        body = DEPLOY.read_text()
        assert "_preflight_check_app_rename" in body
        guard_at = body.index("_preflight_check_app_rename")
        deploy_at = body.index("databricks bundle deploy")
        assert guard_at < deploy_at, "the guard must run before anything is applied"

    def test_the_guard_runs_before_rendering_app_yaml(self):
        body = DEPLOY.read_text()
        assert body.index("_preflight_check_app_rename") < body.index(
            "python3 scripts/_internal/_render-app-yaml.py"
        )

    def test_a_failed_guard_aborts_the_deploy(self):
        body = DEPLOY.read_text()
        after = body.split("_preflight_check_app_rename", 2)[2]
        assert "die" in after.split("\n\n")[0]


class TestDeployDoesNotBindNeo4jSecrets:
    """Neo4j is optional — DAB must not require a workspace secret (GH #136)."""

    def test_the_bundle_has_no_neo4j_secret_overlay(self):
        bundle = yaml.safe_load(BUNDLE.read_text())
        app = bundle["targets"][TARGET]["resources"]["apps"][KEY]
        secrets = [r for r in app.get("resources") or [] if "secret" in r]
        assert secrets == [], "DAB must not bind neo4j-password (optional engine)"

    def test_the_neo4j_secret_scope_variable_is_gone(self):
        bundle = yaml.safe_load(BUNDLE.read_text())
        assert "neo4j_secret_scope" not in bundle.get("variables", {})

    def test_deploy_does_not_pass_neo4j_secret_scope(self):
        assert "neo4j_secret_scope" not in DEPLOY.read_text()

    def test_app_manifest_does_not_reference_unbound_neo4j_secret(self):
        app = yaml.safe_load(APP_TEMPLATE.read_text())
        assert all(item.get("valueFrom") != "neo4j-password" for item in app["env"])
        assert all(item.get("name") != "neo4j-password" for item in app["resources"])

    def test_make_targets_do_not_pass_removed_neo4j_scope(self):
        assert "neo4j_secret_scope" not in MAKEFILE.read_text()

    def test_deploy_config_does_not_export_neo4j_secret_scope(self):
        body = CONFIG.read_text()
        assert "NEO4J_SECRET_SCOPE" not in body
        assert "DEFAULT_NEO4J_SECRET_SCOPE" not in body

    def test_instance_target_generator_does_not_bind_neo4j_secret(self):
        gen = (ROOT / "scripts/_internal/_ensure-instance-target.sh").read_text()
        assert "key: neo4j-password" not in gen
        assert "neo4j_secret_scope" not in gen
