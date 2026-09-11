"""INSTANCE_ID drives app name + a per-instance DAB target (separate tfstate)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
CONFIG = ROOT / "scripts" / "deploy.config.sh"
ENSURE = ROOT / "scripts" / "_internal" / "_ensure-instance-target.sh"
GENERATED = ROOT / "resources" / "_generated_instance_target.yml"


def _source_config(env: dict | None = None) -> dict[str, str]:
    """Source deploy.config.sh and print the derived identity exports."""
    script = (
        f"source '{CONFIG}'\n"
        "printf 'INSTANCE_ID=%s\\n' \"$INSTANCE_ID\"\n"
        "printf 'APP_NAME=%s\\n' \"$APP_NAME\"\n"
        "printf 'MCP_APP_NAME=%s\\n' \"$MCP_APP_NAME\"\n"
        "printf 'DAB_TARGET=%s\\n' \"$DAB_TARGET\"\n"
        "printf 'APP_RESOURCE_KEY=%s\\n' \"$APP_RESOURCE_KEY\"\n"
        "printf 'MCP_APP_RESOURCE_KEY=%s\\n' \"$MCP_APP_RESOURCE_KEY\"\n"
    )
    # Drop stale identity exports so the file defaults win unless env sets them.
    clean = {
        k: v
        for k, v in os.environ.items()
        if k
        not in {
            "INSTANCE_ID",
            "APP_NAME",
            "MCP_APP_NAME",
            "DAB_TARGET",
            "DAB_BACKEND",
            "DEFAULT_INSTANCE_ID",
            "DEFAULT_DAB_TARGET",
            "DEFAULT_DAB_BACKEND",
            "DEFAULT_APP_NAME",
        }
    }
    if env:
        clean.update(env)
    result = subprocess.run(
        ["bash", "-c", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=clean,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    out: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            out[k] = v
    return out


class TestInstanceIdDerivation:
    def test_default_id_yields_suffixed_lakebase_target(self):
        got = _source_config()
        assert got["INSTANCE_ID"] == "08x"
        assert got["APP_NAME"] == "ontobricks-08x"
        assert got["MCP_APP_NAME"] == "mcp-ontobricks-08x"
        assert got["DAB_TARGET"] == "dev-lakebase-08x"
        # Resource keys stay static — isolation is the target, not the key.
        assert got["APP_RESOURCE_KEY"] == "ontobricks_dev_app"
        assert got["MCP_APP_RESOURCE_KEY"] == "mcp_ontobricks_app"

    def test_changing_id_changes_name_and_target_together(self):
        got = _source_config(env={"DEFAULT_INSTANCE_ID": "080"})
        assert got["APP_NAME"] == "ontobricks-080"
        assert got["DAB_TARGET"] == "dev-lakebase-080"

    def test_legacy_target_override_keeps_unsuffixed_name(self):
        got = _source_config(env={"DEFAULT_DAB_TARGET": "dev-lakebase"})
        assert got["DAB_TARGET"] == "dev-lakebase"
        assert got["APP_NAME"] == "ontobricks-08x"

    def test_dab_backend_knob_is_gone(self):
        assert "DEFAULT_DAB_BACKEND" not in CONFIG.read_text()
        assert "DAB_BACKEND" not in CONFIG.read_text()


class TestEnsureInstanceTarget:
    def setup_method(self):
        if GENERATED.exists():
            GENERATED.unlink()

    def teardown_method(self):
        if GENERATED.exists():
            GENERATED.unlink()

    def _ensure(self, target: str) -> subprocess.CompletedProcess[str]:
        script = (
            f"source '{ENSURE}'\n"
            f"ensure_instance_target '{target}'\n"
        )
        return subprocess.run(
            ["bash", "-c", script],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )

    def test_legacy_lakebase_removes_generated_file(self):
        GENERATED.write_text("stale: true\n")
        assert self._ensure("dev-lakebase").returncode == 0
        assert not GENERATED.exists()

    def test_lakebase_instance_writes_postgres_overlay(self):
        assert self._ensure("dev-lakebase-080").returncode == 0
        data = yaml.safe_load(GENERATED.read_text())
        assert "dev-lakebase-080" in data["targets"]
        apps = data["targets"]["dev-lakebase-080"]["resources"]["apps"]
        assert "postgres" in {r["name"] for r in apps["ontobricks_dev_app"]["resources"]}
        assert "postgres" in {r["name"] for r in apps["mcp_ontobricks_app"]["resources"]}

    def test_volume_instance_target_is_rejected(self):
        result = self._ensure("dev-080")
        assert result.returncode != 0

    def test_unknown_target_fails(self):
        result = self._ensure("prod")
        assert result.returncode != 0


class TestDeployWiresInstanceTarget:
    def test_deploy_sources_and_calls_ensure(self):
        body = (ROOT / "scripts" / "deploy.sh").read_text()
        assert "_ensure-instance-target.sh" in body
        # Real call (not a comment) — must run before the validate invocation.
        call_at = body.index("\nensure_instance_target ")
        validate_at = body.index("\ndatabricks bundle validate ")
        assert call_at < validate_at
