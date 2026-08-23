"""Deploy-time sanity checks for the graph-analytics bundle resource and scripts.

These guard three failures that were only discoverable by attempting a real
deploy, which is not something the suite does:

1. ``resources/graph_analytics.job.yml`` had mis-indented list items. Invalid
   YAML in any file under ``resources/`` breaks *every* ``databricks`` command
   for the bundle — including ``auth describe`` — so the blast radius is much
   wider than the job itself.
2. ``scripts/_internal/_render-app-yaml.py`` computed ``REPO_ROOT`` two levels
   up after the file moved into ``_internal/``, so it looked for the templates
   inside ``scripts/``.
3. ``scripts/_internal/_deploy-preflight.sh`` sourced a sibling via
   ``${SCRIPT_DIR:-<own dir>}``, but ``deploy.sh`` sources it with SCRIPT_DIR
   already set to ``scripts/``, so the default never applied.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
JOB_YML = REPO_ROOT / "resources/graph_analytics.job.yml"


class TestAllBundleResourcesParse:
    def test_every_resource_file_is_valid_yaml(self):
        files = sorted((REPO_ROOT / "resources").glob("*.yml"))
        assert files, "expected at least one bundle resource file"
        for path in files:
            try:
                yaml.safe_load(path.read_text())
            except yaml.YAMLError as exc:  # pragma: no cover - failure path
                pytest.fail(f"{path.relative_to(REPO_ROOT)} is not valid YAML: {exc}")

    def test_databricks_yml_is_valid_yaml(self):
        yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())


@pytest.fixture(scope="module")
def job() -> dict:
    return yaml.safe_load(JOB_YML.read_text())["resources"]["jobs"]["graph_analytics_job"]


@pytest.fixture(scope="module")
def task_parameters(job: dict) -> list:
    return job["tasks"][0]["spark_python_task"]["parameters"]


class TestJobParameterWiring:
    def test_every_reference_is_declared(self, job, task_parameters):
        referenced = {
            p.removeprefix("{{job.parameters.").removesuffix("}}")
            for p in task_parameters
            if p.startswith("{{job.parameters.")
        }
        declared = {p["name"] for p in job["parameters"]}
        assert referenced - declared == set(), "task references undeclared job parameters"

    def test_no_declared_parameter_is_unused(self, job, task_parameters):
        referenced = {
            p.removeprefix("{{job.parameters.").removesuffix("}}")
            for p in task_parameters
            if p.startswith("{{job.parameters.")
        }
        declared = {p["name"] for p in job["parameters"]}
        assert declared - referenced == set(), "job declares parameters the task never passes"

    def test_flags_and_values_alternate(self, task_parameters):
        """A mis-indent can silently drop a value, shifting every later pair."""
        assert len(task_parameters) % 2 == 0
        for flag, value in zip(task_parameters[::2], task_parameters[1::2]):
            assert flag.startswith("--"), f"expected a flag, got {flag!r}"
            assert not value.startswith("--"), f"expected a value, got {value!r}"

    def test_serverless_environment_key_is_declared(self, job):
        # A serverless spark_python_task is rejected without a matching entry.
        key = job["tasks"][0]["environment_key"]
        assert key in {e["environment_key"] for e in job["environments"]}

    def test_python_file_exists_in_the_repo(self, job):
        ref = job["tasks"][0]["spark_python_task"]["python_file"]
        rel = ref.split("${workspace.file_path}/", 1)[1]
        assert (REPO_ROOT / rel).is_file(), f"{rel} is referenced but missing"

    def test_job_source_is_not_excluded_from_bundle_sync(self, job):
        """The script is synced as a workspace file; an exclude would strip it."""
        ref = job["tasks"][0]["spark_python_task"]["python_file"]
        rel = ref.split("${workspace.file_path}/", 1)[1]
        excludes = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())["sync"]["exclude"]
        for pattern in excludes:
            assert not rel.startswith(pattern.rstrip("*/")), (
                f"sync.exclude pattern {pattern!r} would drop {rel}"
            )


class TestFlagsMatchTheJobCli:
    """A flag the script does not accept fails the run at argparse, not deploy."""

    def test_argparse_accepts_exactly_the_bundle_flags(self, job, task_parameters):
        spec = importlib.util.spec_from_file_location(
            "_gaj_under_test", REPO_ROOT / "src/jobs/graph_analytics_job.py"
        )
        module = importlib.util.module_from_spec(spec)
        # Registered before exec: the module defines dataclasses, which resolve
        # their annotations through sys.modules at class-creation time.
        sys.modules["_gaj_under_test"] = module
        try:
            spec.loader.exec_module(module)
            argv = [
                p if p.startswith("--") else (p.replace("{{job.parameters.", "").replace("}}", ""))
                for p in task_parameters
            ]
            # Substitute plausible values for the two numeric flags.
            defaults = {p["name"]: p.get("default", "") for p in job["parameters"]}
            argv = [p if p.startswith("--") else defaults.get(p, "x") or "x" for p in argv]
            ns = module.parse_args(argv)
            assert ns.source_table and ns.output_table
        finally:
            sys.modules.pop("_gaj_under_test", None)


class TestDeployScriptPathResolution:
    def test_render_script_repo_root_finds_its_templates(self):
        script = REPO_ROOT / "scripts/_internal/_render-app-yaml.py"
        text = script.read_text()
        assert "parents[2]" in text, "REPO_ROOT must climb out of scripts/_internal/"
        # Verify behaviourally, not just textually.
        root = script.resolve().parents[2]
        assert (root / "app.yaml.template").is_file()
        assert (root / "src/mcp-server/app.yaml.template").is_file()

    def test_preflight_sources_sibling_independently_of_the_caller(self):
        text = (REPO_ROOT / "scripts/_internal/_deploy-preflight.sh").read_text()
        assert "_PREFLIGHT_DIR=" in text
        assert 'source "${_PREFLIGHT_DIR}/_lakebase-diag.sh"' in text
        # The old form silently inherited deploy.sh's SCRIPT_DIR.
        assert 'source "${SCRIPT_DIR}/_lakebase-diag.sh"' not in text

    def test_sourced_sibling_actually_exists(self):
        assert (REPO_ROOT / "scripts/_internal/_lakebase-diag.sh").is_file()


class TestBootstrapScriptsRepoRoot:
    """scripts/bootstrap/* must cd to the repo root, not scripts/ (#133).

    After the scripts/ reorg these lived one level deeper; ``cd "$SCRIPT_DIR/.."``
    lands in ``scripts/``, so relative paths like
    ``scripts/_internal/check-deploy-prerequisites.sh`` miss.
    """

    _BOOTSTRAP = (
        "scripts/bootstrap/setup-lakebase.sh",
        "scripts/bootstrap/app-permissions.sh",
        "scripts/bootstrap/lakebase-perms.sh",
    )

    @pytest.mark.parametrize("rel", _BOOTSTRAP)
    def test_climbs_out_of_bootstrap_to_repo_root(self, rel: str):
        script = REPO_ROOT / rel
        text = script.read_text()
        # Must climb two levels (bootstrap → scripts → repo), not one.
        assert 'cd "$SCRIPT_DIR/../.."' in text or 'REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"' in text
        assert 'cd "$SCRIPT_DIR/.."\n' not in text
        root = script.resolve().parents[2]
        assert root == REPO_ROOT
        assert (root / "scripts/_internal/check-deploy-prerequisites.sh").is_file()
        assert (root / "scripts/deploy.config.sh").is_file()

    def test_setup_lakebase_does_not_document_removed_default_segment(self):
        text = (REPO_ROOT / "scripts/bootstrap/setup-lakebase.sh").read_text()
        assert "DEFAULT_LAKEBASE_DATABASE_RESOURCE_SEGMENT" not in text
        assert "DEFAULT_LAKEBASE_DATABASE" in text
        assert "LAKEBASE_DATABASE_RESOURCE_SEGMENT" in text


class TestAppPermissionsInBundle:
    """App ACL (CAN_USE) must nest under resources.apps.*, not top-level (#134).

    Top-level ``permissions:`` only accepts CAN_MANAGE / CAN_VIEW / CAN_RUN —
    putting CAN_USE there fails ``databricks bundle validate``.
    """

    def test_no_top_level_permissions_block(self):
        data = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())
        assert "permissions" not in data, (
            "top-level permissions: rejects CAN_USE; nest under resources.apps.*"
        )

    def test_each_app_declares_can_use_for_users(self):
        data = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())
        apps = data["resources"]["apps"]
        for key in ("ontobricks_dev_app", "mcp_ontobricks_app"):
            perms = apps[key].get("permissions") or []
            levels = {p.get("level") for p in perms}
            assert "CAN_MANAGE" in levels, f"{key} missing CAN_MANAGE"
            assert "CAN_USE" in levels, f"{key} missing CAN_USE"
            assert any(p.get("group_name") == "users" for p in perms)


class TestAnalyticsJobPermissionBootstrap:
    """The app SP must get CAN_MANAGE_RUN on the analytics job at deploy time.

    ``jobs.list()`` is ACL-filtered, so without this grant LakeflowRunner
    reports the job as missing even when the bundle deployed it correctly.
    DAB cannot declare the grant (the app SP only exists after the app is
    created), so ``make deploy`` relies on ``app-permissions.sh`` for it.
    """

    @pytest.fixture(scope="class")
    def bootstrap_script(self) -> str:
        return (REPO_ROOT / "scripts/bootstrap/app-permissions.sh").read_text()

    def test_bootstrap_grants_can_manage_run_on_the_analytics_job(self, bootstrap_script):
        assert "CAN_MANAGE_RUN" in bootstrap_script
        assert "permissions update jobs" in bootstrap_script
        assert "graph-analytics" in bootstrap_script

    def test_bootstrap_resolves_dev_mode_prefixed_job_names(self, bootstrap_script):
        # DAB development mode prefixes the name with '[dev <user>] '.
        assert "] " in bootstrap_script
        assert "endswith" in bootstrap_script

    def test_bootstrap_uses_the_bootstrapped_app_not_the_env_default(self, bootstrap_script):
        # Positional ``./app-permissions.sh ontobricks-07x …`` must grant on
        # that app's job, not the hard-coded ``ontobricks-030`` default.
        assert "FIRST_APP" in bootstrap_script
        assert "_MAIN_APP=" in bootstrap_script

    def test_deploy_invokes_app_permissions_bootstrap(self):
        deploy = (REPO_ROOT / "scripts/deploy.sh").read_text()
        assert "scripts/bootstrap/app-permissions.sh" in deploy
        assert 'app-permissions.sh "$APP_NAME" "$MCP_APP_NAME"' in deploy
