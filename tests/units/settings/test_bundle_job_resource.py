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
