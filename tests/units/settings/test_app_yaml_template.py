"""Contract tests for ``app.yaml.template``.

The template duplicates the analytics defaults from
:mod:`shared.config.settings` so an admin can retune a deployed app without a
code change. Duplication rots silently: a default changed in one place and not
the other means a deployed app behaves differently from a local one, with
nothing to signal it. These tests make that drift a test failure.
"""

from __future__ import annotations

import re
import string
from pathlib import Path

import pytest
import yaml

from shared.config.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = REPO_ROOT / "app.yaml.template"

#: Template env var → the ``Settings`` field it must agree with.
ANALYTICS_VARS = {
    "ONTOBRICKS_ANALYTICS_MAX_TRIPLES": "analytics_max_triples",
    "ONTOBRICKS_ANALYTICS_PUSHDOWN_ENABLED": "analytics_pushdown_enabled",
    "ONTOBRICKS_ANALYTICS_TOP_N": "analytics_top_n",
    "ONTOBRICKS_ANALYTICS_JOB_ENABLED": "analytics_job_enabled",
    "ONTOBRICKS_ANALYTICS_JOB_NAME": "analytics_job_name",
    "ONTOBRICKS_ANALYTICS_JOB_PAGERANK_ITERATIONS": "analytics_job_pagerank_iterations",
    "ONTOBRICKS_ANALYTICS_JOB_PIVOTS": "analytics_job_pivots",
}


def _render() -> dict:
    """Render the template with dummy substitutions and parse it.

    The deploy script uses ``Template.substitute`` rather than
    ``safe_substitute``, so every ``${VAR}`` must be supplied. Feeding it
    placeholders here reproduces that strictness: a ``$`` accidentally left in
    a literal value would fail this call, which is exactly when we want to
    hear about it rather than at deploy time.
    """
    raw = TEMPLATE.read_text()
    names = set(re.findall(r"\$\{(\w+)\}", raw))
    return yaml.safe_load(string.Template(raw).substitute({n: "x" for n in names}))


@pytest.fixture(scope="module")
def env_map() -> dict:
    doc = _render()
    return {e["name"]: e.get("value") for e in doc.get("env", [])}


class TestTemplateRenders:
    def test_template_substitutes_and_parses_as_yaml(self):
        assert isinstance(_render(), dict)

    def test_no_duplicate_env_names(self):
        doc = _render()
        names = [e["name"] for e in doc.get("env", [])]
        assert len(names) == len(set(names)), "a duplicated env name silently wins"


class TestAnalyticsDefaultsMatchSettings:
    @pytest.mark.parametrize("var,field", sorted(ANALYTICS_VARS.items()))
    def test_var_is_declared(self, env_map, var, field):
        assert var in env_map, f"{var} missing — {field} is not tunable on a deploy"

    @pytest.mark.parametrize("var,field", sorted(ANALYTICS_VARS.items()))
    def test_value_round_trips_to_the_settings_default(self, env_map, var, field, monkeypatch):
        """The template value must parse to the same thing the code defaults to.

        Comparing the *parsed* value rather than the raw string is deliberate:
        it catches `"True"` vs `"true"` and `"1e5"` vs `500000` without pinning
        the template to one spelling.
        """
        monkeypatch.setenv(var, str(env_map[var]))
        assert getattr(Settings(), field) == getattr(Settings.model_fields[field], "default")


class TestJobToggleIsOptIn:
    def test_job_mode_ships_disabled(self, env_map):
        # Job mode needs a deployed bundle and Spark-readable data, so a fresh
        # deploy must not turn it on for the user.
        assert env_map["ONTOBRICKS_ANALYTICS_JOB_ENABLED"] == "false"

    def test_job_name_is_empty_so_it_is_derived(self, env_map):
        # Empty means "derive <app>-graph-analytics", which is what matches the
        # bundle. A hard-coded name here would break every renamed deploy.
        assert env_map["ONTOBRICKS_ANALYTICS_JOB_NAME"] == ""
