"""Contracts so the live scenario campaign actually drives 0.9 Generate.

Two failure modes showed up in the 0.9 campaign:

1. ``make scenario-campaign`` sourced ``.venv/bin/activate`` while
   ``VIRTUAL_ENV`` still pointed at another worktree, so pytest ran that
   worktree's interpreter (and its scenario files) against this app.
2. Scenario 1 still treated Generate as a one-shot OWL apply. The top
   button now starts Stage 1 detection; classes only land after Review →
   Complete. Polling ``/ontology/load`` right after the click times out
   with an empty class list even when detection succeeded.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
MAKEFILE = REPO_ROOT / "Makefile"
SCENARIO_01 = (
    REPO_ROOT / "tests/e2e/scenarios/test_scenario_01_generate_live.py"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _campaign_recipe() -> str:
    text = _read(MAKEFILE)
    start = text.index("scenario-campaign:")
    rest = text[start:]
    nxt = rest.find("\nformat:")
    return rest if nxt < 0 else rest[:nxt]


class TestCampaignUsesThisWorktreeInterpreter:
    def test_the_recipe_runs_pytest_through_uv_frozen(self):
        recipe = _campaign_recipe()
        assert "uv run --frozen pytest" in recipe

    def test_the_recipe_does_not_source_activate(self):
        """``source .venv/bin/activate`` keeps a foreign VIRTUAL_ENV python."""
        recipe = _campaign_recipe()
        assert "activate" not in recipe

    def test_the_recipe_clears_a_foreign_virtual_env(self):
        recipe = _campaign_recipe()
        assert "env -u VIRTUAL_ENV" in recipe

    def test_the_recipe_sources_dotenv_so_warehouse_creds_reach_pytest(self):
        """Scenario 4 creates scratch UC tables in the pytest process."""
        recipe = _campaign_recipe()
        assert ".env" in recipe
        assert "DATABRICKS_SQL_WAREHOUSE_ID" in recipe or "set -a" in recipe

    def test_the_recipe_collects_only_the_live_scenario_files(self):
        """``test_full_lifecycle`` is the isolated Playwright server journey."""
        recipe = _campaign_recipe()
        assert "tests/e2e/scenarios/test_scenario_" in recipe
        assert "pytest tests/e2e/scenarios -m scenario" not in recipe


class TestCampaignDomainsArePrefixedZz:
    def test_names_are_valid_camelcase_and_start_with_zz(self):
        from back.objects.session.DomainSession import is_valid_domain_name
        from tests.e2e.scenarios._harness import (
            CAMPAIGN_DOMAIN_NAME,
            LIFECYCLE_DOMAIN_NAME,
            METADATA_DOMAIN_NAME,
            ZERO_COPY_DOMAIN_NAME,
        )

        for name in (
            CAMPAIGN_DOMAIN_NAME,
            METADATA_DOMAIN_NAME,
            ZERO_COPY_DOMAIN_NAME,
            LIFECYCLE_DOMAIN_NAME,
        ):
            assert name.startswith("Zz"), name
            assert is_valid_domain_name(name), name

    def test_leftover_matcher_covers_zz_underscore_and_legacy_folders(self):
        from tests.e2e.scenarios._harness import is_campaign_leftover

        assert is_campaign_leftover("ZzTestScenario1") is True
        assert is_campaign_leftover("ZZ_TestScenario1") is True
        assert is_campaign_leftover("zz_testzerocopy") is True
        assert is_campaign_leftover("testscenario1") is True
        assert is_campaign_leftover("Customer360") is False

    def test_campaign_starts_with_a_purge_step(self):
        purge = REPO_ROOT / "tests/e2e/scenarios/test_scenario_00_purge.py"
        assert purge.is_file()
        src = _read(purge)
        assert "purge_campaign_domains" in src
        assert 'chain_marker("scenario_0")' in src
        one = _read(SCENARIO_01)
        assert 'depends=("scenario_0",)' in one


class TestScenario1DrivesTheStagedWizard:
    def test_it_detects_then_posts_complete(self):
        src = _read(SCENARIO_01)
        assert "wizardTopGenerateBtn" in src
        assert "/ontology/wizard/generate/draft" in src
        assert "/ontology/wizard/generate/complete" in src
        assert "gen_btn.click()" in src
        detect = src.index("gen_btn.click()")
        complete = src.index("/ontology/wizard/generate/complete", detect)
        load = src.index("/ontology/load", complete)
        assert detect < complete < load

    def test_it_polls_the_completion_task_not_the_review_button(self):
        src = _read(SCENARIO_01)
        assert "_poll_task(" in src
        assert "wizardReviewContinueBtn" not in src


class TestCampaignSurvivesASlowHealthEndpoint:
    def test_probe_timeout_is_at_least_a_minute(self):
        from tests.e2e.scenarios._harness import PROBE_TIMEOUT_S

        assert PROBE_TIMEOUT_S >= 60


class TestLoopbackLiveBaseDoesNotNeedAppsToken:
    def test_localhost_does_not_require_a_gateway_bearer(self):
        from tests.e2e.scenarios._harness import needs_apps_gateway_token

        assert needs_apps_gateway_token(None) is False
        assert needs_apps_gateway_token("http://localhost:8000") is False
        assert needs_apps_gateway_token("http://127.0.0.1:8000") is False

    def test_a_deployed_app_url_still_requires_a_gateway_bearer(self):
        from tests.e2e.scenarios._harness import needs_apps_gateway_token

        assert needs_apps_gateway_token(
            "https://ontobricks-xxxx.aws.databricksapps.com"
        ) is True

    def test_the_e2e_bearer_fixture_uses_the_loopback_gate(self):
        src = _read(REPO_ROOT / "tests/e2e/conftest.py")
        assert "needs_apps_gateway_token" in src


class TestLivePageSeedsAWorkspace:
    def test_page_fixture_seeds_a_named_domain_in_live_mode(self):
        src = _read(REPO_ROOT / "tests/e2e/conftest.py")
        assert "_seed_loaded_workspace" in src
        assert "ZzE2eLive" in src
        assert "home_redirect_if_unloaded" in src
        assert "obSubnav" in src


class TestLiveRouteReappliesBearer:
    """Playwright ``route.continue_()`` drops context extra_http_headers."""

    def test_bearer_is_copied_onto_continue_headers(self):
        from tests.e2e.conftest import _bearer_continue_headers

        headers = _bearer_continue_headers(
            {"accept": "text/html", "host": "ontobricks.example"},
            "tok-abc",
        )
        assert headers["authorization"] == "Bearer tok-abc"
        assert headers["accept"] == "text/html"

    def test_install_redirect_fix_passes_headers_on_document_continue(self):
        src = _read(REPO_ROOT / "tests/e2e/conftest.py")
        assert "route.continue_(headers=" in src
        assert "_bearer_continue_headers" in src

    def test_install_redirect_fix_falls_back_for_xhr(self):
        """XHR must not use continue_(headers=): Playwright then fails fetch."""
        src = _read(REPO_ROOT / "tests/e2e/conftest.py")
        assert "route.fallback()" in src
        assert 'resource_type != "document"' in src
        assert 'ctx.route("**/*"' not in src

    def test_live_bearer_is_reminted_before_the_apps_token_dies(self):
        src = _read(REPO_ROOT / "tests/e2e/conftest.py")
        assert "_LIVE_BEARER_TTL_S" in src
        assert "_resolve_live_bearer" in src
        assert "force_refresh=" in src
