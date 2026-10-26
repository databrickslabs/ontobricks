"""
E2E (LIVE) — campaign preflight: drop leftover ``Zz*`` / ``ZZ_*`` domains.

Runs first (zero-padded ``00``) so every campaign starts from a clean
registry. Isolated re-runs of a later scenario do not collect this file, so
they keep the durable domain they need.

Gated behind ``ONTOBRICKS_SCENARIO_LIVE=1``. ``make scenario-campaign`` also
sets ``ONTOBRICKS_SCENARIO_CHAIN=1`` so a failed purge skips 1–5.
"""

from __future__ import annotations

import os

import pytest

from tests.e2e.scenarios._harness import (
    chain_marker,
    is_campaign_leftover,
    make_step,
    purge_campaign_domains,
    registry_domain_keys,
)

pytestmark = [
    pytest.mark.skipif(
        os.environ.get("ONTOBRICKS_SCENARIO_LIVE") != "1",
        reason="live scenario — set ONTOBRICKS_SCENARIO_LIVE=1 to run",
    ),
    *chain_marker("scenario_0"),
]

_step = make_step("scenario_0")


class TestScenario0PurgeCampaignDomains:
    def test_remove_existing_zz_domains(self, scenario_page, scenario_base):
        page = scenario_page
        base = scenario_base

        _step(f"priming session at {base}")
        page.goto(base)
        page.wait_for_load_state("domcontentloaded")

        removed = purge_campaign_domains(page, base, step=_step)
        leftover = [
            name
            for name in registry_domain_keys(page, base)
            if is_campaign_leftover(name)
        ]
        assert leftover == [], leftover
        _step(
            f"DONE — purged {len(removed)} leftover campaign domain(s); "
            "registry has no Zz*/ZZ_* folders"
        )
