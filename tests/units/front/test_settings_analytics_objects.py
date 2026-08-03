"""Contract tests for analytics tables in the Settings → Objects tabs."""

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SETTINGS_JS = REPO_ROOT / "src/front/static/config/js/settings.js"


def _js() -> str:
    return SETTINGS_JS.read_text(encoding="utf-8")


def test_lakebase_cards_mount_an_analytics_slot():
    js = _js()
    assert 'class="lk-analytics-slot" data-lk-base="' in js
    assert "loadLakebaseAnalyticsObjects();" in js


def test_lakehouse_cards_nest_analytics_under_the_domain():
    js = _js()
    assert "analyticsBlock(analyticsItems, false)" in js
    assert "analyticsByKey[grp.key]" in js


def test_domain_delete_includes_the_nested_analytics_tables():
    js = _js()
    # Lakehouse: appended to the triple-store drop list.
    assert "(entry.sortedItems || []).concat(entry.analyticsItems || [])" in js
    # Lakebase: appended to the UC drop pass, which runs after the Postgres one.
    assert "_lkAnalyticsRegistry[domainKey] || []" in js


def test_orphan_groups_get_their_own_deletable_card():
    js = _js()
    assert "dt-drop-orphan-btn" in js
    assert "lk-drop-orphan-btn" in js
    assert "dropDeltaOrphanObjects" in js
    assert "dropLakebaseOrphanObjects" in js


def test_lakebase_match_key_mirrors_the_server_rule():
    """<Domain>_V<n> → <domain>_<n>, the slug the analytics tables carry."""
    js = _js()
    assert "function lkDomainMatchKey(base)" in js
    assert "/^(.+)_V([^_]+)$/i" in js
