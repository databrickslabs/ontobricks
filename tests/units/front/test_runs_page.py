"""Knowledge Graph → Management → Runs renders two independent tables.

Build runs and analytics runs share no columns, so they live in one tab each
rather than being merged into a single timeline. Each tab owns its loading /
empty / error elements: a failure fetching one must not blank the other.

Most assertions drive a ``TestClient`` and parse the RENDERED HTML (via the
``_html`` / ``_tags`` / ``_find`` helpers from ``test_ui_rendering.py``),
which proves the partial is actually included and rendered on ``/dtwin/``.
A few assertions fall back to reading a template file directly off disk —
only where the rendered page genuinely cannot express the fact: e.g. that
the analytics modal lives in ``dtwin.html`` (not merged into the partial's
output by inclusion) or that an id is absent from one specific source file.
"""

import re
from pathlib import Path

import pytest

from tests.units.api.test_ui_rendering import _find, _html, _tags

pytestmark = pytest.mark.unit

_PARTIAL = Path("src/front/templates/partials/domain/_domain_runs.html")
_DTWIN = Path("src/front/templates/dtwin.html")
_ANALYTICS = Path("src/front/templates/partials/dtwin/_query_analytics.html")


def _partial() -> str:
    return _PARTIAL.read_text(encoding="utf-8")


def _analytics_partial() -> str:
    return _ANALYTICS.read_text(encoding="utf-8")


class TestRunsTabs:
    """The two histories sit in a tab each. Both panes are rendered on page
    load — only their visibility differs — so the loaders can populate both
    without waiting for a tab to be opened."""

    def test_there_is_a_tab_for_each_history(self, client):
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        for button_id in ("rtab-btn-build", "rtab-btn-analytics"):
            assert _find(tags, id_=button_id) is not None
        for pane_id in ("rtab-build", "rtab-analytics"):
            assert _find(tags, id_=pane_id) is not None

    def test_each_tab_button_targets_its_own_pane(self, client):
        """A copy-paste slip here points both buttons at one pane, which
        looks like a tab that silently does nothing."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        for button_id, pane_id in (
            ("rtab-btn-build", "#rtab-build"),
            ("rtab-btn-analytics", "#rtab-analytics"),
        ):
            btn = _find(tags, id_=button_id)
            assert btn.get("data-bs-target") == pane_id
            assert btn.get("data-bs-toggle") == "tab"

    def test_build_runs_is_the_default_tab(self, client):
        """Exactly one tab may be active and exactly one pane may carry
        `show active`; two actives renders both panes stacked, none renders
        an empty section."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)

        active_buttons = [
            i for i in ("rtab-btn-build", "rtab-btn-analytics")
            if "active" in (_find(tags, id_=i).get("class") or "")
        ]
        active_panes = [
            i for i in ("rtab-build", "rtab-analytics")
            if "active" in (_find(tags, id_=i).get("class") or "")
        ]

        assert active_buttons == ["rtab-btn-build"]
        assert active_panes == ["rtab-build"]
        assert "show" in (_find(tags, id_="rtab-build").get("class") or "")

    def test_each_pane_is_labelled_by_its_tab(self, client):
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        for pane_id, button_id in (
            ("rtab-build", "rtab-btn-build"),
            ("rtab-analytics", "rtab-btn-analytics"),
        ):
            assert _find(tags, id_=pane_id).get("aria-labelledby") == button_id

    def test_each_table_lives_in_its_own_pane(self, client):
        """The whole point of the split: the analytics table must not be
        inside the build pane, or switching tabs would show both at once."""
        html = _html(client, "/dtwin/")
        build_pane = html.index('id="rtab-build"')
        analytics_pane = html.index('id="rtab-analytics"')

        assert build_pane < html.index('id="runsTableBody"') < analytics_pane
        assert analytics_pane < html.index('id="analyticsRunsTableBody"')


class TestRunsPartial:
    """Rendered-HTML assertions: prove both tabs' elements actually show up
    on the served /dtwin/ page, not merely that the strings exist on disk."""

    @pytest.mark.parametrize(
        "element_id",
        [
            "runsTableBody",
            "analyticsRunsTableBody",
            "analyticsRunsLoading",
            "analyticsRunsEmpty",
            "analyticsRunsError",
            "analyticsRunsErrorMessage",
            "analyticsRunsTableWrapper",
        ],
    )
    def test_both_tabs_have_their_own_elements(self, client, element_id):
        html = _html(client, "/dtwin/")
        assert _find(_tags(html), id_=element_id) is not None

    def test_the_version_filter_is_gone(self, client):
        """Both tables always show every version, so the dropdown that used
        to scope only the build table would now be a half-working control."""
        html = _html(client, "/dtwin/")
        assert _find(_tags(html), id_="runsVersionFilter") is None

    def test_the_analytics_table_names_its_version_column(self, client):
        """With no filter, rows from several versions interleave, so each
        row has to say which version it came from."""
        html = _html(client, "/dtwin/")
        analytics = html[html.index("analyticsRunsTableWrapper"):]
        for header in ("Scope", "Version", "Nodes", "Edges", "Components", "Density"):
            assert f">{header}<" in analytics

    def test_refresh_button_still_calls_load_domain_runs(self, client):
        """The one bit of JS wiring this task keeps: the shared Refresh
        button already calls the existing loadDomainRuns()."""
        html = _html(client, "/dtwin/")
        btn = _find(_tags(html), id_="btnReloadRuns")
        assert btn is not None
        assert btn.get("onclick") == "loadDomainRuns()"

    def test_the_version_filter_is_gone_from_the_partial_source(self):
        """File-read fallback: confirms the id is gone from this specific
        source file (not just absent from one rendered page that happens
        not to include it for other reasons)."""
        assert "runsVersionFilter" not in _partial()


class TestAnalyticsModal:
    def test_modal_renders_on_the_page(self, client):
        """Rendered-HTML: the modal actually shows up in the served page."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        assert _find(tags, id_="analyticsRunDetailsModal") is not None
        assert _find(tags, id_="analyticsRunDetailsBody") is not None

    def test_modal_is_page_level_not_inside_the_section(self):
        """Structural fact about which *file* contains the modal, which the
        merged rendered output cannot show (both dtwin.html's own markup and
        its included partial end up in the same response). File-read is the
        only way to prove the modal was added to dtwin.html itself and not
        into the partial that is also included by domain.html."""
        assert 'id="analyticsRunDetailsModal"' in _DTWIN.read_text(encoding="utf-8")
        assert "analyticsRunDetailsModal" not in _partial()

    def test_modal_has_a_body_for_the_script_to_fill(self):
        assert 'id="analyticsRunDetailsBody"' in _DTWIN.read_text(encoding="utf-8")

    def test_modal_aria_labelledby_references_existing_id(self, client):
        """aria-labelledby on analyticsRunDetailsModal must name an id that
        actually exists on the rendered page (WCAG accessible name linkage)."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        modal = _find(tags, id_="analyticsRunDetailsModal")
        assert modal is not None
        label_id = modal.get("aria-labelledby")
        assert label_id, "analyticsRunDetailsModal is missing aria-labelledby"
        assert _find(tags, id_=label_id) is not None, (
            f"aria-labelledby='{label_id}' points to an id that does not exist on the page"
        )


_PERMISSIONS_CSS = Path("src/front/static/global/css/permissions.css")


class TestPermissionsCssCleanup:
    """The version dropdown is gone from the Runs page (see
    TestRunsPartial.test_the_version_filter_is_gone), so the read-only-mode
    select exemption that named it is stale documentation debt."""

    def test_runs_version_filter_selector_is_gone(self):
        css = _PERMISSIONS_CSS.read_text(encoding="utf-8")
        assert "#runsVersionFilter" not in css

    def test_audit_version_filter_selector_survives(self):
        """#auditVersionFilter still backs a real control on Domain → Audit
        trail, so its exemption from the read-only select gate must stay."""
        css = _PERMISSIONS_CSS.read_text(encoding="utf-8")
        assert "#auditVersionFilter" in css


_JS = Path("src/front/static/domain/js/domain-runs.js")


def _js() -> str:
    return _JS.read_text(encoding="utf-8")


class TestRunsScript:
    def test_it_fetches_both_sources(self):
        src = _js()
        assert "/domain/build-runs" in src
        assert "/dtwin/metrics/history" in src

    def test_the_two_fetches_are_independent(self):
        """One endpoint failing must not blank the other table, so the two
        loads cannot share a try block or a Promise.all() that rejects as
        soon as either promise does. This deliberately does not forbid
        Promise.allSettled() — the one combinator that would make loader
        isolation structural rather than conventional."""
        src = _js()
        assert "Promise.all(" not in src
        assert src.count("async function _loadBuildRuns") == 1
        assert src.count("async function _loadAnalyticsRuns") == 1

    def test_analytics_status_has_its_own_badge_helper(self):
        """Analytics reports completed/failed; builds report
        success/error/cancelled. Overloading one helper would render every
        analytics row as an unknown-status grey badge."""
        src = _js()
        assert "_analyticsStatusBadge" in src
        assert "'completed'" in src or '"completed"' in src

    def test_the_version_dropdown_wiring_is_gone(self):
        src = _js()
        assert "runsVersionFilter" not in src
        assert "_populateRunsVersions" not in src
        assert "_runsVersionSel" not in src

    def test_failed_analytics_rows_do_not_show_zeroed_metrics(self):
        """A failed run records zeros, and printing them as real values
        would read as a graph with no nodes rather than a run that died.

        Extracts the _analyticsRunRow function body and checks that each
        numeric metric column is gated behind the `failed` check rather
        than always calling the raw formatter — a renderer that dropped the
        dashing and printed the stored zeros unconditionally would fail
        this (a bare `assert "_analyticsRunRow" in src` would not)."""
        src = _js()
        match = re.search(
            r"function _analyticsRunRow\(run, idx\) \{(.*?)\n\}",
            src,
            re.DOTALL,
        )
        assert match is not None, "_analyticsRunRow function not found"
        body = match.group(1)

        assert "const failed = " in body
        assert "const dash = " in body

        for metric in ("node_count", "edge_count", "connected_components"):
            assert f"failed ? dash : num(run.{metric})" in body, (
                f"{metric} column must dash out on a failed run, not print "
                "the stored zero"
            )
        assert 'failed ? dash : _esc((Number(run.avg_degree)' in body
        assert 'failed ? dash : _esc((Number(run.density)' in body

    def test_runs_loaded_flag_is_not_latched_unconditionally(self):
        """_runsLoaded must reflect whether the loads actually succeeded, not
        just that loadDomainRuns() ran.

        Before the fix, the body was:
            await _loadBuildRuns();
            await _loadAnalyticsRuns();
            _runsLoaded = true;
        which latches even when both loaders hit their catch block or their
        `!data.success` branch. This asserts the flag assignment is derived
        from values returned by the two loader calls — a regex that would
        NOT match the broken `_runsLoaded = true;` form above."""
        src = _js()
        match = re.search(
            r"async function loadDomainRuns\(\) \{(.*?)\n\}",
            src,
            re.DOTALL,
        )
        assert match is not None, "loadDomainRuns function not found"
        body = match.group(1)

        assert re.search(r"_runsLoaded\s*=\s*true\s*;", body) is None, (
            "loadDomainRuns() latches _runsLoaded unconditionally — a "
            "failed load will never be retried on re-entry"
        )
        assert re.search(r"=\s*await\s+_loadBuildRuns\(\)", body) is not None, (
            "_loadBuildRuns()'s return value must be captured"
        )
        assert re.search(r"=\s*await\s+_loadAnalyticsRuns\(\)", body) is not None, (
            "_loadAnalyticsRuns()'s return value must be captured"
        )
        assert re.search(r"_runsLoaded\s*=\s*\w+\s*&&\s*\w+\s*;", body) is not None, (
            "_runsLoaded must be set from the AND of both loaders' success "
            "results, not latched regardless of outcome"
        )

    def test_loaders_report_failure_on_both_error_paths(self):
        """Each loader must return false from its catch block AND from its
        `!data.success` branch — either path left returning undefined would
        make loadDomainRuns() treat that outcome as success (undefined is
        falsy today, but only by accident; this pins the explicit contract
        so a future refactor cannot silently drop it)."""
        src = _js()
        for name in ("_loadBuildRuns", "_loadAnalyticsRuns"):
            match = re.search(
                r"async function " + name + r"\(\) \{(.*?)\n\}",
                src,
                re.DOTALL,
            )
            assert match is not None, f"{name} function not found"
            body = match.group(1)
            error_branch = body[body.index("if (!data.success)"): body.index("catch (err)")]
            catch_branch = body[body.index("catch (err)"):]
            assert "return false;" in error_branch, (
                f"{name}'s `!data.success` branch must return false"
            )
            assert "return false;" in catch_branch, (
                f"{name}'s catch block must return false"
            )
            assert "return true;" in body, (
                f"{name} must return true on a successful load"
            )


# =====================================================
# HISTORY TAB REMOVED FROM THE ANALYTICS PAGE
# =====================================================


class TestHistoryTabRemoved:
    """Run history lives on the Runs page now, not behind the eighth tab of
    a page about the current result.

    File-reads prove the source partial is clean of every History-tab
    marker; a rendered-HTML assertion additionally proves the served
    ``/dtwin/`` page's tab strip no longer offers a History tab (not just
    that the string is absent from one file for unrelated reasons)."""

    @pytest.mark.parametrize(
        "marker",
        [
            "atab-btn-history",
            "atab-history",
            "analyticsLoadHistory",
            "analyticsHistoryBody",
            "analyticsHistoryEmpty",
        ],
    )
    def test_no_trace_of_the_history_tab_in_the_source(self, marker):
        assert marker not in _analytics_partial()

    def test_helpers_the_other_tabs_still_use_survive(self):
        """_formatComputedAt and _localName were used by the History rows
        but are used by other tabs too — deleting them would be over-reach."""
        src = _analytics_partial()
        assert "function _formatComputedAt" in src
        assert "function _localName" in src

    def test_history_tab_button_is_gone_from_the_rendered_page(self, client):
        """Rendered-HTML: the served /dtwin/ page's tab strip no longer has
        a button targeting #atab-history — proves the served page actually
        changed, not merely that the source file was edited."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        assert _find(tags, id_="atab-btn-history") is None
        assert _find(tags, attr=("data-bs-target", "#atab-history")) is None
        assert _find(tags, id_="atab-history") is None

    def test_the_remaining_analytics_tabs_render_in_order(self, client):
        """Rendered-HTML: the seven surviving tabs are still there, in
        their original order, on the served page — guards against
        accidentally deleting a neighboring tab along with History."""
        html = _html(client, "/dtwin/")
        tags = _tags(html)
        tab_button_ids = [
            a.get("id")
            for t, a in tags
            if t == "button" and (a.get("id") or "").startswith("atab-btn-")
        ]
        assert tab_button_ids == [
            "atab-btn-pagerank",
            "atab-btn-betweenness",
            "atab-btn-degree",
            "atab-btn-closeness",
            "atab-btn-clustering",
            "atab-btn-health",
            "atab-btn-insights",
        ]
