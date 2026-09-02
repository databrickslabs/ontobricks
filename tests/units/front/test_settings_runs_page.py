"""Settings -> Automation -> Runs: the cross-domain twin of the Knowledge
Graph Runs page, and the removal of the Build Analytics page it replaces.

The layout mirrors ``_domain_runs.html`` (two tabs, one per run kind, each
owning its loading / empty / error elements) and adds the two things the
per-domain page has no use for: a Domain filter whose default is every domain,
and real pagination.

Most assertions drive a ``TestClient`` and parse the RENDERED ``/settings``
HTML, which proves the partial is actually included. File-reads are used only
where the rendered page cannot express the fact: which *file* holds a modal,
that a deleted file is gone, and the contents of the scripts.
"""

import json
import re
from pathlib import Path

import pytest

from tests.units.api.test_ui_rendering import _find, _html, _script_srcs, _tags

pytestmark = pytest.mark.unit

_SETTINGS_HTML = Path("src/front/templates/settings.html")
_PARTIAL = Path("src/front/templates/partials/settings/_settings_runs.html")
_OLD_PARTIAL = Path(
    "src/front/templates/partials/settings/_settings_build_analytics.html"
)
_OLD_JS = Path("src/front/static/config/js/build-analytics.js")
_SETTINGS_JS = Path("src/front/static/config/js/settings-runs.js")
_SHARED_JS = Path("src/front/static/global/js/runs-render.js")
_DOMAIN_JS = Path("src/front/static/domain/js/domain-runs.js")
_MENU = Path("src/front/config/menu_config.json")
_DTWIN_HTML = Path("src/front/templates/dtwin.html")
_DOMAIN_HTML = Path("src/front/templates/domain.html")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_tab_body() -> str:
    """The body of settings-runs.js's `loadTab`, for assertions about which
    element each of its branches shows or hides."""
    match = re.search(
        r"async function loadTab\(kind\) \{(.*?)\n    \}",
        _read(_SETTINGS_JS),
        re.DOTALL,
    )
    assert match is not None, "loadTab function not found"
    return match.group(1)


def _menu_items(menu_id: str, group_id: str):
    cfg = json.loads(_read(_MENU))
    menu = next(m for m in cfg["menus"] if m["id"] == menu_id)
    group = next(g for g in menu["groups"] if g["id"] == group_id)
    return group["items"]


class TestBuildAnalyticsIsGone:
    def test_the_section_no_longer_renders(self, client):
        html = _html(client, "/settings")
        assert 'id="build-analytics-section"' not in html
        assert _find(_tags(html), id_="buildAnalyticsDomain") is None

    def test_its_script_is_no_longer_loaded(self, client):
        html = _html(client, "/settings")
        assert not any("build-analytics.js" in src for src in _script_srcs(html))

    def test_its_files_are_deleted(self):
        assert not _OLD_PARTIAL.exists()
        assert not _OLD_JS.exists()

    def test_the_menu_item_is_replaced_by_runs(self):
        items = _menu_items("settings", "settings-automation")
        ids = [i["id"] for i in items]
        assert "build-analytics" not in ids
        assert ids == ["schedule", "runs"]

    def test_the_runs_menu_item_is_admin_only(self):
        """Every row of every domain is on this page, so a non-admin must not
        even be offered the sidebar entry — which is also what keeps the old
        wart from coming back, where the entry showed but its fetches 403'd."""
        runs = next(
            i for i in _menu_items("settings", "settings-automation")
            if i["id"] == "runs"
        )
        assert runs["admin_only"] is True
        assert runs["label"] == "Runs"


class TestRunsSection:
    def test_the_section_renders(self, client):
        html = _html(client, "/settings")
        assert 'id="runs-section"' in html

    def test_the_section_and_its_script_are_admin_gated(self):
        """Both must sit inside settings.html's ``user_role == 'admin'`` block,
        like every other admin-only section: the sidebar entry being hidden is
        a nicety, not a control, and shipping the markup to a non-admin only
        invites fetches that come back 403."""
        src = _read(_SETTINGS_HTML)
        admin_block_starts = [
            m.start() for m in re.finditer(r"\{% if user_role == 'admin' %\}", src)
        ]
        endifs = [m.start() for m in re.finditer(r"\{% endif %\}", src)]

        for marker in ('id="runs-section"', "config/js/settings-runs.js"):
            at = src.index(marker)
            opened = [s for s in admin_block_starts if s < at]
            assert opened, f"{marker} is outside every admin block"
            closing = next(e for e in endifs if e > max(opened))
            assert at < closing, f"{marker} falls after the admin block closes"

    def test_there_is_a_tab_for_each_history(self, client):
        html = _html(client, "/settings")
        tags = _tags(html)
        for element_id in (
            "settingsRunsTabs",
            "settingsRunsTabContent",
            "srtab-btn-build",
            "srtab-btn-analytics",
            "srtab-build",
            "srtab-analytics",
        ):
            assert _find(tags, id_=element_id) is not None

    def test_runs_tabs_use_card_integrated_canonical_hierarchy(self, client):
        html = _html(client, "/settings")
        section = html[html.index('id="runs-section"') :]
        assert '<div class="card h-100">' in section
        assert '<div class="card-body p-0 ob-tabs-wrap">' in section
        assert '<ul class="nav nav-tabs ob-tabs nav-fill" id="settingsRunsTabs" role="tablist">' in section
        assert '<div class="tab-content p-3" id="settingsRunsTabContent">' in section

    def test_runs_tab_content_surface_does_not_use_ob_tab_content_class(self, client):
        html = _html(client, "/settings")
        section = html[html.index('id="runs-section"') :]
        match = re.search(r'<div class="([^"]+)" id="settingsRunsTabContent">', section)
        assert match is not None
        assert "ob-tab-content" not in match.group(1)

    def test_runs_tab_icons_do_not_force_primary_or_success_colors(self, client):
        html = _html(client, "/settings")
        section = html[html.index('id="settingsRunsTabs"') : html.index('id="settingsRunsTabContent"')]
        assert "text-primary" not in section
        assert "text-success" not in section

    def test_each_tab_button_targets_its_own_pane(self, client):
        html = _html(client, "/settings")
        tags = _tags(html)
        for button_id, pane_id in (
            ("srtab-btn-build", "#srtab-build"),
            ("srtab-btn-analytics", "#srtab-analytics"),
        ):
            btn = _find(tags, id_=button_id)
            assert btn.get("data-bs-target") == pane_id
            assert btn.get("data-bs-toggle") == "tab"

    def test_build_runs_is_the_default_tab(self, client):
        html = _html(client, "/settings")
        tags = _tags(html)
        active_panes = [
            i for i in ("srtab-build", "srtab-analytics")
            if "active" in (_find(tags, id_=i).get("class") or "")
        ]
        assert active_panes == ["srtab-build"]

    @pytest.mark.parametrize(
        "element_id",
        [
            "srBuildLoading",
            "srBuildEmpty",
            "srBuildError",
            "srBuildErrorMessage",
            "srBuildTableWrapper",
            "srBuildTableBody",
            "srAnalyticsLoading",
            "srAnalyticsEmpty",
            "srAnalyticsError",
            "srAnalyticsErrorMessage",
            "srAnalyticsTableWrapper",
            "srAnalyticsTableBody",
        ],
    )
    def test_each_tab_owns_its_state_elements(self, client, element_id):
        """One tab's fetch failing must not blank the other, which is only
        possible if neither shares a loading / empty / error element."""
        html = _html(client, "/settings")
        assert _find(_tags(html), id_=element_id) is not None

    def test_each_table_lives_in_its_own_pane(self, client):
        html = _html(client, "/settings")
        build_pane = html.index('id="srtab-build"')
        analytics_pane = html.index('id="srtab-analytics"')

        assert build_pane < html.index('id="srBuildTableBody"') < analytics_pane
        assert analytics_pane < html.index('id="srAnalyticsTableBody"')

    def test_both_tables_name_a_domain_column(self, client):
        """The column that makes this page different from the per-domain one."""
        html = _html(client, "/settings")
        section = html[html.index('id="runs-section"'):]
        build = section[section.index('id="srBuildTableBody"'):]
        assert section.count(">Domain<") >= 2
        assert ">Domain<" in section[: section.index('id="srBuildTableBody"')]
        assert ">Domain<" in build

    def test_there_is_no_version_filter(self, client):
        """Version is a column here, as on the Knowledge Graph page — a
        dropdown would be meaningless while All domains is selected."""
        html = _html(client, "/settings")
        section = html[html.index('id="runs-section"'):]
        assert "VersionFilter" not in section


class TestDomainFilter:
    def test_the_dropdown_exists(self, client):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_="settingsRunsDomain") is not None

    def test_all_domains_is_the_first_option_and_submits_empty(self, client):
        """"All domains" has to bind to no folder at all, so its value must be
        empty rather than a sentinel string the backend would treat as a
        domain name."""
        html = _html(client, "/settings")
        select = html[html.index('id="settingsRunsDomain"'):]
        first_option = select[select.index("<option"): select.index("</select>")]
        assert 'value=""' in first_option
        assert "All domains" in first_option


class TestPagination:
    @pytest.mark.parametrize("prefix", ["srBuild", "srAnalytics"])
    @pytest.mark.parametrize(
        "suffix",
        ["Pagination", "PagingControls", "PageInfo", "Prev", "Next", "PageSize"],
    )
    def test_each_tab_has_its_own_pagination_controls(self, client, prefix, suffix):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_=f"{prefix}{suffix}") is not None

    @pytest.mark.parametrize("prefix", ["srBuild", "srAnalytics"])
    def test_the_rows_selector_sits_outside_the_hideable_paging_controls(
        self, client, prefix
    ):
        """A single-page result hides the page label and Prev/Next, but the
        Rows selector must survive it: hiding the whole footer strands a table
        whose page size can then only be changed by reloading — and on a tab
        that always fits one page, never at all."""
        html = _html(client, "/settings")
        section = html[html.index('id="runs-section"'):]

        rows_select = section.index(f'id="{prefix}PageSize"')
        controls = section.index(f'id="{prefix}PagingControls"')
        prev = section.index(f'id="{prefix}Prev"')

        assert rows_select < controls < prev

    @pytest.mark.parametrize("prefix", ["srBuild", "srAnalytics"])
    def test_the_footer_starts_hidden_by_class_not_by_inline_style(
        self, client, prefix
    ):
        """The footer is a `d-flex` row, and Bootstrap declares
        `.d-flex { display: flex !important }` — which beats any inline
        `display:none`. Hiding it has to go through a class, or a single-page
        result shows an orphan footer with a stale label."""
        html = _html(client, "/settings")
        footer = _find(_tags(html), id_=f"{prefix}Pagination")
        classes = (footer.get("class") or "").split()

        assert "d-none" in classes
        assert "display:none" not in (footer.get("style") or "").replace(" ", "")

    def test_the_script_toggles_the_footer_by_class(self):
        """Same reason, from the other side: a `style.display` assignment on
        the pagination element is silently a no-op."""
        src = _read(_SETTINGS_JS)
        assert "ids.pagination).style.display" not in src
        assert "d-none" in src

    def test_the_footer_hides_on_an_empty_result_not_on_a_single_page(self):
        src = _read(_SETTINGS_JS)
        assert re.search(
            r"nav\.classList\.toggle\('d-none', st\.rows\.length === 0\)", src
        )

    def test_a_single_page_hides_the_paging_controls_only(self):
        src = _read(_SETTINGS_JS)
        match = re.search(r"controls\.classList\.toggle\('d-none',([^)]*)\)", src)
        assert match is not None
        assert "st.total <= st.limit" in match.group(1)

    def test_a_reload_leaves_the_rows_selector_on_screen(self):
        """The loader's pre-fetch reset must not take the whole footer down:
        on a slow registry the Rows control would visibly vanish and reappear
        on every reload. Only the now-stale paging controls go."""
        reset = _load_tab_body()[: _load_tab_body().index("try {")]

        assert "ids.pagingControls).classList.add('d-none')" in reset
        assert "ids.pagination)" not in reset

    @pytest.mark.parametrize("branch", ["if (!data.success)", "catch (err)"])
    def test_a_failed_load_hides_the_footer_along_with_the_table(self, branch):
        """Paging controls for a table that is no longer on screen would
        invite a click that pages nothing, and renderPagination() — which
        normally owns the footer's final state — never runs on this path."""
        body = _load_tab_body()
        slice_ = body[body.index(branch):]
        if branch == "if (!data.success)":
            slice_ = slice_[: slice_.index("catch (err)")]

        assert "ids.pagination).classList.add('d-none')" in slice_

    @pytest.mark.parametrize("prefix", ["srBuild", "srAnalytics"])
    def test_page_size_offers_25_50_100(self, client, prefix):
        html = _html(client, "/settings")
        select = html[html.index(f'id="{prefix}PageSize"'):]
        options = select[: select.index("</select>")]
        for size in ("25", "50", "100"):
            assert f'value="{size}"' in options


class TestDetailsModals:
    @pytest.mark.parametrize(
        "element_id",
        [
            "srRunDetailsModal",
            "srRunDetailsBody",
            "srAnalyticsRunDetailsModal",
            "srAnalyticsRunDetailsBody",
        ],
    )
    def test_both_modals_render(self, client, element_id):
        html = _html(client, "/settings")
        assert _find(_tags(html), id_=element_id) is not None

    @pytest.mark.parametrize(
        "modal_id", ["srRunDetailsModal", "srAnalyticsRunDetailsModal"]
    )
    def test_modals_are_page_level_not_inside_the_section(self, modal_id):
        """A modal inside a hidden .sidebar-section shows its backdrop but
        leaves the dialog invisible, so both must live in settings.html
        itself rather than in the included partial."""
        assert f'id="{modal_id}"' in _read(_SETTINGS_HTML)
        assert modal_id not in _read(_PARTIAL)

    @pytest.mark.parametrize(
        "modal_id", ["srRunDetailsModal", "srAnalyticsRunDetailsModal"]
    )
    def test_modal_aria_labelledby_points_at_a_real_id(self, client, modal_id):
        html = _html(client, "/settings")
        tags = _tags(html)
        label_id = _find(tags, id_=modal_id).get("aria-labelledby")
        assert label_id
        assert _find(tags, id_=label_id) is not None


class TestSharedRenderer:
    """The rendering half of domain-runs.js moved to a shared module so the
    two pages cannot drift apart — and so the modal bodies stay identical by
    construction rather than by review."""

    def test_the_module_exists_and_is_namespaced(self):
        src = _read(_SHARED_JS)
        assert "window.RunsRender" in src

    @pytest.mark.parametrize(
        "member",
        [
            "esc",
            "fmtTs",
            "fmtDuration",
            "fmtMillis",
            "buildStatusBadge",
            "analyticsStatusBadge",
            "kindBadge",
            "analyticsScope",
            "buildRunDetailsHtml",
            "analyticsRunDetailsHtml",
        ],
    )
    def test_it_exposes_the_shared_members(self, member):
        assert re.search(rf"\b{member}\b", _read(_SHARED_JS))

    def test_the_modal_builders_return_html_instead_of_writing_the_dom(self):
        """Returning a string is what lets each page inject into its own
        modal body; a builder that reached for a hardcoded element id could
        only ever serve one page."""
        src = _read(_SHARED_JS)
        assert "runDetailsBody" not in src
        assert "analyticsRunDetailsBody" not in src

    def test_the_domain_row_is_conditional_on_the_row_carrying_one(self):
        """Per-domain rows from /domain/build-runs have no domain key and must
        render exactly as they did before the extraction."""
        assert "run.domain" in _read(_SHARED_JS)

    def test_domain_runs_no_longer_declares_the_extracted_helpers(self):
        src = _read(_DOMAIN_JS)
        for helper in (
            "function _esc(",
            "function _fmtTs(",
            "function _fmtDuration(",
            "function _statusBadge(",
            "function _kindBadge(",
            "function _statsTable(",
            "function _phaseTable(",
        ):
            assert helper not in src, f"{helper} should now come from RunsRender"

    def test_domain_runs_still_exports_the_helper_the_audit_trail_calls(self):
        """domain-audit.js reuses window.showRunDetailsObj for build entries
        in the audit timeline."""
        assert "window.showRunDetailsObj" in _read(_DOMAIN_JS)

    @pytest.mark.parametrize(
        "path,consumer",
        [
            ("/dtwin/", "domain-runs.js"),
            ("/domain", "domain-runs.js"),
            ("/settings", "settings-runs.js"),
        ],
    )
    def test_the_shared_module_loads_before_its_consumer(self, client, path, consumer):
        """RunsRender is read at call time, but the consumers are plain
        scripts, so a swapped order would leave the namespace undefined for
        anything that runs on DOMContentLoaded."""
        srcs = _script_srcs(_html(client, path))
        shared = next(i for i, s in enumerate(srcs) if "runs-render.js" in s)
        used = next(i for i, s in enumerate(srcs) if consumer in s)
        assert shared < used

    def test_the_shared_module_is_served_on_all_three_pages(self, client):
        for path in ("/settings", "/dtwin/", "/domain"):
            html = _html(client, path)
            assert any("runs-render.js" in src for src in _script_srcs(html)), (
                f"runs-render.js missing from {path}"
            )


class TestSettingsRunsScript:
    def test_it_fetches_both_new_endpoints(self):
        src = _read(_SETTINGS_JS)
        assert "/settings/runs/build" in src
        assert "/settings/runs/analytics" in src

    def test_it_populates_the_dropdown_from_the_existing_domains_endpoint(self):
        assert "/settings/registry/domains" in _read(_SETTINGS_JS)

    def test_the_two_fetches_are_independent(self):
        """Same isolation rule as the Knowledge Graph page: one tab's endpoint
        failing must leave the other's table on screen."""
        assert "Promise.all(" not in _read(_SETTINGS_JS)

    def test_it_reuses_the_shared_renderer(self):
        assert "RunsRender" in _read(_SETTINGS_JS)

    def test_changing_the_filter_or_page_size_returns_to_the_first_page(self):
        """Keeping the offset across a filter change lands the user on a page
        that may not exist in the new result set — an empty table that looks
        like "no runs"."""
        src = _read(_SETTINGS_JS)
        assert re.search(r"offset\s*=\s*0", src)

    def test_it_loads_on_entering_the_runs_section(self):
        src = _read(_SETTINGS_JS)
        assert "sidebarSectionChanged" in src
        assert "'runs'" in src or '"runs"' in src
