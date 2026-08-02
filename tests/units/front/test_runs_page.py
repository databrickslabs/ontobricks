"""Knowledge Graph → Management → Runs renders two independent tables.

Build runs and analytics runs share no columns, so they are stacked rather
than merged. Each card owns its loading / empty / error elements: a failure
fetching one must not blank the other.

Most assertions drive a ``TestClient`` and parse the RENDERED HTML (via the
``_html`` / ``_tags`` / ``_find`` helpers from ``test_ui_rendering.py``),
which proves the partial is actually included and rendered on ``/dtwin/``.
A few assertions fall back to reading a template file directly off disk —
only where the rendered page genuinely cannot express the fact: e.g. that
the analytics modal lives in ``dtwin.html`` (not merged into the partial's
output by inclusion) or that an id is absent from one specific source file.
"""

from pathlib import Path

import pytest

from tests.units.api.test_ui_rendering import _find, _html, _tags

pytestmark = pytest.mark.unit

_PARTIAL = Path("src/front/templates/partials/domain/_domain_runs.html")
_DTWIN = Path("src/front/templates/dtwin.html")


def _partial() -> str:
    return _PARTIAL.read_text(encoding="utf-8")


class TestRunsPartial:
    """Rendered-HTML assertions: prove both cards' elements actually show up
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
    def test_both_cards_have_their_own_elements(self, client, element_id):
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
