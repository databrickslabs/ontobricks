"""While GitHub settings load, title stays; the rest waits on the spinner.

The form, Test connection / Sync all toolbar and info banner used to paint
immediately while GET /settings/github was still in flight. They live in
``#githubLoaded``. The section title and subtitle stay visible next to the
spinner.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / "src/front/templates/partials/settings/_settings_github.html"
SCRIPT = ROOT / "src/front/static/config/js/settings-github.js"


@pytest.fixture(scope="module")
def template() -> str:
    return TEMPLATE.read_text()


@pytest.fixture(scope="module")
def script() -> str:
    return SCRIPT.read_text()


def test_loaded_body_starts_hidden(template):
    assert 'id="githubLoaded" class="d-none"' in template
    assert 'id="githubLoading"' in template
    assert "Loading GitHub settings" in template


def test_title_row_stays_visible_while_the_spinner_shows(template):
    header_end = template.index('id="githubLoading"')
    header = template[:header_end]
    loaded = template[template.index('id="githubLoaded"') :]

    assert "section-header" in header
    assert "<h4" in header and "GitHub" in header
    assert "Mirror every domain" in header
    assert 'id="btnSaveGithub"' in header
    assert "section-header" not in loaded


def test_toolbar_and_form_live_inside_the_hidden_loaded_body(template):
    before_loaded = template[: template.index('id="githubLoaded"')]
    loaded = template[template.index('id="githubLoaded"') :]

    assert "btnTestGithub" not in before_loaded
    assert "githubConfigForm" not in before_loaded
    assert "One repository per instance" not in before_loaded

    assert 'id="btnTestGithub"' in loaded
    assert 'id="githubConfigForm"' in loaded
    assert 'id="btnSyncAllGithub"' in loaded
    assert 'id="githubStatus"' in loaded
    assert "One repository per instance" in loaded


def test_script_reveals_the_loaded_body_only_after_fetch(script):
    assert "$('githubLoading')?.classList.add('d-none')" in script
    assert "$('githubLoaded')?.classList.remove('d-none')" in script
    assert "form.classList.remove('d-none')" not in script


def test_configuration_starts_hidden_until_mirror_is_enabled(template):
    assert 'id="githubEnabled"' in template
    assert 'id="githubDetails" class="d-none"' in template
    details = template[template.index('id="githubDetails"') :]
    enable = template[: template.index('id="githubDetails"')]
    assert 'id="githubEnabled"' in enable
    assert 'id="githubOwner"' in details
    assert 'id="btnTestGithub"' in details
    assert 'id="githubPushOnSave"' in details
    assert 'id="githubEnabled"' not in details


def test_script_toggles_configuration_with_the_enable_switch(script):
    assert "$('githubEnabled')?.addEventListener('change', syncDetailsVisibility)" in script
    assert "syncDetailsVisibility();" in script
    assert (
        "$('githubDetails')?.classList.toggle('d-none', !$('githubEnabled').checked)"
        in script
    )
