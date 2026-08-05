"""Knowledge Graph icon must stay distinct from Domain and match the breadcrumb."""

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_MENU = Path("src/front/config/menu_config.json")
_BREADCRUMB = Path("src/front/static/global/js/breadcrumb.js")
_HELP = Path("src/front/templates/partials/layout/help_modal.html")
_VALIDATION = Path("src/front/templates/partials/domain/_domain_validation.html")

_KG_ICON = "bi-radar"


def _menus():
    return {m["id"]: m for m in json.loads(_MENU.read_text(encoding="utf-8"))["menus"]}


def test_digitaltwin_uses_bi_radar():
    assert _menus()["digitaltwin"]["icon"] == _KG_ICON


def test_domain_and_digitaltwin_icons_differ():
    menus = _menus()
    assert menus["domain"]["icon"] != menus["digitaltwin"]["icon"]


def test_breadcrumb_dtwin_matches_menu_icon():
    text = _BREADCRUMB.read_text(encoding="utf-8")
    match = re.search(r"'/dtwin/'\s*:\s*\{[^}]*icon:\s*'([^']+)'", text)
    assert match is not None, "/dtwin/ breadcrumb entry missing"
    assert match.group(1) == _menus()["digitaltwin"]["icon"]


def test_help_modal_walkthrough_uses_kg_icon():
    text = _HELP.read_text(encoding="utf-8")
    assert re.search(
        rf'<i class="bi {_KG_ICON}[^"]*"></i>\s*<strong>5\. Knowledge Graph</strong>',
        text,
    )


def test_domain_validation_card_uses_kg_icon():
    text = _VALIDATION.read_text(encoding="utf-8")
    assert re.search(
        rf'<i class="bi {_KG_ICON}[^"]*"></i>\s*Knowledge Graph',
        text,
    )
