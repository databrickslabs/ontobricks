"""Guard Home / Explorer markup so e2e assertions cannot drift again.

The Playwright home/dtwin flows previously asserted a 3-card workflow strip
and a Knowledge Graph sidebar label. Those surfaces were replaced (All
Domains gateway + New Domain CTA; sigmagraph labelled Explorer) but the
e2e tests were not updated on ``develop``. This module pins the current
templates so ``pytest -m "not scenario"`` fails if they regress.
"""

from __future__ import annotations

import json
from pathlib import Path

_FRONT = Path(__file__).resolve().parents[3] / "src" / "front"
_HOME = _FRONT / "templates" / "home.html"
_MENU = _FRONT / "config" / "menu_config.json"


def test_home_is_all_domains_gateway_with_new_domain_cta():
    html = _HOME.read_text(encoding="utf-8")
    assert 'id="domainGateway"' in html
    assert "New Domain" in html
    assert "workflow-card" not in html
    assert 'id="classCount"' not in html
    assert 'id="propCount"' not in html
    assert 'id="mappingCount"' not in html


def test_sigmagraph_sidebar_label_is_explorer():
    cfg = json.loads(_MENU.read_text(encoding="utf-8"))
    dtwin = next(menu for menu in cfg["menus"] if menu["id"] == "digitaltwin")
    items = [item for group in dtwin["groups"] for item in group["items"]]
    sigma = next(item for item in items if item["id"] == "sigmagraph")
    assert sigma["label"] == "Explorer"
