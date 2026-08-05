# Knowledge Graph Icon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Knowledge Graph's colliding `bi-box` / `bi-box-fill` icons with `bi-radar` everywhere the section is labelled.

**Architecture:** Four literal icon strings change; the navbar and level-2 subnav already read the menu config, so they inherit. A static contract test locks Domain ≠ Knowledge Graph and menu ≡ breadcrumb.

**Tech Stack:** Bootstrap Icons 1.11.2 (`bi-radar`), `menu_config.json`, Jinja templates, pytest unit static contracts.

## Global Constraints

- Icon value is exactly `bi-radar` (no fill variant exists in 1.11.2).
- Domain stays `bi-box`; do not touch unrelated `bi-box` / `bi-box-arrow-*` usages.
- Version folder for changelog: `v0.7.0` (from `pyproject.toml`).
- Tests: `uv run --frozen pytest -q -m "not scenario"`.

---

### Task 1: Static contract (TDD)

**Files:**
- Create: `tests/units/front/test_knowledge_graph_icon.py`
- Modify: (none yet)

**Interfaces:**
- Consumes: `src/front/config/menu_config.json`, `src/front/static/global/js/breadcrumb.js`
- Produces: assertions that fail until Task 2 lands

- [x] **Step 1: Write the failing test**

```python
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
        rf'bi {_KG_ICON}[^"]*"[^>]*>\s*<strong>5\. Knowledge Graph</strong>',
        text,
    )


def test_domain_validation_card_uses_kg_icon():
    text = _VALIDATION.read_text(encoding="utf-8")
    assert f'bi {_KG_ICON}' in text
    assert "Knowledge Graph" in text
    assert "bi-box-fill" not in text or "Knowledge Graph" not in text.split("bi-box-fill")[0][-80:]
```

Prefer a clearer validation assertion:

```python
def test_domain_validation_card_uses_kg_icon():
    text = _VALIDATION.read_text(encoding="utf-8")
    assert re.search(
        rf'bi {_KG_ICON}[^"]*"[^>]*>\s*Knowledge Graph',
        text,
    )
```

- [x] **Step 2: Run test to verify it fails**

Run: `uv run --frozen pytest -q tests/units/front/test_knowledge_graph_icon.py -m "not scenario"`
Expected: FAIL on `digitaltwin_uses_bi_radar` (still `bi-box`).

- [x] **Step 3: Apply the four icon edits**

1. `menu_config.json` → `"digitaltwin"."icon": "bi-radar"`
2. `breadcrumb.js` → `'/dtwin/': … icon: 'bi-radar'`
3. `help_modal.html` → `bi bi-radar` on step 5
4. `_domain_validation.html` → `bi bi-radar` on the Knowledge Graph card header

- [x] **Step 4: Re-run the contract test**

Expected: PASS

- [x] **Step 5: Full suite + changelog**

Run: `uv run --frozen pytest -q -m "not scenario"`
Append section to `changelogs/v0.7.0/benoitcayladbx_2026-08-05.log`

---

## Spec coverage

| Spec requirement | Task |
|---|---|
| `menu_config.json` digitaltwin → `bi-radar` | 1 |
| breadcrumb `/dtwin/` → `bi-radar` | 1 |
| help modal step 5 → `bi-radar` | 1 |
| domain validation card → `bi-radar` | 1 |
| contract: icons differ + menu≡breadcrumb | 1 |
| changelog + full pytest | 1 |
