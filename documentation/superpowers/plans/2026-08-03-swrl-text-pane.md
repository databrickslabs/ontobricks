# SWRL Text Pane Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Business Rules **SWRL** button that toggles an inline text pane for viewing, exporting, and append-importing OntoBricks SWRL rules — mirroring the Data Quality SHACL pane.

**Architecture:** A small `SWRLTextCodec` serializes/parses OntoBricks SWRL text; three `/ontology/swrl/{text,export,import}` routes wrap it; `SwrlModule` owns the pane toggle/import UI parallel to `DataQualityModule`’s SHACL helpers.

**Tech Stack:** FastAPI, Python unit tests (pytest), Vanilla JS + Bootstrap 5, Jinja partials.

**Spec:** `documentation/superpowers/specs/2026-08-03-swrl-text-pane-design.md`

## Global Constraints

- Text format: `# Rule:` / optional `# Description:` / `antecedent -> consequent`; blank-line separated.
- Import: **always append**; duplicate names allowed.
- Fail closed: any invalid block aborts the whole import (no partial append).
- Pane is inline (`#brSwrlPanel`), not a Bootstrap modal; Import uses a small modal.
- Do not commit unless the user explicitly asks.
- After code changes: changelog + `uv run --frozen pytest -q -m "not scenario"`.

## File map

| File | Role |
|------|------|
| `src/back/core/reasoning/SWRLTextCodec.py` (new) | `serialize_rules` / `parse_rules` |
| `src/back/core/reasoning/__init__.py` | Export codec helpers |
| `src/api/routers/internal/ontology.py` | `GET text`, `GET export`, `POST import` |
| `src/front/templates/partials/ontology/_ontology_business_rules.html` | Button, panel, import modal |
| `src/front/static/ontology/js/ontology-swrl.js` | Toggle / refresh / export / import |
| `src/front/static/ontology/css/ontology-business-rules.css` | Editor styles |
| `tests/units/ontology/test_swrl_text_codec.py` (new) | Codec + append semantics |
| `tests/units/front/test_swrl_text_pane.py` (new) | Template/JS source contracts |

---

### Task 1: SWRL text codec (serialize + parse)

**Files:**
- Create: `src/back/core/reasoning/SWRLTextCodec.py`
- Modify: `src/back/core/reasoning/__init__.py`
- Test: `tests/units/ontology/test_swrl_text_codec.py`

**Interfaces:**
- Produces:
  - `serialize_rules(rules: list[dict]) -> str`
  - `parse_rules(text: str) -> list[dict]`  # raises `ValueError` on bad input
  - Each dict: `{ "name": str, "description": str, "antecedent": str, "consequent": str }`

- [ ] **Step 1: Write failing tests**

```python
# tests/units/ontology/test_swrl_text_codec.py
import pytest
from back.core.reasoning.SWRLTextCodec import parse_rules, serialize_rules

SAMPLE = [
    {
        "name": "Claiming customer must have contract",
        "description": "If claim then contract",
        "antecedent": "Customer(?c) ^ hasClaim(?c, ?cl)",
        "consequent": "hasContract(?c, ?ct)",
    },
    {
        "name": "Payment with invoices",
        "description": "",
        "antecedent": "Payment(?p) ^ hasInvoice(?p, ?i)",
        "consequent": "relatedTo(?p, ?i)",
    },
]


def test_serialize_empty():
    assert serialize_rules([]) == ""


def test_roundtrip():
    text = serialize_rules(SAMPLE)
    assert "# Rule: Claiming customer must have contract" in text
    assert "-> " in text
    back = parse_rules(text)
    assert len(back) == 2
    assert back[0]["name"] == SAMPLE[0]["name"]
    assert back[0]["antecedent"] == SAMPLE[0]["antecedent"]
    assert back[0]["consequent"] == SAMPLE[0]["consequent"]
    assert back[0]["description"] == SAMPLE[0]["description"]
    assert back[1]["description"] == ""


def test_missing_rule_name_synthesizes():
    text = "Customer(?c) -> VIP(?c)\n"
    rules = parse_rules(text)
    assert len(rules) == 1
    assert rules[0]["name"].startswith("Imported rule")


def test_bad_arrow_raises():
    with pytest.raises(ValueError, match="implication"):
        parse_rules("# Rule: Bad\nCustomer(?c)\n")


def test_empty_side_raises():
    with pytest.raises(ValueError, match="antecedent|consequent"):
        parse_rules("# Rule: Bad\n -> VIP(?c)\n")


def test_other_hash_comments_ignored():
    text = "# Rule: R1\n# note: ignored\nA(?x) -> B(?x)\n"
    rules = parse_rules(text)
    assert rules[0]["name"] == "R1"
    assert rules[0]["description"] == ""
```

- [ ] **Step 2: Run tests — expect FAIL** (module missing)

Run: `uv run --frozen pytest -q tests/units/ontology/test_swrl_text_codec.py`

- [ ] **Step 3: Implement codec**

```python
# src/back/core/reasoning/SWRLTextCodec.py
"""Serialize / parse OntoBricks SWRL text (IF → THEN blocks)."""
from __future__ import annotations

import re
from typing import Any, Dict, List

_RULE_RE = re.compile(r"^#\s*Rule:\s*(.*)$", re.IGNORECASE)
_DESC_RE = re.compile(r"^#\s*Description:\s*(.*)$", re.IGNORECASE)
_ARROW_RE = re.compile(r"\s*->\s*")


def serialize_rules(rules: List[Dict[str, Any]]) -> str:
    blocks: List[str] = []
    for rule in rules or []:
        name = (rule.get("name") or "").strip()
        if not name:
            continue
        lines = [f"# Rule: {name}"]
        desc = (rule.get("description") or "").strip()
        if desc:
            lines.append(f"# Description: {desc}")
        ant = (rule.get("antecedent") or "").strip()
        cons = (rule.get("consequent") or "").strip()
        lines.append(f"{ant} -> {cons}")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def parse_rules(text: str) -> List[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return []

    blocks = re.split(r"\n\s*\n", raw)
    rules: List[Dict[str, Any]] = []
    for i, block in enumerate(blocks, start=1):
        name = ""
        description = ""
        body_lines: List[str] = []
        for line in block.splitlines():
            s = line.strip()
            if not s:
                continue
            m_rule = _RULE_RE.match(s)
            if m_rule:
                name = m_rule.group(1).strip()
                continue
            m_desc = _DESC_RE.match(s)
            if m_desc:
                description = m_desc.group(1).strip()
                continue
            if s.startswith("#"):
                continue
            body_lines.append(s)

        if not body_lines:
            if name or description:
                raise ValueError(
                    f"Rule block {i}: missing implication (antecedent -> consequent)"
                )
            continue

        body = " ".join(body_lines)
        parts = _ARROW_RE.split(body, maxsplit=1)
        if len(parts) != 2:
            raise ValueError(
                f"Rule block {i}: missing implication (antecedent -> consequent)"
            )
        antecedent, consequent = parts[0].strip(), parts[1].strip()
        if not antecedent:
            raise ValueError(f"Rule block {i}: empty antecedent")
        if not consequent:
            raise ValueError(f"Rule block {i}: empty consequent")
        if not name:
            name = f"Imported rule {i}"
        rules.append(
            {
                "name": name,
                "description": description,
                "antecedent": antecedent,
                "consequent": consequent,
            }
        )
    return rules
```

Export `serialize_rules` and `parse_rules` from `back.core.reasoning.__init__` / `__all__`.

- [ ] **Step 4: Run tests — expect PASS**

Run: `uv run --frozen pytest -q tests/units/ontology/test_swrl_text_codec.py`

---

### Task 2: API routes

**Files:**
- Modify: `src/api/routers/internal/ontology.py` (after existing `/swrl/validate`)
- Test: extend `tests/units/ontology/test_swrl_text_codec.py` with a pure append helper test; route smoke optional via existing ontology API patterns if present

**Interfaces:**
- Consumes: `serialize_rules`, `parse_rules`, `Ontology.validate_swrl_rule`
- Produces:
  - `GET /ontology/swrl/text` → `{ success, text }`
  - `GET /ontology/swrl/export` → file download
  - `POST /ontology/swrl/import` `{ text }` → `{ success, rules, imported_count, message }`

- [ ] **Step 1: Add routes** (mirror SHACL turtle/export/import)

```python
@router.get("/swrl/text")
async def get_swrl_text(session_mgr: SessionManager = Depends(get_session_manager)):
    from back.core.reasoning.SWRLTextCodec import serialize_rules
    domain = get_domain(session_mgr)
    return {"success": True, "text": serialize_rules(domain.swrl_rules)}


@router.get("/swrl/export")
async def export_swrl(session_mgr: SessionManager = Depends(get_session_manager)):
    from fastapi.responses import Response
    from back.core.reasoning.SWRLTextCodec import serialize_rules
    domain = get_domain(session_mgr)
    text = serialize_rules(domain.swrl_rules)
    export_name = (
        domain._data.get("domain", domain._data.get("project", {}))
        .get("info", {})
        .get("name", DEFAULT_GRAPH_NAME)
    )
    filename = f"{export_name}_swrl_rules.swrl"
    return Response(
        content=text,
        media_type="text/plain",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/swrl/import")
async def import_swrl(
    request: Request, session_mgr: SessionManager = Depends(get_session_manager)
):
    from back.core.reasoning.SWRLTextCodec import parse_rules
    with map_route_errors("SWRL import failed", logger):
        data = await request.json()
        text = data.get("text", "")
        if not text or not str(text).strip():
            raise ValidationError("No SWRL text provided")
        try:
            imported = parse_rules(text)
        except ValueError as e:
            raise ValidationError(str(e)) from e
        if not imported:
            raise ValidationError("No valid SWRL rules found in the provided text")
        for rule in imported:
            errors = Ontology.validate_swrl_rule(rule)
            if errors:
                raise ValidationError(
                    f"Invalid rule '{rule.get('name', '')}': {'; '.join(errors)}"
                )
        domain = get_domain(session_mgr)
        rules = list(domain.swrl_rules)
        rules.extend(imported)
        domain.swrl_rules = rules
        domain.record_change(
            "swrl_added",
            entity_type="swrl",
            entity_ref=f"{len(imported)} imported",
            summary=f"Imported {len(imported)} SWRL rule(s)",
        )
        domain.save()
        return {
            "success": True,
            "message": f"Imported {len(imported)} rules",
            "rules": rules,
            "imported_count": len(imported),
        }
```

- [ ] **Step 2: Add append-semantics unit test**

```python
def test_append_leaves_existing():
    existing = [SAMPLE[0]]
    incoming = parse_rules(serialize_rules([SAMPLE[1]]))
    merged = list(existing) + incoming
    assert len(merged) == 2
    assert merged[0]["name"] == SAMPLE[0]["name"]
```

- [ ] **Step 3: Run codec tests — expect PASS**

---

### Task 3: Frontend pane + wiring

**Files:**
- Modify: `src/front/templates/partials/ontology/_ontology_business_rules.html`
- Modify: `src/front/static/ontology/js/ontology-swrl.js`
- Modify: `src/front/static/ontology/css/ontology-business-rules.css`
- Test: `tests/units/front/test_swrl_text_pane.py`

- [ ] **Step 1: Write front source-contract tests**

```python
# tests/units/front/test_swrl_text_pane.py
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HTML = (ROOT / "src/front/templates/partials/ontology/_ontology_business_rules.html").read_text()
JS = (ROOT / "src/front/static/ontology/js/ontology-swrl.js").read_text()


def test_swrl_button_and_panel_markup():
    assert "toggleSwrlPanel" in HTML
    assert 'id="brSwrlPanel"' in HTML
    assert 'id="brSwrlEditor"' in HTML
    assert 'id="brSwrlImportModal"' in HTML


def test_swrl_module_exposes_pane_helpers():
    for name in ("toggleSwrlPanel", "refreshSwrlText", "exportSwrl", "showSwrlImportModal", "doSwrlImport"):
        assert f"{name}(" in JS or f"{name} ()" in JS or f"async {name}(" in JS
```

- [ ] **Step 2: Run — expect FAIL**

- [ ] **Step 3: HTML** — add button after Auto-generate; panel after `#brTabContent` closing div (still inside card-body); import modal at end of partial (mirror DQ import modal).

Button:
```html
<button type="button" class="btn btn-outline-secondary" onclick="SwrlModule.toggleSwrlPanel()">
    <i class="bi bi-code-slash me-1"></i> SWRL
</button>
```

Panel structure mirrors `#dqShaclPanel` with ids `brSwrlPanel`, `brSwrlEditor`, actions calling `SwrlModule.refreshSwrlText` / `exportSwrl` / `showSwrlImportModal`.

- [ ] **Step 4: JS helpers on `SwrlModule`**

```javascript
_swrlPanelOpen: false,

toggleSwrlPanel() {
    this._swrlPanelOpen = !this._swrlPanelOpen;
    const panel = document.getElementById('brSwrlPanel');
    if (panel) panel.style.display = this._swrlPanelOpen ? '' : 'none';
    if (this._swrlPanelOpen) this.refreshSwrlText();
},

async refreshSwrlText() {
    try {
        const resp = await fetch('/ontology/swrl/text', { credentials: 'same-origin' });
        const data = await resp.json();
        if (data.success) {
            const el = document.getElementById('brSwrlEditor');
            if (el) el.value = data.text || '';
        }
    } catch (e) {
        console.error('[SWRL] Text refresh error:', e);
    }
},

exportSwrl() {
    window.location.href = '/ontology/swrl/export';
},

showSwrlImportModal() {
    const file = document.getElementById('brSwrlImportFile');
    const text = document.getElementById('brSwrlImportText');
    if (file) file.value = '';
    if (text) text.value = '';
    new bootstrap.Modal(document.getElementById('brSwrlImportModal')).show();
},

async doSwrlImport() {
    let text = (document.getElementById('brSwrlImportText')?.value || '').trim();
    const fileInput = document.getElementById('brSwrlImportFile');
    if (fileInput?.files?.length > 0 && !text) {
        text = await fileInput.files[0].text();
    }
    if (!text) {
        showNotification('Please provide SWRL text', 'warning');
        return;
    }
    try {
        const resp = await fetch('/ontology/swrl/import', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ text }),
        });
        const data = await resp.json();
        if (!resp.ok || !data.success) {
            showNotification(data.message || data.detail || 'Import failed', 'error');
            return;
        }
        this.rules = data.rules || [];
        this.renderRulesList();
        if (typeof BusinessRulesModule !== 'undefined' && BusinessRulesModule.updateBadges) {
            BusinessRulesModule.updateBadges();
        }
        bootstrap.Modal.getInstance(document.getElementById('brSwrlImportModal'))?.hide();
        showNotification('Imported ' + (data.imported_count || 0) + ' rules', 'success');
        if (this._swrlPanelOpen) this.refreshSwrlText();
    } catch (e) {
        console.error('[SWRL] Import error:', e);
        showNotification('Error importing SWRL: ' + (e.message || e), 'error');
    }
},
```

Confirm `renderRulesList` / badge update method names against existing `ontology-swrl.js` / `ontology-business-rules.js` and adjust calls to the real APIs.

- [ ] **Step 5: CSS** — `#brSwrlEditor { font-size: 0.8rem; background: var(--bs-gray-100); resize: vertical; }`

- [ ] **Step 6: Run front + codec tests — expect PASS**

---

### Task 4: Changelog + full suite

- [ ] Append section to `changelogs/v0.7.0/benoitcayladbx_2026-08-03.log` (version from `pyproject.toml`).
- [ ] Run: `uv run --frozen pytest -q -m "not scenario"`
- [ ] Report results; fix failures if any.

---

## Spec coverage check

| Spec item | Task |
|-----------|------|
| SWRL header button | 3 |
| Inline `#brSwrlPanel` + textarea | 3 |
| Refresh / Export / Import | 2–3 |
| OntoBricks text format | 1 |
| Append import | 2 |
| Fail closed on bad blocks | 1–2 |
| Out of scope DT/SPARQL/Agg | n/a (not built) |
| Tests | 1, 3, 4 |
