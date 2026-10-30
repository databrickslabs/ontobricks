"""Hypothetical-fact wizard: three steps, no inline handlers, reuses graph search."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
FRONT = REPO_ROOT / "src/front"
MODAL = FRONT / "templates/partials/simulation/_sim_fact_wizard.html"
JS = FRONT / "static/simulation/js/simulation-fact-wizard.js"
WORKBENCH = FRONT / "templates/partials/simulation/_sim_workbench.html"
PAGE = FRONT / "templates/simulation.html"
WB_JS = FRONT / "static/simulation/js/simulation-workbench.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_page_wires_wizard_partial_and_script():
    page = _read(PAGE)
    assert 'partials/simulation/_sim_fact_wizard.html' in page
    assert "simulation/js/simulation-fact-wizard.js" in page


def test_workbench_exposes_wizard_and_per_field_lookup():
    html = _read(WORKBENCH)
    assert 'id="simWbFactWizardBtn"' in html
    js = _read(WB_JS)
    assert "data-lookup" in js
    assert "SimulationFactWizard.open" in js


def test_entity_neighbourhood_has_graph_search_picker():
    html = _read(WORKBENCH)
    js = _read(WB_JS)
    wizard = _read(JS)
    assert 'id="simWbFocusSearchBtn"' in html
    assert "openScopeEntityPicker" in js
    assert "applyScopeEntity" in js
    assert "pick: 'scope'" in js
    assert "_syncPickMode" in wizard
    assert "Choose neighbourhood entity" in wizard
    assert "Use this entity" in wizard


def test_wizard_has_subject_predicate_object_steps_without_inline_handlers():
    html = _read(MODAL)
    assert 'id="simFactWizardModal"' in html
    for step_id in ("simFactStepSubject", "simFactStepPredicate", "simFactStepObject"):
        assert f'id="{step_id}"' in html
    assert 'id="simFactSearchBtn"' in html
    assert 'id="simFactApplyBtn"' in html
    assert "onclick=" not in html
    assert "style=" not in html


def test_wizard_locks_operation_after_subject_and_adapts_copy():
    src = _read(JS)
    assert "_syncOpUi" in src
    assert "simFactOp" in src
    assert "disabled" in src
    assert "Apply set" in src and "Apply add" in src and "Apply remove" in src
    assert "op.disabled = this._step !== 1" in src


def test_wizard_hit_lists_show_local_name_not_full_uri():
    src = _read(JS)
    assert "this._local(s.uri)" in src
    assert 'data-label="${SimulationPage.esc(p.label)}"' in src
    assert "_objectDisplayLabel" in src
    assert "predicate_label" in src


def test_wizard_js_searches_live_graph_and_ontology():
    src = _read(JS)
    assert "/dtwin/sync/filter" in src
    assert '"phase": "preview"' in src or "phase: 'preview'" in src
    assert "/ontology/get-loaded-ontology" in src
    assert "spinner-border spinner-border-sm" in src
    assert "alert(" not in src


def test_wizard_lists_only_properties_of_subject_class():
    html = _read(MODAL)
    src = _read(JS)
    assert "simFactPredAll" not in html
    assert "Show all" not in html
    assert "_declaringKeys" in src
    assert "_ensureSubjectType" in src
    assert "showAll" not in src
    assert "including inherited" in src
