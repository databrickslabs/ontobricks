"""Contracts for the Ontology Studio entity panel tabs.

Tabs are grouped by what a section does: General (identity), Attrs. (mapped
then virtual attributes), Model (OWL axioms + bridges), Behaviour (what the
Knowledge Graph can run on one entity, each with an effect chip) and Links
(dashboard, dataset). The five labels must fit the compact rail at the default 420px
panel width, hence the one abbreviation.
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
STATIC = REPO_ROOT / "src/front/static"
PANELS_JS = STATIC / "ontology/js/ontology-shared-panels.js"
MAP_JS = STATIC / "ontology/js/ontology-map.js"
SIGMA_JS = STATIC / "query/js/query-sigmagraph.js"
DETAILS_JS = STATIC / "query/js/query-entity-details.js"

_ORDER = ("details", "attributes", "model", "behaviour", "links")


def _js() -> str:
    return PANELS_JS.read_text(encoding="utf-8")


def _pane(js: str, tab: str) -> str:
    start = js.index(f'data-form-tab-content="{tab}"')
    nxt = _ORDER.index(tab) + 1
    end = (
        js.index(f'data-form-tab-content="{_ORDER[nxt]}"', start)
        if nxt < len(_ORDER)
        else js.index("</form>", start)
    )
    return js[start:end]


def test_entity_rail_has_five_tabs_in_order():
    js = _js()
    nav = js[js.index('id="sharedEntityForm"') : js.index('data-form-tab-content="details"')]
    expected = [
        "{ id: 'details', icon: 'bi-info-circle', label: 'General' }",
        "{ id: 'attributes', icon: 'bi-tags', label: 'Attrs.' }",
        "{ id: 'model', icon: 'bi-diagram-2', label: 'Model' }",
        "{ id: 'behaviour', icon: 'bi-lightning', label: 'Behaviour' }",
        "{ id: 'links', icon: 'bi-link-45deg', label: 'Links' }",
    ]
    positions = [nav.index(item) for item in expected]
    assert positions == sorted(positions)
    for stale in ("'Infos'", "'Refs.'", "label: 'Actions'", "label: 'Constraints'", "label: 'Semantics'"):
        assert stale not in nav


def test_unknown_remembered_tab_falls_back_to_general():
    js = _js()
    assert "const ENTITY_PANEL_TABS = ['details', 'attributes', 'model', 'behaviour', 'links'];" in js
    assert "ENTITY_PANEL_TABS.includes(_entityPanelActiveTab) ? _entityPanelActiveTab : 'details'" in js


def test_general_holds_identity_and_group_only():
    pane = _pane(_js(), "details")
    for el in ("sharedEntityName", "sharedEntityLabel", "sharedEntityIcon", "sharedEntityDescription", "sharedEntityGroup"):
        assert f'id="{el}"' in pane
    assert 'id="sharedEntityParent"' not in pane
    assert 'id="sharedEntityInheritedRelations"' not in pane


def test_attributes_holds_mapped_then_virtual_attributes():
    pane = _pane(_js(), "attributes")
    assert pane.index('id="sharedEntityAttributes"') < pane.index('id="sharedEntityVirtualAttributes"')
    assert "title: 'Virtual attributes'" in pane
    assert pane.count("chip: '") == 1 and "chip: 'viewed'" in pane


def test_model_holds_inheritance_axioms_and_bridges():
    pane = _pane(_js(), "model")
    for el in (
        "sharedEntityParent",
        "sharedEntityInheritedRelations",
        "sharedEntityEquivalentTo",
        "sharedEntityDisjointWith",
        "sharedEntityBridges",
    ):
        assert f'id="{el}"' in pane


def test_behaviour_lists_runtime_features_in_effect_order_with_chips():
    pane = _pane(_js(), "behaviour")
    boxes = [
        'id="sharedEntityActions"',
        'id="sharedEntityJobs"',
        'id="sharedEntityBusinessRules"',
    ]
    positions = [pane.index(b) for b in boxes]
    assert positions == sorted(positions)
    assert 'id="sharedEntityVirtualAttributes"' not in pane
    headings = [pane.index(f"title: '{h}'") for h in ("Functions", "Jobs", "Inference rules (SWRL)")]
    chips = [pane.index(f"chip: '{c}'") for c in ("result", "background", "graphWrite")]
    assert headings == sorted(headings) and chips == sorted(chips)
    assert pane.count("chip: '") == 3
    assert "UC functions" not in pane
    assert "Business rules" not in pane


def test_effect_chip_labels_are_defined_once():
    js = _js()
    for key, label in (
        ("viewed", "Computed when viewed"),
        ("result", "Returns a result"),
        ("background", "Runs in background"),
        ("graphWrite", "Writes to the graph"),
    ):
        assert f"{key}: {{ icon: 'bi-" in js
        assert js.count(f"label: '{label}'") == 1


def test_links_holds_dashboard_and_dataset_only():
    pane = _pane(_js(), "links")
    assert 'id="sharedEntityDashboard"' in pane
    assert 'id="sharedEntityDataset"' in pane
    assert 'id="sharedEntityBridges"' not in pane


def test_relationship_rail_says_general_not_details():
    js = _js()
    nav = js[js.index('id="sharedRelationshipForm"') :][:400]
    assert "{ id: 'details', icon: 'bi-info-circle', label: 'General' }" in nav
    assert "{ id: 'constraints', icon: 'bi-sliders', label: 'Constraints' }" in nav


def test_designer_context_menu_mirrors_the_entity_rail():
    js = MAP_JS.read_text(encoding="utf-8")
    assert "['details', 'attributes', 'model', 'behaviour', 'links'].forEach(tab =>" in js
    menu = js[js.index("function showMapContextMenu(") : js.index("function showMapRelationshipContextMenu(")]
    for tab, label in (
        ("details", "General"),
        ("attributes", "Attributes"),
        ("model", "Model"),
        ("behaviour", "Behaviour"),
        ("links", "Links"),
    ):
        assert f'data-action="open-tab-{tab}"' in menu
        assert f"<span>{label}</span>" in menu
    assert "<span>Refs.</span>" not in menu


def test_graph_explorer_groups_behaviour_actions():
    sigma = SIGMA_JS.read_text(encoding="utf-8")
    assert "'<div class=\"ctx-header\">Behaviour</div>'" in sigma
    assert "'Behaviour (' + behaviourCount + ')'" in sigma
    assert "'<div class=\"ctx-header\">Inference rules</div>'" not in sigma
    assert "'<div class=\"ctx-header\">Actions</div>'" not in sigma
    assert "Business rules" not in sigma
    details = DETAILS_JS.read_text(encoding="utf-8")
    assert '<i class="bi bi-lightning"></i> Behaviour</h6>' in details
    assert "Inference rules</h6>" not in details
    assert "Actions</h6>" not in details
