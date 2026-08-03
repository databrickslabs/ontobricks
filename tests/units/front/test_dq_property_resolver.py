"""Behaviour of the Data Quality property resolver, executed with node.

``test_dq_property_scope.py`` pins the *shape* of the resolver; these tests run
it. A source contract cannot tell that ``_isObjectProperty`` classifies an
untyped property as a relationship, which is what emptied the attribute
dropdown for every entity whose attributes were not materialised in
``dataProperties``.

The fixture ontology mixes the shapes the ingestion paths actually produce:

* ``Customer`` — attributes in ``class.dataProperties`` (OWL import) plus a
  typed datatype property, and a typed relationship.
* ``Station`` — no ``dataProperties`` at all; its attributes exist only as
  top-level properties with no ``type`` and no ``range`` (entity panel), one of
  them with a full URI as ``domain``. It also owns an untyped relationship,
  recognisable only because its ``range`` names another entity.
* ``Line`` — declares nothing, the legitimate empty case.
* ``Loop`` / ``Knot`` — a parent cycle, which must not hang the chain walk.
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DATAQUALITY_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-dataquality.js"

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node is required to run the frontend module"
)

NS = "http://ex.org/onto/"

CLASSES = [
    {
        "name": "Person",
        "uri": f"{NS}Person",
        "parent": "",
        "dataProperties": [{"name": "fullName", "uri": f"{NS}fullName"}],
    },
    {
        "name": "Customer",
        "uri": f"{NS}Customer",
        "parent": "Person",
        "dataProperties": [{"name": "email", "uri": f"{NS}email"}],
    },
    {"name": "Station", "uri": f"{NS}Station", "parent": "", "dataProperties": []},
    {"name": "Line", "uri": f"{NS}Line", "parent": "", "dataProperties": []},
    {
        "name": "Loop",
        "uri": f"{NS}Loop",
        "parent": "Knot",
        "dataProperties": [{"name": "loopAttr", "uri": f"{NS}loopAttr"}],
    },
    {
        "name": "Knot",
        "uri": f"{NS}Knot",
        "parent": "Loop",
        "dataProperties": [{"name": "knotAttr", "uri": f"{NS}knotAttr"}],
    },
]

PROPERTIES = [
    # OWL import: explicit type, domain as a plain class name.
    {
        "name": "status",
        "uri": f"{NS}status",
        "domain": "Customer",
        "range": "xsd:string",
        "type": "DatatypeProperty",
    },
    {
        "name": "hasOrder",
        "uri": f"{NS}hasOrder",
        "domain": "Customer",
        "range": "Order",
        "type": "ObjectProperty",
    },
    # Also declared as a dataProperty of Customer — must be offered once.
    {
        "name": "email",
        "uri": f"{NS}email",
        "domain": "Customer",
        "range": "xsd:string",
        "type": "DatatypeProperty",
    },
    # Entity panel: no type, no range, domain as a URI.
    {"name": "platformCount", "uri": f"{NS}platformCount", "domain": f"{NS}Station"},
    # Entity panel: no type, no range, domain as a name.
    {"name": "stationName", "uri": f"{NS}stationName", "domain": "Station"},
    # Untyped, but the range names an entity — a relationship.
    {"name": "servesLine", "uri": f"{NS}servesLine", "domain": "Station", "range": "Line"},
    # Belongs to no entity.
    {"name": "orphan", "uri": f"{NS}orphan", "domain": "", "range": "", "type": ""},
]

_HARNESS = """
const fs = require('fs');
global.window = {};
const elements = {
    dqCategory: { value: __CATEGORY__ },
    dqProperty: { innerHTML: '', selectedIndex: -1 },
    dqPropertyLabel: { textContent: '' },
};
global.document = { getElementById: (id) => elements[id] || null };
eval(fs.readFileSync(__SOURCE__, 'utf8'));
const dq = window.DataQualityModule;
dq.ontologyClasses = __CLASSES__;
dq.ontologyProperties = __PROPERTIES__;
const result = (() => { __BODY__ })();
process.stdout.write(JSON.stringify(result === undefined ? null : result));
"""


def _evaluate(body: str, category: str = "conformance"):
    """Run *body* against the module and return its JSON-decoded value.

    ``body`` is a function body with `dq` (the module) and `elements` (the DOM
    stubs) in scope.
    """
    script = (
        _HARNESS.replace("__SOURCE__", json.dumps(str(DATAQUALITY_JS)))
        .replace("__CATEGORY__", json.dumps(category))
        .replace("__CLASSES__", json.dumps(CLASSES))
        .replace("__PROPERTIES__", json.dumps(PROPERTIES))
        .replace("__BODY__", body)
    )
    completed = subprocess.run(
        ["node", "-e", script], capture_output=True, text=True, timeout=30
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _resolve(class_name: str):
    """``{attributes, relationships}`` offered for *class_name*."""
    return _evaluate(
        f"""
        const owned = dq._propertiesForClass({json.dumps(class_name)});
        return {{
            attributes: owned.filter(p => !p.isRelationship).map(p => p.name),
            relationships: owned.filter(p => p.isRelationship).map(p => p.name),
        }};
        """
    )


def _populate(class_name: str, category: str):
    """The option labels and the field label the property select ends up with."""
    return _evaluate(
        f"""
        dq._populatePropertySelect({json.dumps(class_name)}, '');
        return {{
            html: elements.dqProperty.innerHTML,
            label: elements.dqPropertyLabel.textContent,
        }};
        """,
        category=category,
    )


def _options(html: str) -> list[str]:
    return re.findall(r"<option[^>]*>([^<]*)</option>", html)


# ---------------------------------------------------------------------------
# Which properties an entity owns
# ---------------------------------------------------------------------------


class TestPropertiesForClass:
    def test_materialised_and_declared_attributes_are_merged(self):
        assert set(_resolve("Customer")["attributes"]) == {"email", "status", "fullName"}

    def test_inherited_attributes_are_offered(self):
        assert "fullName" in _resolve("Customer")["attributes"]
        assert _resolve("Person")["attributes"] == ["fullName"]

    def test_attributes_known_only_as_top_level_properties(self):
        """The regression: Station has no dataProperties at all."""
        assert set(_resolve("Station")["attributes"]) == {"platformCount", "stationName"}

    def test_a_uri_domain_resolves_to_its_entity(self):
        assert "platformCount" in _resolve("Station")["attributes"]

    def test_an_entity_can_be_named_by_its_uri(self):
        by_uri = _resolve(f"{NS}Station")
        assert set(by_uri["attributes"]) == {"platformCount", "stationName"}
        assert by_uri["relationships"] == ["servesLine"]

    def test_a_property_is_offered_once(self):
        """email is both a dataProperty and a top-level property."""
        assert _resolve("Customer")["attributes"].count("email") == 1

    def test_properties_of_another_entity_never_leak(self):
        station = _resolve("Station")
        assert "status" not in station["attributes"]
        assert "hasOrder" not in station["relationships"]

    def test_a_property_without_a_domain_belongs_to_no_entity(self):
        for name in ("Customer", "Station", "Line", "Person"):
            offered = _resolve(name)
            assert "orphan" not in offered["attributes"] + offered["relationships"]

    def test_an_entity_declaring_nothing_offers_nothing(self):
        assert _resolve("Line") == {"attributes": [], "relationships": []}

    def test_an_unknown_entity_offers_nothing(self):
        assert _resolve("Ghost") == {"attributes": [], "relationships": []}

    def test_no_entity_offers_nothing(self):
        assert _resolve("") == {"attributes": [], "relationships": []}

    def test_an_inheritance_cycle_terminates(self):
        assert set(_resolve("Loop")["attributes"]) == {"loopAttr", "knotAttr"}


# ---------------------------------------------------------------------------
# Attribute vs relationship
# ---------------------------------------------------------------------------


class TestClassification:
    @pytest.mark.parametrize(
        "prop,expected",
        [
            ({"type": "ObjectProperty"}, True),
            ({"type": "owl:ObjectProperty"}, True),
            ({"type": "DatatypeProperty", "range": "Line"}, False),
            ({"range": "xsd:string"}, False),
            ({"range": "integer"}, False),
            # No evidence either way: the entity panel writes attributes this way.
            ({}, False),
            ({"type": "", "range": ""}, False),
            # Untyped, but the range names an entity.
            ({"range": "Line"}, True),
            ({"range": f"{NS}Line"}, True),
            ({"range": "line"}, True),
            # Untyped and the range names nothing we know.
            ({"range": "Whatever"}, False),
        ],
    )
    def test_is_object_property(self, prop, expected):
        assert _evaluate(f"return dq._isObjectProperty({json.dumps(prop)});") is expected

    def test_a_typed_relationship_stays_a_relationship(self):
        assert _resolve("Customer")["relationships"] == ["hasOrder"]

    def test_an_untyped_relationship_is_caught_by_its_range(self):
        assert _resolve("Station")["relationships"] == ["servesLine"]


# ---------------------------------------------------------------------------
# What the dropdown ends up showing
# ---------------------------------------------------------------------------


class TestPropertySelect:
    def test_attribute_dimensions_offer_attributes_and_the_label(self):
        result = _populate("Station", "conformance")
        assert _options(result["html"]) == [
            "Select attribute...",
            "platformCount",
            "stationName",
            "rdfs:label",
        ]
        assert result["label"] == "2. Attribute"

    def test_attribute_dimensions_exclude_relationships(self):
        for category in ("completeness", "conformance", "uniqueness"):
            assert "servesLine" not in _options(_populate("Station", category)["html"])

    def test_cardinality_offers_relationships_only(self):
        result = _populate("Station", "cardinality")
        assert _options(result["html"]) == ["Select relationship...", "servesLine"]
        assert result["label"] == "2. Relationship"

    def test_cardinality_does_not_offer_the_label_property(self):
        assert "rdfs:label" not in _options(_populate("Customer", "cardinality")["html"])

    def test_other_dimensions_offer_both(self):
        options = _options(_populate("Customer", "consistency")["html"])
        assert set(options) >= {"email", "status", "fullName", "hasOrder", "rdfs:label"}
        assert _populate("Customer", "consistency")["label"] == "2. Property"

    def test_an_entity_with_no_attribute_says_so(self):
        html = _populate("Line", "conformance")["html"]
        assert "No attribute declared on Line" in html
        assert "disabled" in html

    def test_an_entity_with_no_relationship_says_so(self):
        assert "No relationship declared on Line" in _populate("Line", "cardinality")["html"]

    def test_the_hint_is_absent_when_the_entity_has_properties(self):
        assert "No attribute declared" not in _populate("Station", "conformance")["html"]

    def test_no_entity_selected_shows_no_hint(self):
        assert "declared on" not in _populate("", "conformance")["html"]
