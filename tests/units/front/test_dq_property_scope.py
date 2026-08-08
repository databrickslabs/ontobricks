"""Contract: the Data Quality rule popup only offers the target entity's properties.

Both the property select and the condition rows resolve ownership through one
helper. A property belongs to an entity when its `domain` names that entity (as
a name or a URI) or when the entity inherits it from a parent. A property with
no declared domain belongs to no entity and must not be offered under all of
them, which is what the previous `|| !dom` filter did.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DATAQUALITY_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-dataquality.js"


def _method_body(name: str) -> str:
    """Return the body of the ``[async ]name(...) { ... }`` object method."""
    source = DATAQUALITY_JS.read_text(encoding="utf-8")
    match = re.search(rf"\n    (?:async\s+)?{re.escape(name)}\(", source)
    assert match, f"method {name} not found in ontology-dataquality.js"
    i = source.index("{", match.start()) + 1
    depth = 1
    body_start = i
    while i < len(source) and depth > 0:
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
        i += 1
    assert depth == 0, f"unbalanced braces in {name}"
    return source[body_start : i - 1]


def test_property_without_a_domain_belongs_to_no_entity():
    body = _method_body("_classOwnsProperty")
    assert "if (!domain) return false;" in body, (
        "a domain-less property must not be claimed by every entity"
    )


def test_ownership_tolerates_a_uri_domain():
    body = _method_body("_classOwnsProperty")
    assert "_localName(domain)" in body, (
        "domain may hold a full URI depending on the ingestion path"
    )
    assert "entry.cls.uri" in body, "a domain may also match the entity URI"


def test_inherited_properties_are_offered():
    chain = _method_body("_classChain")
    assert "cls.parent" in chain, "_classChain must follow the parent link"
    assert "seen" in chain, "_classChain must guard against inheritance cycles"

    body = _method_body("_propertiesForClass")
    assert "_classChain(className)" in body, (
        "_propertiesForClass must include inherited properties"
    )


def test_unknown_entity_offers_nothing():
    body = _method_body("_propertiesForClass")
    assert "if (!className) return [];" in body


def test_property_select_is_scoped_to_the_entity():
    body = _method_body("_populatePropertySelect")
    assert "this._propertiesForClass(className)" in body, (
        "the property select must resolve ownership, not filter inline"
    )
    assert "this.ontologyProperties" not in body, (
        "the property select must not read the global property list directly"
    )
    assert "|| !dom" not in body, (
        "regression: domain-less properties must not be offered for every entity"
    )


def test_conditions_use_the_same_scope_as_the_property_select():
    body = _method_body("_conditionProperties")
    assert "_propertiesForClass" in body


def test_an_untyped_property_counts_as_an_attribute():
    """The entity panel writes attributes with neither `type` nor `range`.

    Classifying those as relationships emptied the attribute dropdown for every
    entity whose attributes were not materialised in `dataProperties`.
    """
    body = _method_body("_isObjectProperty")
    assert "if (!range) return false;" in body, (
        "a property with no type and no range must be treated as an attribute"
    )
    assert "return this._isKnownClassName(range);" in body, (
        "an untyped property is a relationship only when its range names an entity"
    )
    assert not re.search(r"\breturn true;", body), (
        "regression: relationship must no longer be the fallback classification"
    )


def test_a_range_naming_an_entity_is_a_relationship():
    body = _method_body("_isKnownClassName")
    assert "this.ontologyClasses.some" in body
    assert "_localName(cls.uri" in body, "a range may be a URI or a local name"
