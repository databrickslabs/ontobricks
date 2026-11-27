from back.objects.ontology.Rules import Rules

CLASSES = {"meter", "customer"}
PROPS = {"meterof", "status"}  # lower-cased local names of known properties
PFX = "PREFIX : <http://ex.org/onto#>\n"


def test_unknown_property_in_removal_is_reported():
    rule = {"query": PFX + "DELETE WHERE { ?m :notAProperty :Customer_42 }"}
    errors = Rules.sparql_reference_errors(rule, CLASSES, PROPS)
    assert any("notaproperty" in e.lower() for e in errors)


def test_a_delete_is_checked_as_a_removal_without_any_kind_field():
    # The form decides the checks: an unknown property in a DELETE is reported.
    errors = Rules.sparql_reference_errors({"query": PFX + "DELETE WHERE { ?m :ghostProp :C }"}, CLASSES, PROPS)
    assert any("ghostprop" in e.lower() for e in errors)


def test_known_property_in_removal_is_accepted():
    rule = {"query": PFX + "DELETE WHERE { ?m :meterOf :Customer_42 }"}
    assert Rules.sparql_reference_errors(rule, CLASSES, PROPS) == []


def test_unknown_property_in_modify_template_is_reported():
    query = PFX + "DELETE { ?m :ghostProp ?o } WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }"
    errors = Rules.sparql_reference_errors({"query": query}, CLASSES, PROPS)
    assert any("ghostprop" in e.lower() for e in errors)


def test_unknown_property_in_modify_where_is_reported():
    query = PFX + "DELETE { ?m :status ?o } WHERE { ?m :ghostWhere :Customer_42 . ?m :status ?o }"
    errors = Rules.sparql_reference_errors({"query": query}, CLASSES, PROPS)
    assert any("ghostwhere" in e.lower() for e in errors)


def test_delete_data_has_no_pattern_to_check():
    rule = {"query": PFX + "DELETE DATA { :m1 :ghostProp :C }"}
    assert Rules.sparql_reference_errors(rule, CLASSES, PROPS) == []


def test_construct_rule_behaviour_is_unchanged():
    rule = {"query": "CONSTRUCT { ?c a :Ghost } WHERE { ?c a :Customer }"}
    errors = Rules.sparql_reference_errors(rule, CLASSES, PROPS)
    assert any("ghost" in e.lower() for e in errors)
