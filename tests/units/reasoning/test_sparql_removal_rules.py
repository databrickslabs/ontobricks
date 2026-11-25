from back.core.reasoning.rules.SPARQLRuleEngine import SPARQLRuleEngine

PFX = "PREFIX : <http://ex.org/onto#>\n"
GOOD = PFX + "DELETE WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }"


def test_removal_rule_validates():
    rule = {"name": "drop", "kind": "removal", "query": GOOD}
    assert SPARQLRuleEngine.validate_rule(rule) == []


def test_removal_rule_rejects_insert():
    rule = {"name": "x", "kind": "removal", "query": PFX + "INSERT DATA { :a :b :c }"}
    assert SPARQLRuleEngine.validate_rule(rule)


def test_default_kind_is_construct():
    rule = {
        "name": "vip",
        "query": PFX + "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b }",
    }
    assert SPARQLRuleEngine.validate_rule(rule) == []


def test_construct_rule_rejects_delete_query():
    rule = {"name": "x", "query": GOOD}  # kind defaults to construct
    assert SPARQLRuleEngine.validate_rule(rule)


def test_unknown_kind_is_reported():
    errors = SPARQLRuleEngine.validate_rule({"name": "x", "kind": "merge", "query": GOOD})
    assert errors == ["Unknown rule kind 'merge'"]
