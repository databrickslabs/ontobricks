from back.core.reasoning.results.ReasoningResult import ReasoningResult
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


# -- Execution ---------------------------------------------------------------


class FakeStore:
    """Returns canned rows for any SQL and records the statements it was given."""

    def __init__(self, rows):
        self.rows = rows
        self.sql = []

    def sql_flavor(self):
        return "spark"

    def sql_table_reference(self, name):
        return name

    def execute_query(self, sql):
        self.sql.append(sql)
        return self.rows


ONTO = {"base_uri": "http://ex.org/onto#", "classes": [], "properties": []}
M1 = "http://ex.org/onto#m1"
METER_OF = "http://ex.org/onto#meterOf"
C42 = "http://ex.org/onto#Customer_42"


def test_removal_rule_produces_removed_triples():
    rows = [{"s": M1, "p": METER_OF, "o": C42}]
    rule = {"name": "drop", "kind": "removal", "query": GOOD}
    result = SPARQLRuleEngine().execute_rules([rule], FakeStore(rows), "triples", ONTO)
    assert result.inferred_triples == []
    assert result.removed_triples == [{"subject": M1, "predicate": METER_OF, "object": C42}]
    assert result.stats["removed_count"] == 1


def test_removal_rule_does_not_infer_triples():
    rule = {"name": "drop", "kind": "removal", "query": GOOD}
    store = FakeStore([{"s": M1, "p": METER_OF, "o": C42}])
    result = SPARQLRuleEngine().execute_rules([rule], store, "triples", ONTO)
    assert result.violations == []
    assert "SELECT" in store.sql[0]


def test_disabled_removal_rule_is_skipped():
    rule = {"name": "drop", "kind": "removal", "query": GOOD, "enabled": False}
    store = FakeStore([{"s": "a", "p": "b", "o": "c"}])
    result = SPARQLRuleEngine().execute_rules([rule], store, "triples", ONTO)
    assert result.removed_triples == []
    assert store.sql == []


def test_delete_data_missing_triple_is_a_violation():
    rule = {"name": "gone", "kind": "removal", "query": PFX + "DELETE DATA { :m1 :meterOf :Customer_42 }"}
    result = SPARQLRuleEngine().execute_rules([rule], FakeStore([]), "triples", ONTO)
    assert result.removed_triples == []
    assert [v.check_type for v in result.violations] == ["removal_not_found"]
    assert result.violations[0].rule_type == "sparql"


def test_delete_data_present_triple_is_not_a_violation():
    rule = {"name": "ok", "kind": "removal", "query": PFX + "DELETE DATA { :m1 :meterOf :Customer_42 }"}
    store = FakeStore([{"s": M1, "p": METER_OF, "o": C42}])
    result = SPARQLRuleEngine().execute_rules([rule], store, "triples", ONTO)
    assert result.violations == []
    assert len(result.removed_triples) == 1


def test_removal_result_round_trips_in_to_dict():
    r = ReasoningResult()
    r.removed_triples.append({"subject": "a", "predicate": "b", "object": "c"})
    assert r.to_dict()["removed_triples"] == [{"subject": "a", "predicate": "b", "object": "c"}]


def test_merge_keeps_removed_triples():
    a, b = ReasoningResult(), ReasoningResult()
    b.removed_triples.append({"subject": "a", "predicate": "b", "object": "c"})
    a.merge(b)
    assert len(a.removed_triples) == 1


def test_construct_rule_still_infers_through_dispatcher():
    rule = {
        "name": "vip",
        "query": PFX + "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b }",
    }
    store = FakeStore([{"s": "http://ex.org/onto#c1", "p": "t", "o": "VIP"}])
    result = SPARQLRuleEngine().execute_rules([rule], store, "triples", ONTO)
    assert len(result.inferred_triples) == 1
    assert result.removed_triples == []
    assert result.stats["removed_count"] == 0
