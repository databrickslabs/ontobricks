"""DecisionTableEngine semantics: optional columns, Unique hit policy, Assign entity."""

from back.core.reasoning.DecisionTableEngine import DecisionTableEngine
from back.core.reasoning.constants import RDF_TYPE
from back.objects.ontology.OntologyRules import OntologyRules

BASE = "http://test.org/ontology#"
DATA = "http://test.org/ontology/"
ONTOLOGY = {
    "base_uri": BASE,
    "classes": [
        {"name": "Customer", "uri": BASE + "Customer"},
        {"name": "HighRisk", "uri": BASE + "HighRisk"},
        {"name": "LowRisk", "uri": BASE + "LowRisk"},
    ],
    "properties": [
        {"name": "income", "uri": DATA + "income"},
        {"name": "country", "uri": DATA + "country"},
        {"name": "riskTier", "uri": DATA + "riskTier"},
    ],
}


def _cond(op, value=""):
    return {"op": op, "value": value}


def _table(**over):
    dt = {
        "name": "Risk",
        "target_class": "Customer",
        "input_columns": [{"property": "income"}, {"property": "country"}],
        "rows": [
            {"conditions": [_cond("gt", "1000"), _cond("any")], "action_value": "high"},
            {"conditions": [_cond("any"), _cond("eq", "FR")], "action_value": "low"},
        ],
        "row_logic": "or",
        "hit_policy": "first",
        "output_column": {"property": "riskTier", "action": "set_value", "value": ""},
    }
    dt.update(over)
    return dt


class _Store:
    """Row 1 (income > 1000) matches a and b; row 2 (country = fr) matches b."""

    def __init__(self):
        self.queries = []

    def sql_table_reference(self, name):
        return name

    def execute_query(self, query):
        self.queries.append(query)
        if "> 1000" in query:
            return [{"s": "urn:a"}, {"s": "urn:b"}]
        if "'fr'" in query:
            return [{"s": "urn:b"}]
        return []


def _run(dt):
    return DecisionTableEngine().execute_tables([dt], _Store(), "triples", ONTOLOGY, materialize=True)


def _sql(dt):
    engine = DecisionTableEngine()
    resolved = engine._resolve_dt(dt, engine._build_uri_map(ONTOLOGY), BASE)
    return engine.build_violation_sql(resolved, "triples", BASE)


class TestOptionalColumns:
    def test_columns_are_left_joined(self):
        sql = _sql(_table())
        assert "LEFT JOIN triples inp0" in sql
        assert "LEFT JOIN triples inp1" in sql
        assert "INNER JOIN" not in sql

    def test_column_without_active_condition_is_not_joined(self):
        dt = _table(rows=[{"conditions": [_cond("gt", "1000"), _cond("any")]}])
        sql = _sql(dt)
        assert "inp0" in sql
        assert "inp1" not in sql

    def test_condition_on_column_without_property_is_skipped(self):
        dt = _table(
            input_columns=[{"property": "income"}, {"property": ""}],
            rows=[{"conditions": [_cond("gt", "1000"), _cond("eq", "FR")]}],
        )
        sql = _sql(dt)
        assert "inp1" not in sql
        assert "> 1000" in sql


class TestHitPolicy:
    def _subjects(self, result):
        return sorted((t.subject, t.object) for t in result.inferred_triples)

    def test_first_keeps_first_matching_row(self):
        res = _run(_table(hit_policy="first"))
        assert self._subjects(res) == [("urn:a", "high"), ("urn:b", "high")]

    def test_all_fires_every_matching_row(self):
        res = _run(_table(hit_policy="all"))
        assert self._subjects(res) == [("urn:a", "high"), ("urn:b", "high"), ("urn:b", "low")]

    def test_unique_reports_conflicts_without_output(self):
        res = _run(_table(hit_policy="unique"))
        assert self._subjects(res) == [("urn:a", "high")]
        conflicts = [v for v in res.violations if v.subject == "urn:b"]
        assert len(conflicts) == 1
        assert "rows 1, 2" in conflicts[0].message
        assert "Unique" in conflicts[0].message


class TestAssignEntity:
    def test_assign_class_types_matching_instances(self):
        dt = _table(output_column={"property": "", "action": "assign_class", "value": "HighRisk"})
        res = _run(dt)
        assert {(t.subject, t.predicate, t.object) for t in res.inferred_triples} == {
            ("urn:a", RDF_TYPE, BASE + "HighRisk"),
            ("urn:b", RDF_TYPE, BASE + "HighRisk"),
        }
        assert all("HighRisk" in v.message for v in res.violations)

    def test_assign_class_per_row_values(self):
        dt = _table(
            hit_policy="all",
            output_column={"property": "", "action": "assign_class", "value": ""},
            rows=[
                {"conditions": [_cond("gt", "1000"), _cond("any")], "action_value": "HighRisk"},
                {"conditions": [_cond("any"), _cond("eq", "FR")], "action_value": "LowRisk"},
            ],
        )
        res = _run(dt)
        assert ("urn:b", BASE + "LowRisk") in {(t.subject, t.object) for t in res.inferred_triples}

    def test_validate_requires_class_for_assign(self):
        dt = _table(output_column={"property": "", "action": "assign_class", "value": ""})
        dt["rows"] = [{"conditions": [_cond("gt", "1")]}]
        dt["input_columns"] = [{"property": "income"}]
        errors = DecisionTableEngine.validate_table(dt)
        assert any("entity to assign" in e for e in errors)

    def test_reference_errors_flag_unknown_assigned_class(self):
        dt = _table(output_column={"property": "", "action": "assign_class", "value": "Ghost"})
        errors = OntologyRules.decision_table_reference_errors(
            dt, {"customer", "highrisk"}, {"income", "country", "risktier"}
        )
        assert any("Ghost" in e for e in errors)
