"""A removal rule keeps its ``kind`` through export, import and GitHub sync."""

from back.objects.ontology.RulesPackage import apply_package, build_package
from back.objects.registry.gitsync.DomainRepoLayout import DomainRepoLayout

REMOVAL = {
    "name": "drop_meters",
    "kind": "removal",
    "enabled": True,
    "description": "hide meters of customer 42",
    "query": (
        "PREFIX : <http://ex.org/onto#> "
        "DELETE WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }"
    ),
}
BAD = {
    "name": "bad",
    "kind": "removal",
    "query": "PREFIX : <http://ex.org/onto#> INSERT DATA { :a :b :c }",
}


def test_export_keeps_kind():
    doc = build_package("demo", [], [REMOVAL])
    assert doc["sparql_rules"][0]["kind"] == "removal"


def test_import_keeps_kind_and_adds_the_rule():
    doc = build_package("demo", [], [REMOVAL])
    result = apply_package(doc, [], [])
    assert result.added == ["drop_meters"]
    assert result.sparql_rules[0]["kind"] == "removal"


def test_import_rejects_an_insert_removal():
    doc = build_package("demo", [], [BAD])
    result = apply_package(doc, [], [])
    assert result.added == []
    assert [r["name"] for r in result.rejected] == ["bad"]


def test_import_replaces_a_rule_and_keeps_its_kind():
    existing = [{"name": "drop_meters", "kind": "construct", "query": "x"}]
    doc = build_package("demo", [], [REMOVAL])
    result = apply_package(doc, [], existing)
    assert result.replaced == ["drop_meters"]
    assert result.sparql_rules[0]["kind"] == "removal"


def test_git_sync_split_and_merge_keep_kind():
    doc = {
        "info": {"name": "demo"},
        "ontology": {"sparql_rules": [REMOVAL], "if_then_rules": []},
    }
    files = DomainRepoLayout.split_source(doc, "1")
    assert "ontology/rules/sparql.json" in files

    merged = DomainRepoLayout.merge_version(files, "1")
    rules = merged["versions"]["1"]["ontology"]["sparql_rules"]
    assert rules == [REMOVAL]
