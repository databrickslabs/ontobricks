"""Routes for the rules package: GET /ontology/rules/export, POST /ontology/rules/import."""

import json

import pytest
from fastapi.testclient import TestClient

from shared.fastapi.main import app
from tests.units.api.test_ui_rendering import ensure_loaded_domain

SIMPLE = {
    "name": "PkgSimpleRule",
    "description": "d",
    "antecedent": "Customer(?c)",
    "consequent": "Payment(?c)",
}
COMPLEX = {
    "name": "PkgComplexRule",
    "description": "d",
    "query": "CONSTRUCT { ?c <http://x/hasclaim> ?k } WHERE { ?c a <http://x/Customer> . ?c <http://x/hasclaim> ?k }",
    "enabled": True,
}


@pytest.fixture
def client_with_rules():
    client = TestClient(app)
    ensure_loaded_domain(client)
    saved = client.post("/ontology/if-then/save", json={"rule": SIMPLE, "index": -1})
    assert saved.status_code == 200, saved.text
    saved = client.post("/ontology/rules/sparql_rules/save", json={"rule": COMPLEX, "index": -1})
    assert saved.status_code == 200, saved.text
    return client


def test_export_returns_attachment_with_documented_shape(client_with_rules):
    response = client_with_rules.get("/ontology/rules/export")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    assert 'filename="' in response.headers["content-disposition"]
    assert response.headers["content-disposition"].endswith('_rules.json"')
    body = json.loads(response.text)
    assert body["format"] == "ontobricks-rules"
    assert body["version"] == 1
    assert "if_then_rules" in body and "sparql_rules" in body


def test_import_round_trip_is_idempotent(client_with_rules):
    exported = json.loads(client_with_rules.get("/ontology/rules/export").text)
    response = client_with_rules.post("/ontology/rules/import", json=exported)
    assert response.status_code == 200
    body = response.json()
    assert body["success"] is True
    assert body["added"] == []
    assert body["rejected"] == []
    assert len(body["replaced"]) == len(exported["if_then_rules"]) + len(exported["sparql_rules"])


def test_import_rejects_wrong_format_without_saving(client_with_rules):
    before = client_with_rules.get("/ontology/if-then/list").json()["rules"]
    response = client_with_rules.post("/ontology/rules/import", json={"format": "nope", "version": 1})
    assert response.status_code in (400, 422)
    after = client_with_rules.get("/ontology/if-then/list").json()["rules"]
    assert before == after


def test_import_rejects_a_body_that_is_not_json(client_with_rules):
    response = client_with_rules.post(
        "/ontology/rules/import",
        content=b"not json at all",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code in (400, 422)


def test_import_reports_rejected_rules_and_keeps_the_rest(client_with_rules):
    payload = {
        "format": "ontobricks-rules",
        "version": 1,
        "if_then_rules": [
            {"name": "NoAntecedent", "antecedent": "", "consequent": "X(?c)"},
            {"name": "NewOk", "antecedent": "Customer(?c)", "consequent": "Payment(?c)"},
        ],
        "sparql_rules": [],
    }
    body = client_with_rules.post("/ontology/rules/import", json=payload).json()
    assert body["added"] == ["NewOk"]
    assert body["rejected"][0]["name"] == "NoAntecedent"
    names = [r["name"] for r in client_with_rules.get("/ontology/if-then/list").json()["rules"]]
    assert "NewOk" in names and "NoAntecedent" not in names


def test_export_includes_both_simple_and_complex_rules(client_with_rules):
    body = json.loads(client_with_rules.get("/ontology/rules/export").text)
    simple_names = [r["name"] for r in body["if_then_rules"]]
    complex_names = [r["name"] for r in body["sparql_rules"]]
    assert "PkgSimpleRule" in simple_names
    assert "PkgComplexRule" in complex_names
    assert body["domain"]


def test_import_adds_and_rejects_complex_rules(client_with_rules):
    payload = {
        "format": "ontobricks-rules",
        "version": 1,
        "if_then_rules": [],
        "sparql_rules": [
            {
                "name": "NewComplex",
                "query": COMPLEX["query"],
                "enabled": True,
            },
            {
                "name": "BrokenComplex",
                "query": "NOT A QUERY",
                "enabled": True,
            },
            {
                "name": "LegacyOff",
                "query": "NOT A QUERY",
                "enabled": False,
            },
        ],
    }
    body = client_with_rules.post("/ontology/rules/import", json=payload).json()
    assert "NewComplex" in body["added"]
    assert "LegacyOff" in body["added"]
    assert body["rejected"][0]["name"] == "BrokenComplex"
    assert body["rejected"][0]["type"] == "sparql_rules"
    names = [
        r["name"]
        for r in client_with_rules.get("/ontology/rules/sparql_rules/list").json()["rules"]
    ]
    assert "NewComplex" in names and "LegacyOff" in names and "BrokenComplex" not in names


def test_import_replaces_existing_simple_and_complex_by_name(client_with_rules):
    payload = {
        "format": "ontobricks-rules",
        "version": 1,
        "if_then_rules": [
            {**SIMPLE, "description": "replaced-simple"},
        ],
        "sparql_rules": [
            {**COMPLEX, "description": "replaced-complex"},
        ],
    }
    body = client_with_rules.post("/ontology/rules/import", json=payload).json()
    assert set(body["replaced"]) == {"PkgSimpleRule", "PkgComplexRule"}
    assert body["added"] == []
    assert body["rejected"] == []
    simple = client_with_rules.get("/ontology/if-then/list").json()["rules"]
    complex_rules = client_with_rules.get("/ontology/rules/sparql_rules/list").json()["rules"]
    assert next(r for r in simple if r["name"] == "PkgSimpleRule")["description"] == "replaced-simple"
    assert next(r for r in complex_rules if r["name"] == "PkgComplexRule")["description"] == "replaced-complex"


def test_if_then_save_rejects_incomplete_rule_with_shared_messages(client_with_rules):
    response = client_with_rules.post(
        "/ontology/if-then/save",
        json={"rule": {"name": "", "antecedent": "", "consequent": ""}, "index": -1},
    )
    assert response.status_code in (400, 422)
    message = response.json().get("message", "")
    assert "Rule name is required" in message
    assert "Rule antecedent is required" in message
    assert "Rule consequent is required" in message


def test_complex_save_rejects_enabled_bad_query_but_allows_disabled(client_with_rules):
    bad = {"name": "BadEnabled", "query": "NOT A QUERY", "enabled": True}
    response = client_with_rules.post(
        "/ontology/rules/sparql_rules/save", json={"rule": bad, "index": -1}
    )
    assert response.status_code in (400, 422)

    disabled = {"name": "BadDisabled", "query": "NOT A QUERY", "enabled": False}
    response = client_with_rules.post(
        "/ontology/rules/sparql_rules/save", json={"rule": disabled, "index": -1}
    )
    assert response.status_code == 200
    names = [
        r["name"]
        for r in client_with_rules.get("/ontology/rules/sparql_rules/list").json()["rules"]
    ]
    assert "BadDisabled" in names
