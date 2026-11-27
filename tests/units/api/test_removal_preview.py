"""POST /ontology/rules/sparql_rules/removal-preview: count before a removal runs."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from shared.config.constants import SPARQL_REMOVAL_CONFIRM_THRESHOLD
from shared.fastapi.main import app
from tests.units.api.test_ui_rendering import ensure_loaded_domain

URL = "/ontology/rules/sparql_rules/removal-preview"
PFX = "PREFIX : <http://ex.org/onto#>\n"
GOOD = PFX + "DELETE WHERE { ?m :meterOf :Customer_42 . ?m :status ?o }"
COUNT_PATH = "back.core.reasoning.ReasoningService.ReasoningService.count_sparql_removal"


@pytest.fixture
def client():
    client = TestClient(app)
    ensure_loaded_domain(client)
    return client


@pytest.fixture
def store(monkeypatch):
    fake = MagicMock()
    monkeypatch.setattr(
        "api.routers.internal.ontology.get_graphdb", lambda *_a, **_kw: fake
    )
    return fake


def test_threshold_is_ten_thousand():
    assert SPARQL_REMOVAL_CONFIRM_THRESHOLD == 10_000


def test_small_removal_reports_count_without_confirmation(client, store):
    with patch(COUNT_PATH, return_value=3) as count:
        resp = client.post(URL, json={"rule": {"name": "drop", "query": GOOD}})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body == {"success": True, "count": 3, "requires_confirmation": False, "errors": []}
    count.assert_called_once()


def test_count_above_threshold_requires_confirmation(client, store):
    over = SPARQL_REMOVAL_CONFIRM_THRESHOLD + 1
    with patch(COUNT_PATH, return_value=over):
        body = client.post(URL, json={"rule": {"name": "drop", "query": GOOD}}).json()
    assert body["count"] == over
    assert body["requires_confirmation"] is True


def test_count_at_threshold_does_not_require_confirmation(client, store):
    with patch(COUNT_PATH, return_value=SPARQL_REMOVAL_CONFIRM_THRESHOLD):
        body = client.post(URL, json={"rule": {"name": "drop", "query": GOOD}}).json()
    assert body["requires_confirmation"] is False


def test_invalid_rule_returns_errors_not_http_error(client, store):
    query = PFX + "INSERT DATA { :a :b :c }"
    resp = client.post(URL, json={"rule": {"name": "bad", "query": query}})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["count"] == 0
    assert body["errors"]


def test_rule_without_name_is_reported(client, store):
    resp = client.post(URL, json={"rule": {"query": GOOD}})
    assert resp.json()["success"] is False


def test_preview_refuses_a_construct_query(client, store):
    # A CONSTRUCT query sent to the removal endpoint is rejected, not run as a rule.
    construct = PFX + "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b }"
    body = client.post(URL, json={"rule": {"name": "vip", "query": construct}}).json()
    assert body["success"] is False
    assert body["errors"] == ["Only a DELETE rule has a removal preview"]
