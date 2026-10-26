import asyncio
from types import SimpleNamespace

from back.core.databricks.request_identity import (
    RequestIdentity,
    set_request_identity,
    reset_request_identity,
)


def test_execute_spark_query_uses_data_plane_client(monkeypatch):
    from back.objects.digitaltwin.DigitalTwin import DigitalTwin

    captured = {}

    class _Client:
        host = "https://x"
        warehouse_id = "wh"

        def has_valid_auth(self):
            return True

        def execute_query(self, sql):
            captured["sql"] = sql
            return []

    def _fake_data_plane(domain, settings):
        captured["used_data_plane"] = True
        return _Client()

    # execute_spark_query does `from back.core.helpers import get_data_plane_client`
    # at call time, so patch the source module attribute.
    monkeypatch.setattr(
        "back.core.helpers.get_data_plane_client", _fake_data_plane, raising=False
    )

    domain = SimpleNamespace(ontology={"base_uri": "http://ex/"}, assignment={})
    dt = DigitalTwin(domain)

    monkeypatch.setattr(
        "back.core.w3c.sparql.extract_r2rml_mappings", lambda c: ({"E": {}}, {})
    )
    monkeypatch.setattr(
        DigitalTwin,
        "augment_mappings_from_config",
        staticmethod(lambda *a, **k: {"E": {}}),
    )
    monkeypatch.setattr(
        DigitalTwin,
        "augment_relationships_from_config",
        staticmethod(lambda *a, **k: {}),
    )
    monkeypatch.setattr(
        "back.core.w3c.sparql.translate_sparql_to_spark",
        lambda *a, **k: {"success": True, "sql": "SELECT 1", "variables": ["s"]},
    )

    tok = set_request_identity(
        RequestIdentity(email="u", user_token="UT", domain_role="viewer")
    )
    try:
        asyncio.run(
            dt.execute_spark_query(
                "SELECT * WHERE {?s ?p ?o}", "r2rml", 10, SimpleNamespace()
            )
        )
    finally:
        reset_request_identity(tok)

    assert captured["used_data_plane"] is True
    assert captured["sql"] == "SELECT 1"
