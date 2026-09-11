"""Route contracts for the Pillar 1 integrity endpoints.

The e2e suite only reaches these with an empty session, so the interesting
cases — a session that actually has mappings — are covered here by patching the
service layer the routers delegate to.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from shared.fastapi.main import app


@pytest.fixture
def client():
    return TestClient(app)


class TestRemovalImpactEndpoint:
    def test_empty_session_reports_no_impact(self, client):
        response = client.post(
            "/domain/metadata/removal-impact",
            json={"table_names": ["cat.sch.customers"]},
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["impact"] == {}
        assert payload["affected_table_count"] == 0
        assert payload["affected_mapping_count"] == 0

    def test_referenced_tables_are_returned_with_their_referrers(self, client):
        impact = {"cat.sch.customers": ["Entity: Customer", "Rel: buys (source)"]}
        with patch(
            "back.objects.mapping.Mapping.Mapping.find_mappings_referencing",
            return_value=impact,
        ):
            response = client.post(
                "/domain/metadata/removal-impact",
                json={"table_names": ["cat.sch.customers"]},
            )
        assert response.status_code == 200
        payload = response.json()
        assert payload["impact"] == impact
        assert payload["affected_table_count"] == 1
        assert payload["affected_mapping_count"] == 2

    def test_mapping_count_deduplicates_across_tables(self, client):
        """One entity reading two doomed tables is one affected mapping."""
        impact = {
            "cat.sch.a": ["Entity: Customer"],
            "cat.sch.b": ["Entity: Customer"],
        }
        with patch(
            "back.objects.mapping.Mapping.Mapping.find_mappings_referencing",
            return_value=impact,
        ):
            response = client.post(
                "/domain/metadata/removal-impact",
                json={"table_names": ["cat.sch.a", "cat.sch.b"]},
            )
        payload = response.json()
        assert payload["affected_table_count"] == 2
        assert payload["affected_mapping_count"] == 1

    def test_missing_table_names_defaults_to_empty(self, client):
        response = client.post("/domain/metadata/removal-impact", json={})
        assert response.status_code == 200
        assert response.json()["impact"] == {}

    def test_endpoint_does_not_mutate_anything(self, client):
        """The guard is a pre-flight — the removal itself goes elsewhere."""
        with patch(
            "back.objects.domain.Domain.Domain.save_metadata_tables"
        ) as save, patch("back.objects.domain.Domain.Domain.clear_metadata") as clear:
            client.post(
                "/domain/metadata/removal-impact",
                json={"table_names": ["cat.sch.customers"]},
            )
        save.assert_not_called()
        clear.assert_not_called()


class TestSchemaDriftEndpoint:
    def test_returns_the_expected_shape(self, client):
        response = client.get("/mapping/schema-drift")
        assert response.status_code == 200
        payload = response.json()
        assert payload["success"] is True
        assert payload["entities"] == {}
        assert payload["relationships"] == {}
        assert payload["tables_checked"] == 0

    def test_drift_is_passed_through_to_the_client(self, client):
        drift = {
            "success": True,
            "entities": {
                "http://t/Customer": {
                    "label": "Customer",
                    "columns": ["email_addr"],
                    "checks": [
                        {
                            "check": "schema_drift:attribute:email",
                            "status": "warning",
                            "detail": "Column 'email_addr' no longer exists",
                        }
                    ],
                }
            },
            "relationships": {},
            "tables_checked": 1,
        }
        with patch(
            "back.objects.mapping.Mapping.Mapping.get_schema_drift",
            return_value=drift,
        ):
            response = client.get("/mapping/schema-drift")
        assert response.status_code == 200
        payload = response.json()
        assert payload["entities"]["http://t/Customer"]["columns"] == ["email_addr"]
        assert payload["tables_checked"] == 1

    def test_is_a_read_only_get(self, client):
        assert client.post("/mapping/schema-drift").status_code in (404, 405)

    def test_does_not_run_the_full_diagnostics(self, client):
        """The whole point of the separate route is skipping SELECT probes."""
        with patch(
            "back.objects.mapping.Mapping.Mapping.run_diagnostics"
        ) as diagnostics:
            client.get("/mapping/schema-drift")
        diagnostics.assert_not_called()
