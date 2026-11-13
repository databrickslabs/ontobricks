"""Domain.info.knowledge_pages selection round-trip."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from back.objects.domain.Domain import Domain
from back.objects.session.DomainSession import get_empty_domain


@pytest.mark.unit
class TestKnowledgePagesSelection:
    def _domain(self):
        data = get_empty_domain()
        session = MagicMock()
        # Domain wraps DomainSession-like object with .info / .save
        ds = SimpleNamespace(
            info=data["domain"]["info"],
            ontology=data["ontology"],
            current_version="1",
            registry=data["settings"]["registry"],
            domain_folder="",
            design_layout=data["design_layout"],
            assignment=data["assignment"],
            triplestore=data["domain"]["triplestore"],
            save=MagicMock(),
            get_classes=lambda: [],
            get_entity_mappings=lambda: [],
            get_properties=lambda: [],
            set_r2rml=MagicMock(),
        )
        return Domain(ds)

    def test_empty_default(self):
        d = self._domain()
        assert d.get_knowledge_pages_selection() == []
        assert "knowledge_pages" in get_empty_domain()["domain"]["info"]
        assert get_empty_domain()["domain"]["info"]["knowledge_pages"] == []

    def test_save_and_get_round_trip(self):
        d = self._domain()
        pages = [
            {
                "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
                "domain": "domains/3ec7f320-1464-4630-a606-409e07d2e778",
                "display_name": "On-Time Delivery",
                "state": "PUBLISHED",
                "description": "Share of shipments",
                "synonyms": ["OTD"],
                "update_time": "2026-10-27T05:17:02.529Z",
                "body": "ignored",
            }
        ]
        saved = d.save_knowledge_pages_selection(pages)
        assert len(saved) == 1
        assert "body" not in saved[0]
        assert d.get_knowledge_pages_selection()[0]["display_name"] == "On-Time Delivery"
        d._s.save.assert_called()

    def test_replace_semantics(self):
        d = self._domain()
        d.save_knowledge_pages_selection(
            [
                {
                    "name": "pages/004f614e-c6bc-4c93-b12d-f95c7569e173",
                    "display_name": "A",
                }
            ]
        )
        d.save_knowledge_pages_selection([])
        assert d.get_knowledge_pages_selection() == []
