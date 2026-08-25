"""Virtual attributes inside the node-context resolution."""

from importlib import import_module
from unittest.mock import MagicMock

import pytest

from back.core.errors import NotFoundError, ValidationError
from back.objects.digitaltwin.NodeContextService import NodeContextService

pytestmark = pytest.mark.unit

va_module = import_module("back.objects.digitaltwin.VirtualAttributeService")

_ENTITY_URI = "https://example.com/Customer/CUST001"

_CLASSES = [
    {
        "name": "Customer",
        "uri": "https://example.com/Customer",
        "dataset": None,
        "bridges": [],
        "actions": [],
        "virtualAttributes": [
            {
                "fullName": "main.kg.customer_risk",
                "function": "customer_risk",
                "returns_table": True,
                "attributes": [
                    {"name": "risk_score", "column": "risk_score"},
                    {"name": "risk_band", "column": "risk_band"},
                ],
            }
        ],
    }
]


@pytest.fixture
def domain():
    mock_domain = MagicMock()
    mock_domain.info = {"name": "Customer 360"}
    mock_domain.get_classes.return_value = _CLASSES
    return mock_domain


@pytest.fixture
def uc_client(monkeypatch):
    fake = MagicMock()
    fake.execute_query.return_value = [{"risk_score": 0.82, "risk_band": "B"}]
    monkeypatch.setattr(va_module, "get_databricks_client", lambda *_a, **_kw: fake)
    return fake


class TestResolveContext:
    async def test_declarations_come_for_free_with_the_class_lookup(
        self, domain, uc_client
    ):
        out = await NodeContextService.resolve_context(
            domain, MagicMock(), entity_uri=_ENTITY_URI
        )

        groups = out["virtual_attributes"]
        assert groups[0]["fullName"] == "main.kg.customer_risk"
        assert "values" not in groups[0]
        uc_client.execute_query.assert_not_called()

    async def test_flag_computes_the_values(self, domain, uc_client):
        out = await NodeContextService.resolve_context(
            domain, MagicMock(), entity_uri=_ENTITY_URI, compute_virtual_attributes=True
        )

        assert out["virtual_attributes"][0]["values"] == {
            "risk_score": 0.82,
            "risk_band": "B",
        }

    async def test_disabled_element_is_withheld_and_the_flag_ignored(
        self, domain, uc_client
    ):
        out = await NodeContextService.resolve_context(
            domain,
            MagicMock(),
            entity_uri=_ENTITY_URI,
            compute_virtual_attributes=True,
            context_policy={"virtual_attributes": "disabled"},
        )

        # None is what the response model drops, same as a disabled dataset.
        assert out["virtual_attributes"] is None
        uc_client.execute_query.assert_not_called()

    async def test_class_without_declarations_carries_nothing(self, domain, uc_client):
        domain.get_classes.return_value = [
            {"name": "Customer", "uri": "https://example.com/Customer"}
        ]

        out = await NodeContextService.resolve_context(
            domain, MagicMock(), entity_uri=_ENTITY_URI
        )

        assert out["virtual_attributes"] is None


class TestComputeFacade:
    async def test_returns_the_values_with_the_resolved_class(self, domain, uc_client):
        out = await NodeContextService.compute_virtual_attributes(
            domain, MagicMock(), entity_uri=_ENTITY_URI
        )

        assert out["success"] is True
        assert out["class_name"] == "Customer"
        assert out["entity_local_id"] == "CUST001"
        assert out["virtual_attributes"][0]["values"]["risk_score"] == 0.82

    async def test_unmatched_entity_is_not_found(self, domain, uc_client):
        with pytest.raises(NotFoundError):
            await NodeContextService.compute_virtual_attributes(
                domain, MagicMock(), entity_uri="https://elsewhere.example/Widget/W1"
            )

        uc_client.execute_query.assert_not_called()

    async def test_disabled_element_refuses_instead_of_answering_empty(
        self, domain, uc_client
    ):
        """The caller learns the element is off; silently returning nothing
        would read as "this entity has none"."""
        with pytest.raises(ValidationError):
            await NodeContextService.compute_virtual_attributes(
                domain,
                MagicMock(),
                entity_uri=_ENTITY_URI,
                context_policy={"virtual_attributes": "disabled"},
            )

        uc_client.execute_query.assert_not_called()

    async def test_policy_check_precedes_the_class_lookup(self, domain, uc_client):
        """A disabled element must not leak whether the URI resolves."""
        with pytest.raises(ValidationError):
            await NodeContextService.compute_virtual_attributes(
                domain,
                MagicMock(),
                entity_uri="https://elsewhere.example/Widget/W1",
                context_policy={"virtual_attributes": "disabled"},
            )

    async def test_function_filter_is_forwarded(self, domain, uc_client):
        out = await NodeContextService.compute_virtual_attributes(
            domain,
            MagicMock(),
            entity_uri=_ENTITY_URI,
            function_full_name="main.kg.customer_risk",
        )

        assert len(out["virtual_attributes"]) == 1
        assert uc_client.execute_query.call_count == 1

    async def test_undeclared_function_is_refused(self, domain, uc_client):
        with pytest.raises(ValidationError):
            await NodeContextService.compute_virtual_attributes(
                domain,
                MagicMock(),
                entity_uri=_ENTITY_URI,
                function_full_name="main.kg.salary_lookup",
            )

        uc_client.execute_query.assert_not_called()
