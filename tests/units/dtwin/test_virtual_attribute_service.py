"""Virtual attribute declaration reading and on-demand computation."""

from importlib import import_module
from unittest.mock import MagicMock

import pytest

from back.core.errors import InfrastructureError, ValidationError
from back.objects.digitaltwin.VirtualAttributeService import (
    SCALAR_RESULT_COLUMN,
    VirtualAttributeService,
)

# The package re-exports the class under the module's own name, so a plain
# import would hand us the class instead of the module to patch.
va_module = import_module("back.objects.digitaltwin.VirtualAttributeService")


pytestmark = pytest.mark.unit


_TABLE_GROUP = {
    "catalog": "main",
    "schema": "kg",
    "function": "customer_risk",
    "fullName": "main.kg.customer_risk",
    "description": "Live credit risk",
    "returns_table": True,
    "attributes": [
        {"name": "risk_score", "column": "risk_score", "dataType": "DOUBLE"},
        {"name": "risk_band", "column": "risk_band", "label": "Band"},
    ],
}

_SCALAR_GROUP = {
    "fullName": "main.kg.customer_churn",
    "returns_table": False,
    "attributes": [{"name": "churn_probability"}],
}


def _class(*groups):
    return {"name": "Customer", "virtualAttributes": list(groups)}


def _client(rows):
    client = MagicMock()
    client.execute_query.return_value = rows
    return client


# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


class TestClassEntries:
    def test_table_group_keeps_every_column(self):
        entries = VirtualAttributeService.class_entries(_class(_TABLE_GROUP))

        assert len(entries) == 1
        assert entries[0]["function"] == "customer_risk"
        assert [a["name"] for a in entries[0]["attributes"]] == [
            "risk_score",
            "risk_band",
        ]
        # A missing label falls back to the attribute name, never to empty.
        assert entries[0]["attributes"][0]["label"] == "risk_score"
        assert entries[0]["attributes"][1]["label"] == "Band"

    def test_scalar_group_reads_the_reserved_alias(self):
        entries = VirtualAttributeService.class_entries(_class(_SCALAR_GROUP))

        assert entries[0]["attributes"][0]["column"] == SCALAR_RESULT_COLUMN

    def test_malformed_function_name_is_dropped_not_raised(self):
        entries = VirtualAttributeService.class_entries(
            _class({"fullName": "main.kg.bad;DROP TABLE x", "attributes": [{"name": "x"}]}, _TABLE_GROUP)
        )

        assert [e["fullName"] for e in entries] == ["main.kg.customer_risk"]

    def test_group_without_usable_attribute_is_dropped(self):
        entries = VirtualAttributeService.class_entries(
            _class(
                {
                    "fullName": "main.kg.empty",
                    "returns_table": True,
                    "attributes": [{"name": "ok", "column": "bad column"}],
                }
            )
        )

        assert entries == []

    def test_duplicate_attribute_name_within_a_group_is_dropped(self):
        entries = VirtualAttributeService.class_entries(
            _class(
                {
                    "fullName": "main.kg.dup",
                    "returns_table": True,
                    "attributes": [
                        {"name": "score", "column": "a"},
                        {"name": "score", "column": "b"},
                    ],
                }
            )
        )

        assert [a["column"] for a in entries[0]["attributes"]] == ["a"]

    def test_class_without_declarations_yields_nothing(self):
        assert VirtualAttributeService.class_entries({"name": "Customer"}) == []


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


class TestCompute:
    @pytest.fixture
    def patched_client(self, monkeypatch):
        """Return a factory installing a fake Databricks client."""

        def install(rows):
            client = _client(rows)
            monkeypatch.setattr(
                va_module,
                "get_databricks_client",
                lambda *_a, **_kw: client,
            )
            return client

        return install

    async def test_maps_first_row_onto_attribute_names(self, patched_client):
        client = patched_client([{"risk_score": 0.82, "risk_band": "B"}])

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_TABLE_GROUP),
        )

        assert groups[0]["values"] == {"risk_score": 0.82, "risk_band": "B"}
        assert "CUST001" in client.execute_query.call_args.args[0]

    async def test_scalar_function_is_aliased_in_sql(self, patched_client):
        client = patched_client([{SCALAR_RESULT_COLUMN: 0.31}])

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_SCALAR_GROUP),
        )

        sql = client.execute_query.call_args.args[0]
        assert f"AS {SCALAR_RESULT_COLUMN}" in sql
        assert groups[0]["values"] == {"churn_probability": 0.31}

    async def test_single_quote_in_the_id_is_escaped(self, patched_client):
        client = patched_client([{"risk_score": 1.0, "risk_band": "A"}])

        await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/O'Neil",
            matched_cls=_class(_TABLE_GROUP),
        )

        assert "'O''Neil'" in client.execute_query.call_args.args[0]

    async def test_function_filter_restricts_to_one_group(self, patched_client):
        patched_client([{SCALAR_RESULT_COLUMN: 0.31}])

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_TABLE_GROUP, _SCALAR_GROUP),
            function_full_name="main.kg.customer_churn",
        )

        assert [g["fullName"] for g in groups] == ["main.kg.customer_churn"]

    async def test_undeclared_function_is_refused_before_any_query(
        self, patched_client
    ):
        client = patched_client([])

        with pytest.raises(ValidationError):
            await VirtualAttributeService.compute(
                MagicMock(),
                MagicMock(),
                entity_uri="https://example.com/Customer/CUST001",
                matched_cls=_class(_TABLE_GROUP),
                function_full_name="main.kg.salary_lookup",
            )

        client.execute_query.assert_not_called()

    async def test_failing_group_does_not_deny_the_healthy_one(self, monkeypatch):
        client = MagicMock()
        client.execute_query.side_effect = [
            RuntimeError("PERMISSION_DENIED"),
            [{SCALAR_RESULT_COLUMN: 0.31}],
        ]
        monkeypatch.setattr(
            va_module,
            "get_databricks_client",
            lambda *_a, **_kw: client,
        )

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_TABLE_GROUP, _SCALAR_GROUP),
        )

        assert groups[0]["values"] == {}
        assert "PERMISSION_DENIED" in groups[0]["error"]
        assert groups[1]["values"] == {"churn_probability": 0.31}

    async def test_no_row_reports_a_message_rather_than_null_values(
        self, patched_client
    ):
        patched_client([])

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_TABLE_GROUP),
        )

        assert groups[0]["values"] == {}
        assert "No row" in groups[0]["message"]

    async def test_extra_rows_are_flagged_not_aggregated(self, patched_client):
        patched_client(
            [
                {"risk_score": 0.82, "risk_band": "B"},
                {"risk_score": 0.10, "risk_band": "A"},
            ]
        )

        groups = await VirtualAttributeService.compute(
            MagicMock(),
            MagicMock(),
            entity_uri="https://example.com/Customer/CUST001",
            matched_cls=_class(_TABLE_GROUP),
        )

        assert groups[0]["values"]["risk_score"] == 0.82
        assert "2 rows" in groups[0]["message"]

    async def test_missing_databricks_client_raises(self, monkeypatch):
        monkeypatch.setattr(
            va_module,
            "get_databricks_client",
            lambda *_a, **_kw: None,
        )

        with pytest.raises(InfrastructureError):
            await VirtualAttributeService.compute(
                MagicMock(),
                MagicMock(),
                entity_uri="https://example.com/Customer/CUST001",
                matched_cls=_class(_TABLE_GROUP),
            )

    async def test_class_without_declarations_skips_the_client_entirely(
        self, monkeypatch
    ):
        def fail(*_a, **_kw):
            raise AssertionError("must not resolve a client with nothing to compute")

        monkeypatch.setattr(
            va_module,
            "get_databricks_client",
            fail,
        )

        assert (
            await VirtualAttributeService.compute(
                MagicMock(),
                MagicMock(),
                entity_uri="https://example.com/Customer/CUST001",
                matched_cls={"name": "Customer"},
            )
            == []
        )
