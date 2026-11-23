"""Original vs Certified graph variants: which store and which relation a read uses."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ValidationError
from back.core.graphdb.constants import GRAPH_VARIANT_CERTIFIED, GRAPH_VARIANT_ORIGINAL
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.core.helpers.SQLHelpers import SQLHelpers
from back.objects.digitaltwin.TwinGraphAccess import TwinGraphAccess

_VIEW = "cat.sch.triplestore_dom_V2"


def _domain(backend: str):
    return SimpleNamespace(
        info={"name": "Dom", "graph_backend": backend},
        current_version="2",
        delta={"catalog": "cat", "schema": "sch"},
    )


class TestFactoryVariant:
    def test_original_always_opens_the_delta_store(self):
        factory = GraphDBFactory()
        with (
            patch.object(factory, "_create_delta", return_value="delta-store") as delta,
            patch.object(factory, "_create_auto") as auto,
        ):
            store = factory.create(_domain("lakebase"), None, variant="original")

        assert store == "delta-store"
        delta.assert_called_once()
        auto.assert_not_called()

    def test_certified_opens_the_context_backend(self):
        factory = GraphDBFactory()
        with (
            patch.object(factory, "_create_auto", return_value="lakebase-store") as auto,
            patch.object(factory, "_create_delta") as delta,
        ):
            store = factory.create(_domain("lakebase"), None, variant="certified")

        assert store == "lakebase-store"
        auto.assert_called_once()
        delta.assert_not_called()

    def test_an_unknown_variant_is_refused(self):
        with pytest.raises(ValidationError):
            GraphDBFactory().create(_domain("lakebase"), None, variant="live")


class TestQueryTableVariant:
    @pytest.mark.parametrize("backend", ["databricks", "lakebase", "neo4j"])
    def test_original_reads_the_data_relation_whatever_the_backend(self, backend):
        table = SQLHelpers.effective_graph_query_table(
            _domain(backend), variant=GRAPH_VARIANT_ORIGINAL
        )

        assert table == _VIEW + "_data"

    def test_certified_lakehouse_reads_the_certified_view(self):
        table = SQLHelpers.effective_graph_query_table(
            _domain("databricks"), variant=GRAPH_VARIANT_CERTIFIED
        )

        assert table == _VIEW + "_certified"

    @pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
    def test_certified_graph_engine_reads_the_versioned_graph(self, backend):
        table = SQLHelpers.effective_graph_query_table(
            _domain(backend), variant=GRAPH_VARIANT_CERTIFIED
        )

        assert table == "Dom_V2"

    def test_no_variant_keeps_the_current_lakehouse_union_view(self):
        table = SQLHelpers.effective_graph_query_table(_domain("databricks"))

        assert table == _VIEW + "_graph"

    def test_an_unknown_variant_is_refused(self):
        with pytest.raises(ValidationError):
            SQLHelpers.effective_graph_query_table(
                _domain("databricks"), variant="live"
            )


class TestTwinGraphAccessVariant:
    def test_the_variant_reaches_the_query_table(self):
        table = TwinGraphAccess.graph_query_table(
            _domain("databricks"), None, variant=GRAPH_VARIANT_CERTIFIED
        )

        assert table == _VIEW + "_certified"

    def test_a_simulation_still_wins_over_the_variant(self):
        from back.objects.intelligence import Simulation

        with patch.object(Simulation, "explorer_relation", return_value="sim_union"):
            table = TwinGraphAccess.graph_query_table(
                _domain("databricks"),
                None,
                simulation_id="s0123abcd",
                variant=GRAPH_VARIANT_CERTIFIED,
                store=MagicMock(),
            )

        assert table == "sim_union"


class TestServedQueryTable:
    """External reads target the Certified Graph, with a Lakehouse fallback (option A)."""

    def test_a_certified_lakehouse_serves_the_certified_view(self):
        store = MagicMock()
        store.table_exists.return_value = True

        table = TwinGraphAccess.served_query_table(_domain("databricks"), None, store)

        assert table == _VIEW + "_certified"

    def test_an_uncertified_lakehouse_falls_back_to_the_current_graph(self):
        store = MagicMock()
        store.table_exists.return_value = False

        table = TwinGraphAccess.served_query_table(_domain("databricks"), None, store)

        assert table == _VIEW + "_graph"

    def test_the_fallback_keeps_the_asserted_only_choice(self):
        store = MagicMock()
        store.table_exists.return_value = False

        table = TwinGraphAccess.served_query_table(
            _domain("databricks"), None, store, include_inferred=False
        )

        assert table == _VIEW + "_data"

    @pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
    def test_a_graph_engine_serves_the_versioned_graph(self, backend):
        store = MagicMock()

        table = TwinGraphAccess.served_query_table(_domain(backend), None, store)

        assert table == "Dom_V2"
        store.table_exists.assert_not_called()

    def test_a_simulation_is_still_served_from_its_sandbox(self):
        from back.objects.intelligence import Simulation

        with patch.object(Simulation, "explorer_relation", return_value="sim_union"):
            table = TwinGraphAccess.served_query_table(
                _domain("databricks"), None, MagicMock(), simulation_id="s0123abcd"
            )

        assert table == "sim_union"


class TestSourceTokens:
    """The graph-source picker selects the Original or the Certified Graph."""

    @pytest.mark.parametrize(
        "raw,variant",
        [
            ("original", GRAPH_VARIANT_ORIGINAL),
            (" Certified ", GRAPH_VARIANT_CERTIFIED),
            ("live", None),
            ("live:inferred", None),
            ("s0123abcd", None),
            (None, None),
        ],
    )
    def test_source_variant(self, raw, variant):
        assert TwinGraphAccess.source_variant(raw) == variant

    @pytest.mark.parametrize("raw", ["original", "certified"])
    def test_variant_tokens_are_not_sandboxes(self, raw):
        assert TwinGraphAccess.sandbox_id(raw) is None

    def test_an_original_token_reads_the_data_relation(self):
        table = TwinGraphAccess.graph_query_table(
            _domain("lakebase"), None, MagicMock(), simulation_id="original"
        )

        assert table == _VIEW + "_data"

    def test_a_certified_token_reads_the_certified_relation(self):
        table = TwinGraphAccess.graph_query_table(
            _domain("databricks"), None, MagicMock(), simulation_id="certified"
        )

        assert table == _VIEW + "_certified"

    def test_the_store_follows_the_source_token(self):
        get_store = MagicMock(return_value="delta-store")

        store = TwinGraphAccess.require_graph_store(
            _domain("lakebase"), None, get_store=get_store, source="original"
        )

        assert store == "delta-store"
        assert get_store.call_args.kwargs == {"variant": GRAPH_VARIANT_ORIGINAL}

    def test_no_token_keeps_the_default_store(self):
        get_store = MagicMock(return_value="store")

        TwinGraphAccess.require_graph_store(
            _domain("lakebase"), None, get_store=get_store, source="live"
        )

        assert get_store.call_args.kwargs == {}

    def test_variant_from_payload_reads_the_raw_source(self):
        assert (
            TwinGraphAccess.variant_from_payload({"simulation_id": "certified"})
            == GRAPH_VARIANT_CERTIFIED
        )
        assert TwinGraphAccess.variant_from_payload({"simulation_id": "s1"}) is None
