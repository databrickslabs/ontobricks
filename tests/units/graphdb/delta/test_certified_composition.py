"""Delta objects of the Certified Graph composition (base view, enriched table, union view)."""

from unittest.mock import MagicMock

from back.core.graphdb.delta import _table_naming, materialize
from back.core.graphdb.GraphDBFactory import GraphDBFactory


def _domain(name="MyDomain", version=1, catalog="cat", schema="sch"):
    d = MagicMock()
    d.info = {"name": name}
    d.current_version = version
    d.delta = {"catalog": catalog, "schema": schema}
    return d


def _statements(client) -> list:
    return [c.args[0] for c in client.execute_statement.call_args_list]


class TestCertifiedNaming:
    def test_cert_base_is_named_off_the_gateway_view(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        assert _table_naming.cert_base_fqn(domain) == view + "_cert_base"

    def test_cert_enriched_is_named_off_the_gateway_view(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        assert _table_naming.cert_enriched_fqn(domain) == view + "_cert_enriched"

    def test_certified_view_is_named_off_the_gateway_view(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        assert _table_naming.certified_view_fqn(domain) == view + "_certified"

    def test_certified_suffixes_strip_back_to_the_gateway_leaf(self):
        for suffix in ("_cert_base", "_cert_enriched", "_certified"):
            assert _table_naming.strip_graph_leaf_suffix("g_V1" + suffix) == "g_V1"


class TestCertifiedCompositionSql:
    def test_enriched_table_carries_kind_and_provenance(self):
        sql = materialize.build_ensure_cert_enriched_sql("c.s.v_cert_enriched")
        assert "CREATE TABLE IF NOT EXISTS c.s.v_cert_enriched" in sql
        for column in ("kind STRING", "provenance STRING", "run_id STRING"):
            assert column in sql
        assert "created_at TIMESTAMP" in sql

    def test_composition_creates_base_enriched_then_union(self):
        client = MagicMock()

        materialize.ensure_certified_composition(
            client,
            data_fqn="c.s.v_data",
            base_fqn="c.s.v_cert_base",
            enriched_fqn="c.s.v_cert_enriched",
            certified_fqn="c.s.v_certified",
        )

        base, enriched, union = _statements(client)
        assert "CREATE OR REPLACE VIEW c.s.v_cert_base" in base
        assert "FROM c.s.v_data" in base
        assert "CREATE TABLE IF NOT EXISTS c.s.v_cert_enriched" in enriched
        assert "CREATE OR REPLACE VIEW c.s.v_certified" in union
        assert "FROM c.s.v_cert_base" in union
        assert "FROM c.s.v_cert_enriched" in union

    def test_composition_never_clears_the_enriched_table(self):
        client = MagicMock()

        materialize.ensure_certified_composition(
            client,
            data_fqn="c.s.v_data",
            base_fqn="c.s.v_cert_base",
            enriched_fqn="c.s.v_cert_enriched",
            certified_fqn="c.s.v_certified",
        )

        joined = " ".join(_statements(client)).upper()
        assert "TRUNCATE" not in joined
        assert "DELETE" not in joined
        assert "DROP" not in joined


class TestBuilderCertifiedComposition:
    def test_builder_composes_from_the_domain_data_relation(self):
        client = MagicMock()
        domain = _domain()
        builder = GraphDBFactory.get_builder(domain, None, engine="delta", client=client)
        view = _table_naming.view_fqn(domain)

        builder.ensure_certified_composition()

        base, _enriched, union = _statements(client)
        assert f"CREATE OR REPLACE VIEW {view}_cert_base" in base
        assert f"FROM {view}_data" in base
        assert f"CREATE OR REPLACE VIEW {view}_certified" in union

    def test_builder_exposes_the_certified_relation_names(self):
        domain = _domain()
        builder = GraphDBFactory.get_builder(
            domain, None, engine="delta", client=MagicMock()
        )
        view = _table_naming.view_fqn(domain)

        assert builder.cert_base_fqn() == view + "_cert_base"
        assert builder.cert_enriched_fqn() == view + "_cert_enriched"
        assert builder.certified_view_fqn() == view + "_certified"
