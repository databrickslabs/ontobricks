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

    def test_cert_data_is_named_off_the_gateway_view(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        assert _table_naming.cert_data_fqn(domain) == view + "_cert_data"

    def test_cert_sync_source_is_named_off_the_gateway_view(self):
        domain = _domain()
        view = _table_naming.view_fqn(domain)
        assert _table_naming.cert_sync_source_fqn(domain) == view + "_cert_sync"

    def test_certified_suffixes_strip_back_to_the_gateway_leaf(self):
        suffixes = (
            "_cert_base",
            "_cert_enriched",
            "_certified",
            "_cert_data",
            "_cert_sync",
        )
        for suffix in suffixes:
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


class TestMaterializedCertifiedBase:
    def test_copies_the_base_then_points_the_union_at_the_copy(self):
        client = MagicMock()

        materialize.materialize_certified_base(
            client,
            base_fqn="c.s.v_cert_base",
            cert_data_fqn="c.s.v_cert_data",
            enriched_fqn="c.s.v_cert_enriched",
            certified_fqn="c.s.v_certified",
        )

        statements = _statements(client)
        ctas = next(s for s in statements if "CREATE OR REPLACE TABLE" in s)
        assert "c.s.v_cert_data" in ctas
        assert "FROM c.s.v_cert_base" in ctas
        union = statements[-1]
        assert "CREATE OR REPLACE VIEW c.s.v_certified" in union
        assert "FROM c.s.v_cert_data" in union
        assert "FROM c.s.v_cert_enriched" in union

    def test_never_clears_the_enriched_table(self):
        client = MagicMock()

        materialize.materialize_certified_base(
            client,
            base_fqn="c.s.v_cert_base",
            cert_data_fqn="c.s.v_cert_data",
            enriched_fqn="c.s.v_cert_enriched",
            certified_fqn="c.s.v_certified",
        )

        for statement in _statements(client):
            if "c.s.v_cert_enriched" in statement:
                upper = statement.upper()
                assert "TRUNCATE" not in upper and "DELETE" not in upper
                assert "DROP" not in upper


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
        assert builder.cert_data_fqn() == view + "_cert_data"
        assert builder.cert_sync_source_fqn() == view + "_cert_sync"

    def test_builder_materializes_the_domain_certified_base(self):
        client = MagicMock()
        domain = _domain()
        builder = GraphDBFactory.get_builder(domain, None, engine="delta", client=client)
        view = _table_naming.view_fqn(domain)

        builder.materialize_certified_base()

        statements = _statements(client)
        assert any(f"{view}_cert_data" in s and "CREATE OR REPLACE TABLE" in s for s in statements)
        assert f"FROM {view}_cert_data" in statements[-1]


class TestCertEnrichedWrites:
    def test_insert_sql_carries_kind_provenance_and_run(self):
        sql = materialize.build_insert_cert_enriched_sql(
            "c.s.v_cert_enriched",
            [{"subject": "s", "predicate": "p", "object": "o", "provenance": "owlrl"}],
            kind="inference",
            run_id="t-1",
        )

        assert sql.startswith("INSERT INTO c.s.v_cert_enriched")
        assert "'s', 'p', 'o', 'inference', 'owlrl', 't-1'" in sql

    def test_insert_sql_escapes_quotes(self):
        sql = materialize.build_insert_cert_enriched_sql(
            "c.s.v_cert_enriched",
            [{"subject": "s", "predicate": "p", "object": "O'Brien"}],
            kind="rule",
            run_id="",
        )

        assert "'O''Brien'" in sql

    def test_insert_runs_one_statement_per_batch(self):
        client = MagicMock()
        rows = [{"subject": f"s{i}", "predicate": "p", "object": "o"} for i in range(5)]

        count = materialize.insert_cert_enriched(
            client, "c.s.v_cert_enriched", rows, kind="inference", run_id="t", batch_size=2
        )

        assert count == 5
        assert client.execute_statement.call_count == 3

    def test_ensure_objects_never_replaces_an_existing_view(self):
        client = MagicMock()
        domain = _domain()
        builder = GraphDBFactory.get_builder(domain, None, engine="delta", client=client)
        view = _table_naming.view_fqn(domain)

        builder.ensure_certified_objects()

        base, enriched, union = _statements(client)
        assert base.startswith(f"CREATE VIEW IF NOT EXISTS {view}_cert_base")
        assert enriched.startswith("CREATE TABLE IF NOT EXISTS")
        assert union.startswith(f"CREATE VIEW IF NOT EXISTS {view}_certified")
        assert "REPLACE" not in " ".join(_statements(client)).upper()

    def test_delete_scope_targets_kind_and_provenance(self):
        sql = materialize.build_delete_cert_enriched_scope_sql(
            "c.s.v_cert_enriched", kind="cohort", provenance="cohort:r'1"
        )

        assert sql == (
            "DELETE FROM c.s.v_cert_enriched "
            "WHERE kind = 'cohort' AND provenance = 'cohort:r''1'"
        )

    def test_scope_select_reads_the_triples_of_one_scope(self):
        sql = materialize.build_select_cert_enriched_scope_sql(
            "c.s.v_cert_enriched", kind="community", provenance="community"
        )

        assert sql == (
            "SELECT subject, predicate, object FROM c.s.v_cert_enriched "
            "WHERE kind = 'community' AND provenance = 'community'"
        )

    def test_insert_from_select_tags_the_scope(self):
        client = MagicMock()

        materialize.insert_cert_enriched_from_select(
            client,
            "c.s.v_cert_enriched",
            "SELECT 's' AS subject, 'p' AS predicate, 'o' AS object",
            kind="community",
            provenance="community",
            run_id="t-9",
        )

        (sql,) = _statements(client)
        assert sql.startswith("INSERT INTO c.s.v_cert_enriched")
        assert "'community', 'community', 't-9', current_timestamp()" in sql
        assert "FROM (SELECT 's' AS subject" in sql
