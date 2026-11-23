"""CertifiedGraphWriter: the single write path into the Certified Graph."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import ConflictError
from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.certify import CertifiedBuild, CertifiedGraphWriter

pytestmark = pytest.mark.unit

_TRIPLES = [
    {"subject": "s1", "predicate": "p", "object": "o1", "provenance": "owlrl"},
    {"subject": "(batch)", "predicate": "p", "object": "o"},
    {"subject": "", "predicate": "p", "object": "o"},
]


def _domain():
    return SimpleNamespace(info={"name": "Dom"}, current_version="2")


def _write(backend, *, editable=True, triples=_TRIPLES):
    builder = MagicMock()
    store = MagicMock()
    with (
        patch.object(CertifiedBuild, "ensure_editable")
        if editable
        else patch.object(
            CertifiedBuild, "ensure_editable", side_effect=ConflictError("locked")
        ),
        patch.object(GraphDBFactory, "_resolve_graph_backend", return_value=backend),
        patch.object(GraphDBFactory, "get_builder", return_value=builder),
        patch.object(GraphDBFactory, "get_graphdb", return_value=store) as get_graphdb,
    ):
        count = CertifiedGraphWriter(_domain(), SimpleNamespace()).add_enrichments(
            triples, kind="inference", run_id="t-1"
        )
    return count, builder, store, get_graphdb


def test_a_lakehouse_write_only_touches_the_composition():
    count, builder, _store, get_graphdb = _write("databricks")

    assert count == 1
    builder.ensure_certified_objects.assert_called_once_with()
    rows = builder.insert_cert_enriched.call_args.args[0]
    assert rows == [
        {"subject": "s1", "predicate": "p", "object": "o1", "provenance": "owlrl"}
    ]
    assert builder.insert_cert_enriched.call_args.kwargs == {
        "kind": "inference",
        "run_id": "t-1",
    }
    get_graphdb.assert_not_called()


@pytest.mark.parametrize("backend", ["lakebase", "neo4j"])
def test_a_graph_engine_write_also_reaches_the_served_backend(backend):
    _count, builder, store, get_graphdb = _write(backend)

    builder.insert_cert_enriched.assert_called_once()
    assert get_graphdb.call_args.kwargs["variant"] == "certified"
    graph_name, triples = store.insert_triples.call_args.args
    assert graph_name == "Dom_V2"
    assert triples == [{"subject": "s1", "predicate": "p", "object": "o1"}]


def test_the_composition_views_are_not_recreated_by_a_write():
    _count, builder, _store, _get = _write("databricks")

    builder.ensure_certified_composition.assert_not_called()
    builder.materialize_certified_base.assert_not_called()


def test_a_read_only_version_is_refused_before_any_write():
    with pytest.raises(ConflictError):
        _write("lakebase", editable=False)


def test_nothing_to_write_writes_nothing():
    count, builder, store, _get = _write("lakebase", triples=[_TRIPLES[1]])

    assert count == 0
    builder.insert_cert_enriched.assert_not_called()
    store.insert_triples.assert_not_called()


def _scope(backend, call, *, old_rows=(), editable=True):
    builder = MagicMock()
    builder.iter_cert_enriched_scope.return_value = iter([list(old_rows)] if old_rows else [])
    builder.count_cert_enriched_scope.return_value = 7
    store = MagicMock()
    with (
        patch.object(CertifiedBuild, "ensure_editable")
        if editable
        else patch.object(
            CertifiedBuild, "ensure_editable", side_effect=ConflictError("locked")
        ),
        patch.object(GraphDBFactory, "_resolve_graph_backend", return_value=backend),
        patch.object(GraphDBFactory, "get_builder", return_value=builder),
        patch.object(GraphDBFactory, "get_graphdb", return_value=store),
    ):
        out = call(CertifiedGraphWriter(_domain(), SimpleNamespace()))
    return out, builder, store


_OLD = [{"subject": "c1", "predicate": "p", "object": "o"}]
_NEW = [{"subject": "c2", "predicate": "p", "object": "o"}]


class TestReplaceScope:
    def _replace(self, backend, **kw):
        return _scope(
            backend,
            lambda w: w.replace_scope(
                _NEW, kind="cohort", provenance="cohort:r1", run_id="t-1"
            ),
            **kw,
        )

    def test_a_lakehouse_scope_is_cleared_then_rewritten_in_the_composition(self):
        count, builder, store = self._replace("databricks")

        assert count == 1
        builder.delete_cert_enriched_scope.assert_called_once_with(
            kind="cohort", provenance="cohort:r1"
        )
        rows = builder.insert_cert_enriched.call_args.args[0]
        assert rows == [{**_NEW[0], "provenance": "cohort:r1"}]
        store.delete_triples.assert_not_called()
        store.insert_triples.assert_not_called()

    def test_a_graph_engine_scope_also_swaps_the_backend_rows(self):
        _count, builder, store = self._replace("lakebase", old_rows=_OLD)

        assert store.delete_triples.call_args.args == ("Dom_V2", _OLD)
        assert store.insert_triples.call_args.args == ("Dom_V2", _NEW)
        builder.delete_cert_enriched_scope.assert_called_once()

    def test_a_read_only_version_is_refused(self):
        with pytest.raises(ConflictError):
            self._replace("lakebase", editable=False)


class TestReplaceScopeFromSelect:
    def _replace(self, backend, **kw):
        return _scope(
            backend,
            lambda w: w.replace_scope_from_select(
                ["SELECT a", "SELECT b"],
                kind="community",
                provenance="community",
                run_id="t-2",
            ),
            **kw,
        )

    def test_the_selects_are_inserted_on_the_warehouse(self):
        count, builder, _store = self._replace("databricks")

        assert count == 7
        sqls = [c.args[0] for c in builder.insert_cert_enriched_from_select.call_args_list]
        assert sqls == ["SELECT a", "SELECT b"]
        builder.delete_cert_enriched_scope.assert_called_once_with(
            kind="community", provenance="community"
        )

    def test_a_graph_engine_receives_the_new_scope_rows(self):
        builder = MagicMock()
        builder.iter_cert_enriched_scope.side_effect = [iter([_OLD]), iter([_NEW])]
        store = MagicMock()
        with (
            patch.object(CertifiedBuild, "ensure_editable"),
            patch.object(GraphDBFactory, "_resolve_graph_backend", return_value="neo4j"),
            patch.object(GraphDBFactory, "get_builder", return_value=builder),
            patch.object(GraphDBFactory, "get_graphdb", return_value=store),
        ):
            CertifiedGraphWriter(_domain(), SimpleNamespace()).replace_scope_from_select(
                ["SELECT a"], kind="community", provenance="community"
            )

        assert store.delete_triples.call_args.args == ("Dom_V2", _OLD)
        assert store.insert_triples.call_args.args == ("Dom_V2", _NEW)


class TestRemoveScope:
    def test_a_graph_engine_scope_is_removed_from_both_sides(self):
        count, builder, store = _scope(
            "lakebase",
            lambda w: w.remove_scope(kind="community", provenance="community"),
            old_rows=_OLD,
        )

        assert count == 1
        assert store.delete_triples.call_args.args == ("Dom_V2", _OLD)
        builder.delete_cert_enriched_scope.assert_called_once_with(
            kind="community", provenance="community"
        )


def test_the_backend_cache_is_rebuilt_once_per_scope_operation():
    builder = MagicMock()
    builder.iter_cert_enriched_scope.side_effect = [iter([]), iter([_NEW, _NEW, _NEW])]
    store = MagicMock(supports_adjacency=True)
    with (
        patch.object(CertifiedBuild, "ensure_editable"),
        patch.object(GraphDBFactory, "_resolve_graph_backend", return_value="lakebase"),
        patch.object(GraphDBFactory, "get_builder", return_value=builder),
        patch.object(GraphDBFactory, "get_graphdb", return_value=store),
        patch("back.core.graphdb.search_cache.rebuild_graph_cache_if_enabled") as rebuild,
    ):
        CertifiedGraphWriter(_domain(), SimpleNamespace()).replace_scope_from_select(
            ["SELECT a"], kind="community", provenance="community"
        )

    assert store.insert_triples.call_count == 3
    rebuild.assert_called_once()


class TestOverrideScope:
    """Set-attribute values replace the backend value of their (subject, predicate)."""

    _VALUE = [{"subject": "c1", "predicate": "points", "object": "100"}]

    def _override(self, backend):
        return _scope(
            backend,
            lambda w: w.replace_scope(
                self._VALUE,
                kind=CertifiedGraphWriter.KIND_OVERRIDE,
                provenance="if_then:Points@c1",
            ),
        )

    def test_a_graph_engine_drops_the_previous_value_before_inserting(self):
        _count, _builder, store = self._override("lakebase")

        store.delete_subject_predicate.assert_called_once_with(
            "Dom_V2", [{"subject": "c1", "predicate": "points"}]
        )
        assert store.insert_triples.call_args.args == ("Dom_V2", self._VALUE)

    def test_a_lakehouse_override_is_left_to_the_certified_view(self):
        _count, builder, store = self._override("databricks")

        assert builder.insert_cert_enriched.call_args.kwargs["kind"] == "override"
        store.delete_subject_predicate.assert_not_called()

    def test_other_kinds_keep_the_backend_values(self):
        _count, _builder, store = _scope(
            "lakebase",
            lambda w: w.replace_scope(
                self._VALUE, kind=CertifiedGraphWriter.KIND_RULE, provenance="r"
            ),
        )

        store.delete_subject_predicate.assert_not_called()


class TestDropAttributeValues:
    def test_a_store_without_pair_delete_drops_the_exact_current_rows(self):
        store = SimpleNamespace(
            sql_table_reference=lambda table: "g",
            execute_query=MagicMock(
                return_value=[{"subject": "c1", "predicate": "points", "object": "5"}]
            ),
            delete_triples=MagicMock(),
        )

        CertifiedGraphWriter.drop_attribute_values(
            store, "Dom_V2", [{"subject": "c1", "predicate": "points", "object": "100"}]
        )

        store.delete_triples.assert_called_once_with(
            "Dom_V2", [{"subject": "c1", "predicate": "points", "object": "5"}]
        )

    def test_uri_objects_are_not_attribute_values(self):
        store = MagicMock()

        CertifiedGraphWriter.drop_attribute_values(
            store, "Dom_V2", [{"subject": "c1", "predicate": "p", "object": "https://x/o"}]
        )

        store.delete_subject_predicate.assert_not_called()
