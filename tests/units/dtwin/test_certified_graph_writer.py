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
    builder.ensure_cert_enriched_table.assert_called_once_with()
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
