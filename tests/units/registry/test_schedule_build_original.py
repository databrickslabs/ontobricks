"""Scheduled build: refreshes the Original Graph, never loads a backend."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.registry.schedule import build

pytestmark = pytest.mark.unit


def _run(count):
    builder = MagicMock()
    builder.count_triples.return_value = count
    with patch.object(GraphDBFactory, "get_builder", return_value=builder) as get:
        n = build._build_original(
            "src", "c.s.v", SimpleNamespace(), SimpleNamespace(), "dom"
        )
    return n, builder, get


def test_the_original_graph_is_exposed_and_its_companions_refreshed():
    n, builder, get = _run(12)

    assert n == 12
    assert get.call_args.kwargs == {"engine": "delta", "client": "src"}
    builder.apply_original_graph.assert_called_once_with("c.s.v")
    builder.rebuild_original_companions.assert_called_once_with()


def test_an_empty_mapping_leaves_the_data_view_untouched():
    n, builder, _get = _run(0)

    assert n == 0
    builder.apply_original_graph.assert_not_called()
    builder.rebuild_original_companions.assert_not_called()
