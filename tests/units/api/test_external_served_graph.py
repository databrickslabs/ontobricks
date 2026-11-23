"""External reads are served from the Certified Graph (with the Lakehouse fallback)."""

from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.unit

MODULE = "api.routers.digitaltwin"


async def test_status_reports_the_served_relation_and_the_certified_flag():
    from api.routers.digitaltwin import dt_status

    store = MagicMock()
    store.table_exists.return_value = True
    store.get_status.return_value = {"count": 3, "last_modified": None}
    with (
        patch(f"{MODULE}._resolve_served_domain", return_value=MagicMock()),
        patch(f"{MODULE}.get_graphdb", return_value=store),
        patch(f"{MODULE}.effective_view_table", return_value="c.s.v"),
        patch(f"{MODULE}.effective_graph_name", return_value="Dom_V2"),
        patch(
            f"{MODULE}.TwinGraphAccess.served_query_table",
            return_value="c.s.v_certified",
        ) as served,
        patch(f"{MODULE}.CertifiedBuild.has_succeeded", return_value=True),
    ):
        resp = await dt_status(
            request=MagicMock(), session_mgr=MagicMock(), settings=MagicMock()
        )

    served.assert_called_once()
    store.get_status.assert_called_once_with("c.s.v_certified")
    assert resp.certified is True
    assert resp.count == 3


def test_external_graphql_reads_the_served_relation():
    from back.fastapi import graphql_routes

    domain = MagicMock()
    domain.ontology = {"classes": [{"uri": "C"}], "properties": [{"uri": "p"}]}
    domain.info = {"name": "Dom"}
    with (
        patch(
            "back.core.graphql.build_schema_for_domain",
            return_value=("schema", {}),
        ),
        patch.object(graphql_routes, "get_graphdb", return_value=MagicMock()),
        patch.object(
            graphql_routes.TwinGraphAccess,
            "served_query_table",
            return_value="c.s.v_certified",
        ) as served,
        patch.object(graphql_routes.TwinGraphAccess, "graph_query_table") as live,
    ):
        _schema, context = graphql_routes._get_schema_and_context(
            domain, MagicMock(), served=True
        )

    served.assert_called_once()
    live.assert_not_called()
    assert context["table_name"] == "c.s.v_certified"
