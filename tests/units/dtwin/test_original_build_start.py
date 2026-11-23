"""The Original Graph build is started the same way for every backend."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.core.graphdb.GraphDBFactory import GraphDBFactory
from back.objects.digitaltwin.build import TwinLakehouseBuild

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path", ["/dtwin/sync/start", "/dtwin/databricks-build/start"])
def test_both_build_routes_start_the_original_build(api_client, path):
    domain = MagicMock()
    with (
        patch(f"{MODULE}.get_domain", return_value=domain),
        patch.object(TwinLakehouseBuild, "start", return_value="t-1") as start,
    ):
        response = api_client.post(path, json={})

    assert response.status_code == 200
    assert response.json()["task_id"] == "t-1"
    assert start.call_args.args[0] is domain


def test_start_runs_a_tracked_task_with_the_original_steps():
    plan = MagicMock()
    builder = MagicMock()
    builder.build_steps.return_value = [{"name": "prepare"}]
    tm = MagicMock()
    tm.run_background_task.return_value = SimpleNamespace(id="t-9")
    with (
        patch.object(TwinLakehouseBuild, "prepare_session", return_value=plan),
        patch.object(GraphDBFactory, "get_builder", return_value=builder),
        patch("back.core.task_manager.get_task_manager", return_value=tm),
    ):
        task_id = TwinLakehouseBuild.start(MagicMock(), MagicMock())

    assert task_id == "t-9"
    args = tm.run_background_task.call_args
    assert args.args[:3] == ("Original Graph Build", "original_build", plan._run)
    assert args.kwargs["steps"] == [{"name": "prepare"}]


def test_the_task_runs_the_delta_original_build():
    plan = TwinLakehouseBuild(
        view_table="c.s.v",
        data_table="c.s.v_data",
        r2rml_content="r2rml",
        host="h",
        token="t",
        warehouse_id="wh",
        base_uri="http://ex/",
        mapping_config={},
        ontology_config={},
        domain_snap=MagicMock(),
    )
    builder = MagicMock()

    plan._run(SimpleNamespace(id="t-9"), MagicMock(), builder, "session")

    kwargs = builder.run_triplestore_build.call_args.kwargs
    assert kwargs["task_id"] == "t-9"
    assert kwargs["view_table"] == "c.s.v"
    assert kwargs["data_table"] == "c.s.v_data"
    assert kwargs["build_kind"] == "session"


def test_the_external_build_starts_the_original_build(api_client):
    domain = MagicMock()
    with (
        patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain),
        patch.object(TwinLakehouseBuild, "start", return_value="t-api") as start,
    ):
        response = api_client.post("/api/v1/digitaltwin/build?domain_name=demo", json={})

    assert response.status_code == 200
    assert response.json()["task_id"] == "t-api"
    assert start.call_args.kwargs == {"build_kind": "api"}
