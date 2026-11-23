"""The Original Graph phase of the Build: inputs and Delta run."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from back.objects.digitaltwin.build import TwinLakehouseBuild
from back.objects.digitaltwin.certify import GraphBuild

pytestmark = pytest.mark.unit

MODULE = "api.routers.internal.dtwin"


@pytest.fixture
def api_client():
    from shared.fastapi.main import app

    return TestClient(app, raise_server_exceptions=False)


def test_prepare_validates_with_the_session_resolvers():
    with patch.object(TwinLakehouseBuild, "prepare_session", return_value="plan") as prep:
        plan = TwinLakehouseBuild.prepare(MagicMock(), MagicMock())

    assert plan == "plan"
    assert set(prep.call_args.kwargs) == {
        "view_table_fn",
        "data_table_fn",
        "get_credentials",
        "is_databricks_app_fn",
    }


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


def test_the_external_build_starts_the_single_build(api_client):
    domain = MagicMock()
    with (
        patch("api.routers.digitaltwin.DigitalTwin.resolve_domain", return_value=domain),
        patch.object(GraphBuild, "start", return_value="t-api") as start,
    ):
        response = api_client.post("/api/v1/digitaltwin/build?domain_name=demo", json={})

    assert response.status_code == 200
    assert response.json()["task_id"] == "t-api"
    assert start.call_args.kwargs == {"build_kind": "api"}
