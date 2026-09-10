"""Regression tests for the Databricks-only triple-store build pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from back.core.graphdb.delta.DeltaTripleStoreBuildPipeline import (
    DeltaTripleStoreBuildPipeline,
)


@pytest.mark.unit
def test_prepare_translation_uses_configured_build_transport() -> None:
    pipe = DeltaTripleStoreBuildPipeline.__new__(DeltaTripleStoreBuildPipeline)
    pipe.tm = MagicMock()
    pipe.task_id = "task-1"
    pipe.domain = SimpleNamespace()
    pipe.settings = SimpleNamespace()
    pipe.host = "host"
    pipe.token = "token"
    pipe.warehouse_id = "wh-build"
    pipe.r2rml_content = "mapping"
    pipe.mapping_config = {}
    pipe.ontology_config = {}
    pipe.base_uri = "https://example.test/"
    pipe.is_api = False

    with (
        patch("back.core.databricks.DatabricksClient") as client_cls,
        patch("back.core.helpers.resolve_build_use_sea", return_value=True),
        patch(
            "back.core.w3c.sparql.extract_r2rml_mappings",
            return_value=([{"entity": "Customer"}], []),
        ),
        patch(
            "back.objects.digitaltwin.DigitalTwin.DigitalTwin."
            "augment_mappings_from_config",
            side_effect=lambda mappings, *_args: mappings,
        ),
        patch(
            "back.objects.digitaltwin.DigitalTwin.DigitalTwin."
            "augment_relationships_from_config",
            side_effect=lambda mappings, *_args: mappings,
        ),
        patch(
            "back.core.w3c.sparql.translate_sparql_to_spark",
            return_value={"success": True, "sql": "SELECT 1"},
        ),
    ):
        assert pipe._prepare_translation() is True

    client_cls.assert_called_once_with(
        host="host",
        token="token",
        warehouse_id="wh-build",
        use_sea=True,
    )
