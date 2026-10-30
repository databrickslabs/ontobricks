"""Catalog of tables, views and graphs a simulation sandbox creates."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from back.core.graphdb.lakebase import _companion_ddl
from back.objects.simulation import Artifacts

pytestmark = pytest.mark.unit


class TestArtifacts:
    def test_lakebase_expected_names(self):
        objects = Artifacts.expected("lakebase", "Dom_V1", "sAbc")
        kinds = {o["role"]: o for o in objects}
        assert kinds["sandbox"]["kind"] == "table"
        assert kinds["sandbox"]["name"] == _companion_ddl.sim_phy("Dom_V1", "sAbc")
        assert kinds["explorer_inferred"]["name"] == _companion_ddl.sim_union_phy(
            "Dom_V1", "sAbc", include_inferred=True
        )
        assert kinds["explorer_asserted"]["name"] == _companion_ddl.sim_union_phy(
            "Dom_V1", "sAbc", include_inferred=False
        )

    def test_delta_and_neo4j_expected_names(self):
        delta = Artifacts.expected(
            "databricks", "cat.sch.tbl", "s1", sandbox_table="cat.sch.tbl__sim_s1"
        )
        assert [o["kind"] for o in delta] == ["table", "view", "view"]
        assert delta[1]["name"] == "cat.sch.tbl__smu_s1"
        neo = Artifacts.expected("neo4j", "G_V1", "s1")
        assert neo == [{"kind": "graph", "role": "sandbox", "name": "G_V1__sim_s1"}]

    def test_describe_draft_has_names_without_counts(self):
        row = {"id": "s1", "status": "draft", "diff_summary": {}}
        domain = SimpleNamespace(info={"name": "Dom", "graph_backend": "lakebase"}, current_version="1")
        out = Artifacts.describe(domain, row)
        assert out["counts"] == {}
        assert out["objects"][0]["name"] == _companion_ddl.sim_phy("Dom_V1", "s1")

    def test_describe_ready_uses_run_summary(self):
        objects = [{"kind": "table", "role": "sandbox", "name": "g__sim_s1"}]
        row = {
            "id": "s1",
            "status": "ready",
            "diff_summary": {
                "artifacts": objects,
                "entity_count": 12,
                "after_count": 40,
                "before_count": 30,
                "added_count": 10,
                "removed_count": 0,
                "added_inferred_count": 4,
                "touched_entity_count": 3,
            },
        }
        domain = SimpleNamespace(info={"name": "Dom"}, current_version="1")
        out = Artifacts.describe(domain, row)
        assert out["objects"] == objects
        assert out["counts"]["entities"] == 12
        assert out["counts"]["triples"] == 40
        assert out["counts"]["inferred"] == 4

    def test_from_sandbox_delta_views(self):
        sandbox = SimpleNamespace(
            table="cat.sch.g__sim_s1",
            _graph_name="g",
            _sim_id="s1",
            _view_name=lambda inferred: (
                "cat.sch.g__smu_s1" if inferred else "cat.sch.g__sms_s1"
            ),
        )
        objects = Artifacts.from_sandbox(sandbox, "databricks")
        assert [o["name"] for o in objects] == [
            "cat.sch.g__sim_s1",
            "cat.sch.g__smu_s1",
            "cat.sch.g__sms_s1",
        ]
