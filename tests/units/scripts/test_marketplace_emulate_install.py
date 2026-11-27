"""Unit tests for scripts/marketplace/emulate_install.py.

The helper installs the assembled Marketplace folder into a workspace the way a
Marketplace install would: fixed app.yaml, resources bound by name, and none of
the ``make deploy`` bootstrap grants. Only the pure planning logic is tested
here; the CLI calls are exercised by running the helper against a workspace.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest
import yaml

pytestmark = pytest.mark.unit

_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "marketplace_emulate_install",
    _ROOT / "scripts" / "marketplace" / "emulate_install.py",
)
mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(mod)

_BRANCH = "projects/ontobricks-mp/branches/production"


def _names(resources):
    return [r["name"] for r in resources]


class TestBuildResources:
    def test_names_match_manifest(self):
        manifest = yaml.safe_load((_ROOT / "marketplace" / "manifest.yaml").read_text())
        expected = sorted(r["name"] for r in manifest["resource_specs"])
        assert sorted(_names(mod.build_resources("wh1", _BRANCH, "db-abc"))) == expected

    def test_postgres_paths_are_full_resource_paths(self):
        got = mod.build_resources("wh1", _BRANCH, "db-abc")
        pg = next(r for r in got if r["name"] == "postgres")["postgres"]
        assert pg == {
            "branch": _BRANCH,
            "database": f"{_BRANCH}/databases/db-abc",
            "permission": "CAN_CONNECT_AND_CREATE",
        }

    def test_no_volume_is_bound(self):
        # 0.10.x keeps no domain artefacts on a UC Volume.
        assert "volume" not in _names(mod.build_resources("wh1", _BRANCH, "db-abc"))

    @pytest.mark.parametrize(
        "bad", [None, "", "ontobricks-mp", "projects/p", "projects/p/branch/b"]
    )
    def test_rejects_malformed_branch_path(self, bad):
        with pytest.raises(ValueError, match="projects/<project>/branches/<branch>"):
            mod.build_resources("wh1", bad, "db-abc")

    def test_branch_without_database_is_rejected(self):
        with pytest.raises(ValueError, match="database"):
            mod.build_resources("wh1", _BRANCH, None)


class TestUserScopes:
    def test_scopes_come_from_the_manifest(self):
        manifest = yaml.safe_load((_ROOT / "marketplace" / "manifest.yaml").read_text())
        assert (
            mod.manifest_user_scopes(_ROOT / "marketplace" / "manifest.yaml")
            == manifest["user_api_scopes"]
        )

    def test_new_app_is_created_with_the_scopes(self):
        cmds = mod.install_commands(
            "prof",
            "ob-mp",
            "/Workspace/x",
            exists=False,
            resources=[],
            user_scopes=["sql"],
        )
        assert json.loads(cmds[0][4])["user_api_scopes"] == ["sql"]

    def test_existing_app_gets_resources_and_scopes_by_mask(self):
        cmds = mod.install_commands(
            "prof",
            "ob-mp",
            "/Workspace/x",
            exists=True,
            resources=[],
            user_scopes=["sql"],
        )
        body = json.loads(cmds[0][cmds[0].index("--json") + 1])
        assert body["update_mask"] == "resources,user_api_scopes"
        assert body["app"]["user_api_scopes"] == ["sql"]


class TestAppName:
    @pytest.mark.parametrize("good", ["ontobricks-mp-test", "ob1"])
    def test_accepts_valid_names(self, good):
        assert mod.validate_app_name(good) == good

    @pytest.mark.parametrize("bad", ["OntoBricks", "ob_test", "-ob", "x", "a" * 31])
    def test_rejects_invalid_names(self, bad):
        with pytest.raises(ValueError):
            mod.validate_app_name(bad)


class TestWorkspacePath:
    def test_path_is_scoped_to_a_test_folder(self):
        path = mod.workspace_source_path("me@databricks.com", "ob-mp")
        assert (
            path
            == "/Workspace/Users/me@databricks.com/ontobricks-marketplace-test/ob-mp"
        )


class TestReplaceGuard:
    def test_app_created_by_this_helper_may_be_replaced(self):
        assert mod.may_replace(
            {"name": "ob-mp", "description": mod.TEST_APP_DESCRIPTION}
        )

    @pytest.mark.parametrize("desc", ["OntoBricks (development sandbox app)", "", None])
    def test_any_other_app_is_protected(self, desc):
        assert not mod.may_replace({"name": "ontobricks", "description": desc})

    def test_new_app_uses_the_test_description(self):
        cmds = mod.install_commands(
            "prof", "ob-mp", "/Workspace/x", exists=False, resources=[]
        )
        assert json.loads(cmds[0][4])["description"] == mod.TEST_APP_DESCRIPTION


class TestCommandPlan:
    def test_new_app_is_created_with_name_and_resources_in_json(self):
        # The CLI rejects a positional NAME when --json is given.
        cmds = mod.install_commands(
            "prof", "ob-mp", "/Workspace/x", exists=False, resources=[{"name": "r"}]
        )
        assert cmds[0][:4] == ["databricks", "apps", "create", "--json"]
        body = json.loads(cmds[0][4])
        assert body["name"] == "ob-mp" and body["resources"] == [{"name": "r"}]

    def test_every_command_carries_the_profile(self):
        cmds = mod.install_commands(
            "prof", "ob-mp", "/Workspace/x", exists=False, resources=[]
        )
        assert all(c[c.index("--profile") + 1] == "prof" for c in cmds)

    def test_existing_app_gets_resources_replaced_by_mask(self):
        cmds = mod.install_commands(
            "prof", "ob-mp", "/Workspace/x", exists=True, resources=[{"name": "r"}]
        )
        assert cmds[0][:4] == ["databricks", "apps", "create-update", "ob-mp"]
        body = json.loads(cmds[0][cmds[0].index("--json") + 1])
        assert body["app"]["resources"] == [{"name": "r"}]

    def test_last_command_deploys_from_workspace_path(self):
        cmds = mod.install_commands(
            "prof", "ob-mp", "/Workspace/x", exists=False, resources=[]
        )
        assert cmds[-1][:4] == ["databricks", "apps", "deploy", "ob-mp"]
        assert cmds[-1][cmds[-1].index("--source-code-path") + 1] == "/Workspace/x"
