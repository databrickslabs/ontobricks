"""Unit tests for scripts/marketplace/assemble.py."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

pytestmark = pytest.mark.unit

_ROOT = pathlib.Path(__file__).resolve().parents[3]
_SPEC = importlib.util.spec_from_file_location(
    "marketplace_assemble", _ROOT / "scripts" / "marketplace" / "assemble.py"
)
mod = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(mod)


@pytest.fixture
def fake_repo(tmp_path: pathlib.Path) -> pathlib.Path:
    repo = tmp_path / "repo"
    for name in mod.ROOT_FILES:
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(name)
    (repo / "src/back").mkdir(parents=True)
    (repo / "src/back/x.py").write_text("x = 1\n")
    (repo / "src/back/__pycache__").mkdir()
    (repo / "src/back/__pycache__/x.cpython-311.pyc").write_bytes(b"\0")
    (repo / "src/mcp-server").mkdir()
    (repo / "src/mcp-server/app.yaml").write_text("mcp")
    (repo / "src/.coding_rules.md").write_text("repo meta")
    # Help Center set (0.10.x): docs/*.md + docs/images/ + docs/screenshots/.
    (repo / "docs/images").mkdir(parents=True)
    (repo / "docs/screenshots").mkdir()
    (repo / "docs/README.md").write_text("# overview")
    (repo / "docs/user-guide.md").write_text("# guide")
    (repo / "docs/images/arch.png").write_bytes(b"png")
    (repo / "docs/screenshots/home.png").write_bytes(b"png")
    # Not part of the Help Center set.
    (repo / "docs/sphinx/_build").mkdir(parents=True)
    (repo / "docs/sphinx/_build/index.html").write_text("built")
    (repo / "docs/diagrams").mkdir()
    (repo / "docs/diagrams/arch.drawio").write_text("drawio")
    (repo / "tests").mkdir()
    (repo / "tests/test_x.py").write_text("")
    (repo / "marketplace").mkdir()
    (repo / "marketplace/app.yaml").write_text("command: []\n")
    (repo / "marketplace/manifest.yaml").write_text("version: 1\n")
    return repo


def test_app_folder_contains_runtime_files(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    for name in (*mod.ROOT_FILES, "app.yaml", "manifest.yaml", "src/back/x.py"):
        assert (target / name).is_file(), name


def test_app_folder_is_a_subfolder(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert target == tmp_path / "out" / "ontobricks"


def test_dev_only_content_is_excluded(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not (target / "tests").exists()
    assert not (target / "src/mcp-server").exists()
    assert not list(target.rglob("*.pyc"))
    assert not (target / "src/.coding_rules.md").exists()


def test_help_center_set_is_shipped(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    for name in (
        "docs/README.md",
        "docs/user-guide.md",
        "docs/images/arch.png",
        "docs/screenshots/home.png",
    ):
        assert (target / name).is_file(), name


def test_non_help_center_docs_are_not_shipped(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not (target / "docs/sphinx").exists()
    assert not (target / "docs/diagrams").exists()


def test_static_configs_come_from_marketplace_dir(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert (target / "app.yaml").read_text() == "command: []\n"


def test_reassembly_removes_stale_files(fake_repo, tmp_path):
    target = mod.assemble(fake_repo, tmp_path / "out")
    (target / "src/back/stale.py").write_text("old")
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not (target / "src/back/stale.py").exists()


def test_missing_root_file_fails_before_writing(fake_repo, tmp_path):
    (fake_repo / "run.py").unlink()
    with pytest.raises(FileNotFoundError, match="run.py"):
        mod.assemble(fake_repo, tmp_path / "out")
    assert not (tmp_path / "out" / "ontobricks").exists()


def test_out_dir_inside_copied_tree_is_refused(fake_repo):
    with pytest.raises(ValueError, match="inside"):
        mod.assemble(fake_repo, fake_repo / "src" / "dist")


def test_out_dir_that_contains_the_repo_is_refused(fake_repo):
    # --out ~/dev/workspaces would otherwise rmtree ~/dev/workspaces/ontobricks.
    with pytest.raises(ValueError, match="contains the repo"):
        mod.assemble(fake_repo, fake_repo.parent)


def test_unrelated_existing_folder_is_never_deleted(fake_repo, tmp_path):
    precious = tmp_path / "out" / "ontobricks" / "precious.txt"
    precious.parent.mkdir(parents=True)
    precious.write_text("keep me")
    with pytest.raises(ValueError, match="not a previous bundle"):
        mod.assemble(fake_repo, tmp_path / "out")
    assert precious.read_text() == "keep me"


def test_env_files_and_logs_are_not_shipped(fake_repo, tmp_path):
    (fake_repo / "src/back/.env.local").write_text("DATABRICKS_TOKEN=dapi-secret")
    (fake_repo / "src/back/app.log").write_text("log")
    target = mod.assemble(fake_repo, tmp_path / "out")
    assert not list(target.rglob(".env*"))
    assert not list(target.rglob("*.log"))


def test_real_repo_has_every_input():
    assert mod.missing_inputs(_ROOT) == []
