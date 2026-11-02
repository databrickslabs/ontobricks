"""``scripts/_internal/_render-slot-pipelines.py``: DAB slot pipelines of published versions."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from back.objects.digitaltwin.build import TwinIncrementalBuild
from tests.units.objects import test_build_pipeline_modes as _modes

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts/_internal/_render-slot-pipelines.py"
_SP = "00000000-aaaa-bbbb-cccc-111111111111"


@pytest.fixture(scope="module")
def renderer():
    spec = importlib.util.spec_from_file_location("_render_slot_pipelines", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Registry:
    """Published versions per folder; a folder without one is skipped."""

    def __init__(self, published: dict, ok: bool = True):
        self.published, self.ok = published, ok

    def list_domains(self):
        return self.ok, sorted(self.published), "" if self.ok else "registry down"

    def find_published_version(self, folder):
        version = self.published[folder]
        return (version, {}) if version else (None, {})


class _Runner:
    def __init__(self, existing: dict | None = None, bundled: tuple = ()):
        self.existing = existing or {}
        self.bundled = set(bundled)

    def find_pipeline_id(self, name):
        return f"pid-{name}" if name in self.existing else None

    def stored_fingerprint(self, pipeline_id):
        return self.existing[pipeline_id.removeprefix("pid-")]

    def bundle_managed(self, pipeline_id):
        return pipeline_id.removeprefix("pid-") in self.bundled


def _domain(name: str, version: str, mode: str):
    return SimpleNamespace(
        info={"name": name, "materialization_mode": mode},
        current_version=version,
        delta={"catalog": "main", "schema": "kg"},
    )


@pytest.fixture
def mappings(monkeypatch):
    entities = {_modes._CUSTOMER: _modes._row_local(), _modes._ORDER: _modes._batch()}
    monkeypatch.setattr(
        TwinIncrementalBuild, "mappings_for", staticmethod(lambda d: (entities, [], "http://ex.org/"))
    )
    return entities


def _collect(renderer, registry, modes: dict, runner=None, previous=None, notices=None):
    def load(svc, folder, version):
        return _domain(folder, version, modes[folder])

    return renderer.SlotPipelineBundle.collect(
        registry,
        SimpleNamespace(slot_pipeline_channel="PREVIEW", databricks_triplestore_table=""),
        runner or _Runner(),
        app_principal=_SP,
        load_domain=load,
        notice=(notices.append if notices is not None else lambda message: None),
        previous=previous,
    )


def _compile_fails(monkeypatch):
    from back.core.errors import ValidationError

    def fail(*_args, **_kwargs):
        raise ValidationError("unknown column")

    monkeypatch.setattr(TwinIncrementalBuild, "compile_for", staticmethod(fail))


class TestCollect:
    def test_a_published_incremental_version_gets_the_runner_s_pipeline(self, renderer, mappings):
        resources = _collect(renderer, _Registry({"sales": "3"}), {"sales": "incremental"})

        [(key, pipeline)] = resources.items()
        assert key == "ontobricks_slots_sales_v3"
        assert pipeline["name"] == "ontobricks-slots-sales-v3"
        assert pipeline["serverless"] is True and pipeline["development"] is False
        assert pipeline["continuous"] is False and pipeline["channel"] == "PREVIEW"
        assert (pipeline["catalog"], pipeline["schema"]) == ("main", "kg")
        assert pipeline["libraries"] == [
            {"file": {"path": f"/Workspace/Users/{_SP}/.ontobricks/pipelines/ontobricks-slots-sales-v3.sql"}}
        ]
        assert pipeline["permissions"] == [{"service_principal_name": _SP, "level": "CAN_MANAGE"}]

    @pytest.mark.parametrize("mode", ["microbatch", "realtime"])
    def test_continuous_modes_deploy_a_continuous_pipeline(self, renderer, mappings, mode):
        resources = _collect(renderer, _Registry({"sales": "3"}), {"sales": mode})

        assert resources["ontobricks_slots_sales_v3"]["continuous"] is True

    def test_a_new_pipeline_gets_no_fingerprint_an_existing_one_keeps_its_own(self, renderer, mappings):
        runner = _Runner({"ontobricks-slots-sales-v3": "fp-deployed"})

        resources = _collect(
            renderer, _Registry({"sales": "3", "hr": "1"}), {"sales": "incremental", "hr": "incremental"}, runner
        )

        key = "ontobricks.slot_sql_sha256"
        assert resources["ontobricks_slots_sales_v3"]["configuration"][key] == "fp-deployed"
        assert resources["ontobricks_slots_hr_v1"]["configuration"][key] == ""

    def test_full_unpublished_and_batch_only_versions_get_none(self, renderer, mappings, monkeypatch):
        registry = _Registry({"full_dom": "1", "draft_dom": None, "sales": "2"})
        resources = _collect(
            renderer, registry, {"full_dom": "full", "draft_dom": "incremental", "sales": "incremental"}
        )
        assert set(resources) == {"ontobricks_slots_sales_v2"}

        monkeypatch.setattr(
            TwinIncrementalBuild,
            "mappings_for",
            staticmethod(lambda d: ({_modes._ORDER: _modes._batch()}, [], "u")),
        )
        assert _collect(renderer, _Registry({"sales": "2"}), {"sales": "incremental"}) == {}

    def test_an_unreadable_registry_raises(self, renderer, mappings):
        with pytest.raises(RuntimeError, match="registry down"):
            _collect(renderer, _Registry({}, ok=False), {})

    def test_mappings_that_no_longer_compile_keep_the_previous_resource(self, renderer, mappings, monkeypatch):
        _compile_fails(monkeypatch)
        previous = {"ontobricks_slots_sales_v3": {"name": "ontobricks-slots-sales-v3", "kept": True}}
        notices: list = []

        resources = _collect(
            renderer, _Registry({"sales": "3"}), {"sales": "incremental"}, previous=previous, notices=notices
        )

        assert resources == previous
        assert "keep the previous ontobricks_slots_sales_v3" in notices[0] and "unknown column" in notices[0]

    def test_mappings_that_never_compiled_are_skipped(self, renderer, mappings, monkeypatch):
        _compile_fails(monkeypatch)
        notices: list = []

        assert _collect(renderer, _Registry({"sales": "3"}), {"sales": "incremental"}, notices=notices) == {}
        assert notices[0].startswith("skip sales v3")


class TestBindings:
    def test_a_pipeline_the_app_created_is_bound_a_bundled_or_new_one_is_not(self, renderer, mappings):
        runner = _Runner(
            {"ontobricks-slots-sales-v3": "fp", "ontobricks-slots-hr-v1": "fp"},
            bundled=("ontobricks-slots-hr-v1",),
        )
        resources = _collect(
            renderer,
            _Registry({"sales": "3", "hr": "1", "ops": "2"}),
            {"sales": "incremental", "hr": "incremental", "ops": "incremental"},
            runner,
        )

        assert renderer.SlotPipelineBundle.bindings(resources, runner) == [
            ("ontobricks_slots_sales_v3", "pid-ontobricks-slots-sales-v3")
        ]

    def test_an_absent_or_unreadable_previous_file_yields_no_resources(self, renderer, tmp_path):
        bad = tmp_path / "bad.yml"
        bad.write_text(":\n  - [unclosed")

        assert renderer.SlotPipelineBundle.previous_resources(tmp_path / "missing.yml") == {}
        assert renderer.SlotPipelineBundle.previous_resources(bad) == {}


class TestMain:
    def test_the_file_is_rendered_as_a_bundle_resource(self, renderer, mappings, tmp_path, monkeypatch):
        monkeypatch.setenv("APP_SP_CLIENT_ID", _SP)
        monkeypatch.setattr(
            renderer.SlotPipelineBundle,
            "load_domain",
            staticmethod(lambda svc, folder, version: _domain(folder, version, "incremental")),
        )
        output = tmp_path / "_generated_triple_slots.yml"

        code = renderer.main(
            ["--output", str(output)],
            svc_factory=lambda: _Registry({"sales": "3"}),
            runner_factory=_Runner,
            settings=SimpleNamespace(slot_pipeline_channel="CURRENT", databricks_triplestore_table=""),
        )

        assert code == 0
        text = output.read_text()
        assert text.startswith("# AUTO-GENERATED")
        pipelines = yaml.safe_load(text)["resources"]["pipelines"]
        assert pipelines["ontobricks_slots_sales_v3"]["channel"] == "CURRENT"
        assert (tmp_path / "_generated_triple_slots.bind").read_text() == ""

    def test_the_bind_list_names_the_pipelines_to_adopt(self, renderer, mappings, tmp_path, monkeypatch):
        monkeypatch.setenv("APP_SP_CLIENT_ID", _SP)
        monkeypatch.setattr(
            renderer.SlotPipelineBundle,
            "load_domain",
            staticmethod(lambda svc, folder, version: _domain(folder, version, "incremental")),
        )
        output = tmp_path / "_generated_triple_slots.yml"

        renderer.main(
            ["--output", str(output)],
            svc_factory=lambda: _Registry({"sales": "3"}),
            runner_factory=lambda: _Runner({"ontobricks-slots-sales-v3": "fp"}),
            settings=SimpleNamespace(slot_pipeline_channel="CURRENT", databricks_triplestore_table=""),
        )

        bind = (tmp_path / "_generated_triple_slots.bind").read_text()
        assert bind == "ontobricks_slots_sales_v3 pid-ontobricks-slots-sales-v3\n"

    @pytest.mark.parametrize("problem", ["no-principal", "registry-down"])
    def test_a_skipped_render_keeps_the_previous_file(self, renderer, tmp_path, monkeypatch, problem):
        output = tmp_path / "_generated_triple_slots.yml"
        output.write_text("previous")
        if problem == "no-principal":
            monkeypatch.delenv("APP_SP_CLIENT_ID", raising=False)
            factory = lambda: _Registry({})  # noqa: E731
        else:
            monkeypatch.setenv("APP_SP_CLIENT_ID", _SP)

            def factory():
                raise ConnectionError("Lakebase unreachable")

        stale_bind = tmp_path / "_generated_triple_slots.bind"
        stale_bind.write_text("ontobricks_slots_sales_v3 pid-old\n")

        assert renderer.main(["--output", str(output)], svc_factory=factory, runner_factory=_Runner) == 0
        assert output.read_text() == "previous"
        assert not stale_bind.exists()

    def test_no_eligible_version_renders_an_empty_resource_map(self, renderer, tmp_path, monkeypatch):
        monkeypatch.setenv("APP_SP_CLIENT_ID", _SP)
        output = tmp_path / "out.yml"

        renderer.main(["--output", str(output)], svc_factory=lambda: _Registry({}), runner_factory=_Runner)

        assert yaml.safe_load(output.read_text()) == {"resources": {"pipelines": {}}}


class TestDeployWiring:
    def test_deploy_renders_the_pipelines_before_validating_the_bundle(self):
        deploy = (REPO_ROOT / "scripts/deploy.sh").read_text()
        render = deploy.index("_render-slot-pipelines.py")
        validate = deploy.index('databricks bundle validate -t "$TARGET"', render - 3000)
        assert render < validate

    def test_the_generated_files_are_gitignored(self):
        ignored = (REPO_ROOT / ".gitignore").read_text()
        assert "/resources/_generated_triple_slots.yml" in ignored
        assert "/resources/_generated_triple_slots.bind" in ignored

    def test_deploy_binds_the_listed_pipelines_before_deploying(self):
        deploy = (REPO_ROOT / "scripts/deploy.sh").read_text()
        bind = deploy.index('databricks bundle deployment bind "$_key" "$_pipeline_id"')
        assert deploy.index('databricks bundle validate -t "$TARGET"') < bind
        assert bind < deploy.index('databricks bundle deploy -t "$TARGET"')
        assert "resources/_generated_triple_slots.bind" in deploy
        assert "databricks bundle deployment bind --help" in deploy
        assert "--auto-approve" in deploy[bind : bind + 300]
