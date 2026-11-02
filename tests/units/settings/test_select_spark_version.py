"""``scripts/_internal/_select-spark-version.py``: the realtime stream job's runtime."""

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[3] / "scripts/_internal/_select-spark-version.py"


@pytest.fixture(scope="module")
def picker():
    spec = importlib.util.spec_from_file_location("_select_spark_version", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _v(key: str, name: str = "") -> dict:
    return {"key": key, "name": name or key}


class TestPick:
    def test_the_newest_19_lts_wins_over_a_newer_non_lts(self, picker):
        versions = [
            _v("19.0.x-scala2.13", "19.0 LTS (includes Apache Spark 4.2.0, Scala 2.13)"),
            _v("19.1.x-scala2.13", "19.1 (includes Apache Spark 4.2.0, Scala 2.13)"),
            _v("17.3.x-scala2.13", "17.3 LTS (includes Apache Spark 4.0.0, Scala 2.13)"),
        ]
        assert picker.SparkVersionPicker.pick(versions) == "19.0.x-scala2.13"

    def test_without_an_lts_the_newest_19_is_picked(self, picker):
        versions = [_v("19.0.x-scala2.13"), _v("19.2.x-scala2.13"), _v("19.10.x-scala2.13")]
        assert picker.SparkVersionPicker.pick(versions) == "19.10.x-scala2.13"

    def test_ml_gpu_photon_and_arm_runtimes_are_skipped(self, picker):
        versions = [
            _v("19.0.x-cpu-ml-scala2.13", "19.0 LTS ML (includes Apache Spark 4.2.0)"),
            _v("19.0.x-gpu-ml-scala2.13", "19.0 LTS GPU ML"),
            _v("19.0.x-photon-scala2.13", "19.0 LTS Photon"),
            _v("19.0.x-aarch64-scala2.13", "19.0 LTS ARM"),
        ]
        assert picker.SparkVersionPicker.pick(versions) == ""

    def test_no_19_runtime_picks_nothing(self, picker):
        assert picker.SparkVersionPicker.pick([_v("18.2.x-scala2.13", "18.2")]) == ""


class TestMain:
    def test_it_prints_the_pick_of_the_cli_json(self, picker, capsys):
        payload = {"versions": [_v("19.0.x-scala2.13", "19.0 LTS"), _v("18.2.x-scala2.13")]}
        assert picker.main(io.StringIO(json.dumps(payload))) == 0
        assert capsys.readouterr().out.strip() == "19.0.x-scala2.13"

    @pytest.mark.parametrize("text", ["", "not json", "[]", '{"versions": null}'])
    def test_unusable_input_prints_nothing(self, picker, capsys, text):
        assert picker.main(io.StringIO(text)) == 0
        assert capsys.readouterr().out == ""
