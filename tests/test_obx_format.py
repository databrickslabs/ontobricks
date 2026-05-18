"""Tests for back.objects.registry.obx_format — `.obx` envelope contract.

Covers the version compatibility surface: ``CURRENT_OBX_FORMAT_VERSION``,
validation of ``format_version``, the ``min_ontobricks_version`` gate, and
the upgrader chain pattern (registered + executed transparently when an
older file is loaded).
"""

from __future__ import annotations

import pytest

from back.core.errors import ValidationError
from back.objects.registry import obx_format


def _make_envelope(**overrides):
    base = {
        "format_version": obx_format.CURRENT_OBX_FORMAT_VERSION,
        "ontobricks_version": "0.4.0",
        "exported_at": "2026-05-16T17:00:00Z",
        "exported_by": "tester@example.com",
        "domains": [],
    }
    base.update(overrides)
    return base


class TestBuildEnvelope:
    def test_includes_required_fields(self):
        env = obx_format.build_envelope(
            [{"name": "claims", "info": {}, "versions": {"1": {}}}],
            exported_by="me@example.com",
        )
        assert env["format_version"] == obx_format.CURRENT_OBX_FORMAT_VERSION
        assert env["ontobricks_version"]
        assert env["exported_by"] == "me@example.com"
        assert isinstance(env["domains"], list) and len(env["domains"]) == 1
        assert "min_ontobricks_version" not in env

    def test_min_version_only_when_set(self):
        env = obx_format.build_envelope([], min_ontobricks_version="1.0.0")
        assert env["min_ontobricks_version"] == "1.0.0"


class TestLoadValidation:
    def test_rejects_non_dict(self):
        with pytest.raises(ValidationError):
            obx_format.load("not a dict")
        with pytest.raises(ValidationError):
            obx_format.load([])

    def test_rejects_missing_format_version(self):
        with pytest.raises(ValidationError, match="format_version"):
            obx_format.load({"domains": []})

    def test_rejects_non_integer_format_version(self):
        with pytest.raises(ValidationError, match="positive integer"):
            obx_format.load(_make_envelope(format_version="1"))
        with pytest.raises(ValidationError, match="positive integer"):
            obx_format.load(_make_envelope(format_version=0))
        with pytest.raises(ValidationError, match="positive integer"):
            obx_format.load(_make_envelope(format_version=True))

    def test_rejects_future_format_version(self):
        future = obx_format.CURRENT_OBX_FORMAT_VERSION + 10
        with pytest.raises(ValidationError, match="Unsupported"):
            obx_format.load(_make_envelope(format_version=future))

    def test_rejects_when_min_ontobricks_version_too_high(self, monkeypatch):
        monkeypatch.setattr(obx_format, "APP_VERSION", "0.1.0")
        with pytest.raises(ValidationError, match="requires OntoBricks"):
            obx_format.load(_make_envelope(min_ontobricks_version="9.9.9"))

    def test_accepts_current_version(self):
        env = obx_format.load(_make_envelope())
        assert env["format_version"] == obx_format.CURRENT_OBX_FORMAT_VERSION

    def test_rejects_non_list_domains(self):
        with pytest.raises(ValidationError, match="'domains' must be a list"):
            obx_format.load(_make_envelope(domains={"oops": True}))


class TestUpgraderChain:
    def test_chain_runs_through_to_current(self, monkeypatch):
        """Pretend the current version is 3 and prove v1 envelopes get upgraded."""

        def _v1_to_v2(env):
            return {**env, "upgraded_v2": True}

        def _v2_to_v3(env):
            return {**env, "upgraded_v3": True}

        monkeypatch.setattr(obx_format, "CURRENT_OBX_FORMAT_VERSION", 3)
        monkeypatch.setattr(
            obx_format,
            "_UPGRADERS",
            {1: _v1_to_v2, 2: _v2_to_v3, 3: None},
        )

        env_v1 = {
            "format_version": 1,
            "ontobricks_version": "0.4.0",
            "domains": [],
        }
        loaded = obx_format.load(env_v1)
        assert loaded["format_version"] == 3
        assert loaded["upgraded_v2"] is True
        assert loaded["upgraded_v3"] is True

    def test_missing_upgrader_raises(self, monkeypatch):
        monkeypatch.setattr(obx_format, "CURRENT_OBX_FORMAT_VERSION", 2)
        monkeypatch.setattr(obx_format, "_UPGRADERS", {1: None, 2: None})

        with pytest.raises(ValidationError, match="No upgrade path"):
            obx_format.load(
                {"format_version": 1, "ontobricks_version": "x", "domains": []}
            )
