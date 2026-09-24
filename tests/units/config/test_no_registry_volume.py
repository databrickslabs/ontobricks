"""Guardrails: the registry UC Volume was removed in v0.9.0.

Parsed documents (the Knowledge Store) live in Lakebase, so the deploy
pipeline no longer provisions or binds a Unity Catalog Volume. These
static checks fail loudly if a Volume reference creeps back into the
config surface.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]


def _read(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


def test_databricks_yml_has_no_volume_resource():
    text = _read("databricks.yml")
    assert "registry_volume" not in text
    assert "WRITE_VOLUME" not in text
    assert 'securable_type: "VOLUME"' not in text


def test_app_yaml_template_has_no_volume_binding():
    text = _read("app.yaml.template")
    assert "REGISTRY_VOLUME_PATH" not in text
    assert "REGISTRY_VOLUME" not in text
    assert "valueFrom: volume" not in text
    # The catalog/schema env are still needed for Delta build targets.
    assert "REGISTRY_CATALOG" in text
    assert "REGISTRY_SCHEMA" in text


def test_deploy_scripts_have_no_volume_wiring():
    for rel in (
        "scripts/deploy.sh",
        "scripts/deploy.config.sh",
        "scripts/update-deployed-app.sh",
        "scripts/_internal/check-deploy-prerequisites.sh",
    ):
        text = _read(rel)
        assert "REGISTRY_VOLUME" not in text, rel
        assert "databricks volumes read" not in text, rel


def test_settings_dropped_volume_fields():
    from shared.config.settings import Settings

    s = Settings(_env_file=None)
    assert not hasattr(s, "registry_volume")
    assert not hasattr(s, "registry_volume_path")


def test_health_dropped_volume_probes():
    from shared.fastapi import health

    assert not hasattr(health, "_check_registry_volume_read")
    assert not hasattr(health, "_check_registry_volume_write")
