"""Contract: post-deploy bootstrap grants CAN_USE on the bound SQL warehouse."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "bootstrap" / "app-permissions.sh"


def test_bootstrap_grants_warehouse_can_use_from_app_binding():
    src = SCRIPT.read_text(encoding="utf-8")
    assert "warehouses update-permissions" in src
    assert "CAN_USE" in src
    assert "sql-warehouse" in src or "sql_warehouse" in src
