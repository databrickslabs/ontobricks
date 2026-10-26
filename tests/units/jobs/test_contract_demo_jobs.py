"""SQL and CLI contracts for the create/delete fake-contract test Jobs."""

from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path

import pytest

from jobs.contract_demo import (
    ContractDemoTable,
    FAKE_CONTRACT_PREFIX,
    require_entity_id,
    validate_identifier,
)
from jobs.create_fake_contract_job import parse_args as parse_create
from jobs.delete_contract_job import parse_args as parse_delete

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_not_declared_in_the_bundle():
    """Test-only Jobs: created by hand in the workspace, never deployed by DAB."""
    for path in (REPO_ROOT / "resources").glob("*.yml"):
        text = path.read_text()
        assert "create_fake_contract_job" not in text, path.name
        assert "delete_contract_job" not in text, path.name


def test_argparse_accepts_bundle_flags():
    create = parse_create(
        ["--entity-id", "CUST1", "--contract-table", "cat.sch.demo_contracts"]
    )
    assert create.entity_id == "CUST1"
    assert create.contract_table == "cat.sch.demo_contracts"
    delete = parse_delete(
        ["--entity-id", "FAKE-CUST1-ab", "--contract-table", "cat.sch.demo_contracts"]
    )
    assert delete.entity_id == "FAKE-CUST1-ab"


def test_fake_row_is_prefixed_and_tied_to_customer():
    store = ContractDemoTable("cat.sch.demo_contracts")
    row = store.fake_row("CUST1", now=datetime(2026, 1, 2, 3, 4, 5), suffix="deadbeef")
    assert row["contract_id"] == f"{FAKE_CONTRACT_PREFIX}CUST1-deadbeef"
    assert row["customer_id"] == "CUST1"
    assert row["status"] == "DRAFT"
    assert row["label"] == "Fake contract for CUST1"


def test_delete_sql_matches_contract_or_fake_customer_rows():
    store = ContractDemoTable("cat.sch.demo_contracts")
    sql = store.delete_sql("CUST'1")
    assert "DELETE FROM cat.sch.demo_contracts" in sql
    assert "contract_id = 'CUST''1'" in sql
    assert "customer_id = 'CUST''1'" in sql
    assert f"contract_id LIKE '{FAKE_CONTRACT_PREFIX}%'" in sql


def test_require_entity_id_and_identifier_guards():
    with pytest.raises(ValueError, match="entity_id"):
        require_entity_id("  ")
    with pytest.raises(ValueError, match="plain identifier"):
        validate_identifier("cat.sch;drop", "--contract-table")
    with pytest.raises(ValueError, match="entity_id"):
        ContractDemoTable("cat.sch.t").fake_row("")


def test_job_scripts_load_without_pyspark():
    """Spark is imported inside main(); loading the module must stay cheap."""
    for name in ("create_fake_contract_job", "delete_contract_job"):
        path = REPO_ROOT / "src/jobs" / f"{name}.py"
        spec = importlib.util.spec_from_file_location(f"_cdj_{name}", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        assert callable(module.main)
