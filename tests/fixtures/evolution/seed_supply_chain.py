"""Seed the OntoBricks Evolve supply-chain fixture into Unity Catalog.

Usage:
    uv run --frozen python tests/fixtures/evolution/seed_supply_chain.py \
        --profile <profile> --warehouse-id <id> --catalog <cat> --schema <sch>

Creates the fixture tables from ``supply_chain.sql``, then runs
``gold_check.sql`` and fails unless every hand-authored gold answer matches
the answer recomputed with plain SQL. Prints the Delta version of each
fixture table so an evaluation run can pin them.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
TABLES = (
    "suppliers",
    "products",
    "orders",
    "order_lines",
    "order_lines_nofk",
    "order_lines_orphan",
    "products_dup",
    "suppliers_ambiguous",
    "restricted_costs",
    "gold_answers",
)


def render(path: Path, catalog: str, schema: str) -> str:
    return path.read_text().replace("__CATALOG__", catalog).replace("__SCHEMA__", schema)


def statements(sql: str) -> list[str]:
    body = "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )
    return [s.strip() for s in body.split(";") if s.strip()]


def run(profile: str, warehouse_id: str, sql: str):
    """Run one statement through the Databricks CLI, which owns authentication."""
    body = json.dumps(
        {"statement": sql, "warehouse_id": warehouse_id, "wait_timeout": "50s"}
    )
    out = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "--profile", profile, "--json", body],
        capture_output=True, text=True, check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(f"CLI call failed: {out.stderr.strip()[:300]}")
    resp = json.loads(out.stdout)
    status = resp.get("status", {})
    if status.get("state") != "SUCCEEDED":
        error = (status.get("error") or {}).get("message") or status.get("state")
        raise RuntimeError(f"Statement failed ({error}): {sql[:120]}")
    return (resp.get("result") or {}).get("data_array") or []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", required=True)
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    for stmt in statements(render(HERE / "supply_chain.sql", args.catalog, args.schema)):
        run(args.profile, args.warehouse_id, stmt)

    rows = run(args.profile, args.warehouse_id, render(HERE / "gold_check.sql", args.catalog, args.schema))
    failed = [r for r in rows if str(r[3]).lower() != "true"]
    for r in rows:
        print(f"gold {r[0]}: expected={r[1]} computed={r[2]} matches={r[3]}")
    if failed:
        print(f"FAIL: {len(failed)} gold answer(s) do not match the source data")
        return 1

    for table in TABLES:
        fqn = f"{args.catalog}.{args.schema}.{table}"
        version = run(args.profile, args.warehouse_id, f"DESCRIBE HISTORY {fqn} LIMIT 1")[0][0]
        print(f"table {fqn} version={version}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
