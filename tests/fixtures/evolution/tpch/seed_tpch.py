"""Seed realistic Ontology Evolve fixtures from ``samples.tpch`` into Unity Catalog.

Usage:
    uv run --frozen python tests/fixtures/evolution/tpch/seed_tpch.py \
        --profile <p> --warehouse-id <id> --catalog <cat> --tier S|M|L

Each tier is a schema ``ontobricks_evo_tpch_<tier>`` holding TPC-H tables with
their raw prefixed column names and bigint keys, cut from the SF5 sample by
order key, plus dimension rows referenced by those orders. Primary and
foreign keys are declared (informational, as in a modelled lakehouse);
``lineitem`` gets one surrogate key ``l_lineid`` because the natural key is
composite. The S tier also gets messy variants: no constraints, a key cast
to string, 5% null keys, and a view instead of a table.

Gold answers are computed with plain SQL into ``gold_answers`` and the
filter values they use into ``gold_params``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

TIERS = {
    "S": "o_orderkey <= 3000",
    "M": "o_orderkey % 30 = 0",
    "L": "o_orderkey % 5 = 0",
}


def run(profile: str, warehouse: str, sql: str, timeout_s: int = 3600):
    """Run one statement via the CLI (which owns auth), polling until done."""
    body = json.dumps(
        {
            "statement": sql,
            "warehouse_id": warehouse,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        }
    )
    resp = _cli(profile, ["api", "post", "/api/2.0/sql/statements", "--json", body])
    deadline = time.time() + timeout_s
    while resp.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.time() > deadline:
            raise TimeoutError(sql[:120])
        time.sleep(5)
        resp = _cli(
            profile, ["api", "get", f"/api/2.0/sql/statements/{resp['statement_id']}"]
        )
    status = resp.get("status", {})
    if status.get("state") != "SUCCEEDED":
        error = (status.get("error") or {}).get("message") or status.get("state")
        raise RuntimeError(f"Statement failed ({error}): {sql[:160]}")
    return (resp.get("result") or {}).get("data_array") or []


def _cli(profile: str, args):
    out = subprocess.run(
        ["databricks", *args, "--profile", profile],
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(f"CLI call failed: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout)


def tier_sql(cat: str, sch: str, where: str):
    s = f"{cat}.{sch}"
    src = "samples.tpch"
    stmts = [
        f"CREATE SCHEMA IF NOT EXISTS {s}",
        f"CREATE OR REPLACE TABLE {s}.orders AS SELECT * FROM {src}.orders WHERE {where}",
        f"""CREATE OR REPLACE TABLE {s}.lineitem AS
            SELECT concat(CAST(l.l_orderkey AS STRING), '_', CAST(l.l_linenumber AS STRING)) AS l_lineid, l.*
            FROM {src}.lineitem l JOIN {s}.orders o ON o.o_orderkey = l.l_orderkey""",
        f"""CREATE OR REPLACE TABLE {s}.customer AS SELECT c.* FROM {src}.customer c
            WHERE c.c_custkey IN (SELECT o_custkey FROM {s}.orders)""",
        f"""CREATE OR REPLACE TABLE {s}.part AS SELECT p.* FROM {src}.part p
            WHERE p.p_partkey IN (SELECT l_partkey FROM {s}.lineitem)""",
        f"""CREATE OR REPLACE TABLE {s}.partsupp AS SELECT ps.* FROM {src}.partsupp ps
            WHERE ps.ps_partkey IN (SELECT p_partkey FROM {s}.part)""",
        f"""CREATE OR REPLACE TABLE {s}.supplier AS SELECT su.* FROM {src}.supplier su
            WHERE su.s_suppkey IN (SELECT l_suppkey FROM {s}.lineitem)
               OR su.s_suppkey IN (SELECT ps_suppkey FROM {s}.partsupp)""",
        f"CREATE OR REPLACE TABLE {s}.nation AS SELECT * FROM {src}.nation",
        f"CREATE OR REPLACE TABLE {s}.region AS SELECT * FROM {src}.region",
    ]
    pks = {
        "region": "r_regionkey",
        "nation": "n_nationkey",
        "customer": "c_custkey",
        "supplier": "s_suppkey",
        "part": "p_partkey",
        "orders": "o_orderkey",
        "lineitem": "l_lineid",
    }
    for table, col in pks.items():
        stmts += [
            f"ALTER TABLE {s}.{table} ALTER COLUMN {col} SET NOT NULL",
            f"ALTER TABLE {s}.{table} ADD CONSTRAINT {table}_pk PRIMARY KEY ({col})",
        ]
    fks = [
        ("nation", "n_regionkey", "region"),
        ("customer", "c_nationkey", "nation"),
        ("supplier", "s_nationkey", "nation"),
        ("orders", "o_custkey", "customer"),
        ("lineitem", "l_orderkey", "orders"),
        ("lineitem", "l_partkey", "part"),
        ("lineitem", "l_suppkey", "supplier"),
        ("partsupp", "ps_partkey", "part"),
        ("partsupp", "ps_suppkey", "supplier"),
    ]
    for table, col, ref in fks:
        stmts.append(
            f"ALTER TABLE {s}.{table} ADD CONSTRAINT {table}_{col}_fk "
            f"FOREIGN KEY ({col}) REFERENCES {s}.{ref}"
        )
    return stmts


def variant_sql(cat: str, sch: str):
    s = f"{cat}.{sch}"
    return [
        f"CREATE OR REPLACE TABLE {s}.lineitem_nofk AS SELECT * FROM {s}.lineitem",
        f"""CREATE OR REPLACE TABLE {s}.lineitem_cast AS
            SELECT * EXCEPT (l_suppkey), CAST(l_suppkey AS STRING) AS l_suppkey FROM {s}.lineitem""",
        f"""CREATE OR REPLACE TABLE {s}.lineitem_nulls AS
            SELECT * EXCEPT (l_suppkey),
                   CASE WHEN abs(hash(l_lineid)) % 20 = 0 THEN NULL ELSE l_suppkey END AS l_suppkey
            FROM {s}.lineitem""",
        f"CREATE OR REPLACE VIEW {s}.lineitem_v AS SELECT * FROM {s}.lineitem",
    ]


def gold_sql(cat: str, sch: str):
    s = f"{cat}.{sch}"
    return [
        f"""CREATE OR REPLACE TABLE {s}.gold_params AS
            WITH top_supp AS (
              SELECT su.s_name AS v FROM {s}.lineitem l JOIN {s}.supplier su ON su.s_suppkey = l.l_suppkey
              GROUP BY su.s_suppkey, su.s_name ORDER BY count(DISTINCT l.l_orderkey) DESC, su.s_suppkey LIMIT 1),
            top_part AS (
              SELECT p.p_name AS v FROM {s}.lineitem l JOIN {s}.part p ON p.p_partkey = l.l_partkey
              GROUP BY p.p_partkey, p.p_name ORDER BY count(*) DESC, p.p_partkey LIMIT 1),
            top_cust AS (
              SELECT c.c_name AS v FROM {s}.orders o JOIN {s}.customer c ON c.c_custkey = o.o_custkey
              GROUP BY c.c_custkey, c.c_name ORDER BY count(*) DESC, c.c_custkey LIMIT 1)
            SELECT 'orders_by_supplier' AS question_id, v AS value FROM top_supp
            UNION ALL SELECT 'orders_by_part', v FROM top_part
            UNION ALL SELECT 'suppliers_of_part', v FROM top_part
            UNION ALL SELECT 'orders_by_customer', v FROM top_cust
            UNION ALL SELECT 'customers_in_nation', 'GERMANY'
            UNION ALL SELECT 'suppliers_in_region', 'EUROPE'""",
        f"""CREATE OR REPLACE TABLE {s}.gold_answers AS
            WITH p AS (SELECT question_id, value FROM {s}.gold_params)
            SELECT 'orders_by_supplier' AS question_id,
                   array_sort(collect_set(CAST(l.l_orderkey AS STRING))) AS expected_ids
            FROM {s}.lineitem l JOIN {s}.supplier su ON su.s_suppkey = l.l_suppkey
            WHERE su.s_name = (SELECT value FROM p WHERE question_id = 'orders_by_supplier')
            UNION ALL
            SELECT 'orders_by_part', array_sort(collect_set(CAST(l.l_orderkey AS STRING)))
            FROM {s}.lineitem l JOIN {s}.part pt ON pt.p_partkey = l.l_partkey
            WHERE pt.p_name = (SELECT value FROM p WHERE question_id = 'orders_by_part')
            UNION ALL
            SELECT 'suppliers_of_part', array_sort(collect_set(CAST(ps.ps_suppkey AS STRING)))
            FROM {s}.partsupp ps JOIN {s}.part pt ON pt.p_partkey = ps.ps_partkey
            WHERE pt.p_name = (SELECT value FROM p WHERE question_id = 'suppliers_of_part')
            UNION ALL
            SELECT 'orders_by_customer', array_sort(collect_set(CAST(o.o_orderkey AS STRING)))
            FROM {s}.orders o JOIN {s}.customer c ON c.c_custkey = o.o_custkey
            WHERE c.c_name = (SELECT value FROM p WHERE question_id = 'orders_by_customer')
            UNION ALL
            SELECT 'customers_in_nation', array_sort(collect_set(CAST(c.c_custkey AS STRING)))
            FROM {s}.customer c JOIN {s}.nation n ON n.n_nationkey = c.c_nationkey WHERE n.n_name = 'GERMANY'
            UNION ALL
            SELECT 'suppliers_in_region', array_sort(collect_set(CAST(su.s_suppkey AS STRING)))
            FROM {s}.supplier su JOIN {s}.nation n ON n.n_nationkey = su.s_nationkey
            JOIN {s}.region r ON r.r_regionkey = n.n_regionkey WHERE r.r_name = 'EUROPE'""",
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--profile", required=True)
    ap.add_argument("--warehouse-id", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--tier", required=True, choices=sorted(TIERS))
    args = ap.parse_args()
    sch = f"ontobricks_evo_tpch_{args.tier.lower()}"
    stmts = tier_sql(args.catalog, sch, TIERS[args.tier])
    if args.tier == "S":
        stmts += variant_sql(args.catalog, sch)
    stmts += gold_sql(args.catalog, sch)
    t0 = time.time()
    for stmt in stmts:
        run(args.profile, args.warehouse_id, stmt)
    counts = run(
        args.profile,
        args.warehouse_id,
        " UNION ALL ".join(
            f"SELECT '{t}', count(*) FROM {args.catalog}.{sch}.{t}"
            for t in ("orders", "lineitem", "customer", "supplier", "part", "partsupp")
        ),
    )
    gold = run(
        args.profile,
        args.warehouse_id,
        f"SELECT g.question_id, size(g.expected_ids), p.value FROM {args.catalog}.{sch}.gold_answers g "
        f"JOIN {args.catalog}.{sch}.gold_params p USING (question_id) ORDER BY 1",
    )
    print(
        json.dumps(
            {
                "schema": sch,
                "seconds": round(time.time() - t0),
                "rows": counts,
                "gold": gold,
            },
            indent=1,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
