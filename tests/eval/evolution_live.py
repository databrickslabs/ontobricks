"""Live Ontology Evolve driver: real Lakebase registry, real warehouses, real Delta builds.

Calls the same services the ``/evolution`` routes call (coverage, proposal,
evaluation with both arm builds), without HTTP, and reports timings,
warehouse query counts and outcomes per scenario. Environment: the local
app environment (``DATABRICKS_*``, ``REGISTRY_*``, ``LAKEBASE_*``).

    uv run --frozen python tests/eval/evolution_live.py --tier S --variant base \
        --target orders_by_supplier --checks customers_in_nation orders_by_customer
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
for extra in (ROOT / "src", ROOT):
    if str(extra) not in sys.path:
        sys.path.insert(0, str(extra))

from back.core.errors import OntoBricksError  # noqa: E402
from back.core.helpers import get_databricks_client  # noqa: E402
from back.objects.ontology.evolution import (  # noqa: E402
    CandidateBuilder,
    EvaluationService,
    EvolutionPipeline,
    EvolutionService,
    WarehouseSourceProbe,
)
from back.objects.registry.RegistryService import RegistryService  # noqa: E402
from back.objects.registry.schedule.TaskContext import _FakeSessionMgr  # noqa: E402
from back.objects.session import sanitize_domain_folder  # noqa: E402
from back.objects.session.DomainSession import DomainSession  # noqa: E402
from shared.config.settings import get_settings  # noqa: E402
from tests.fixtures.evolution.tpch.tpch_domain import (  # noqa: E402
    questions,
    tpch_document,
)  # noqa: E402

CATALOG = "serverless_lakebase_praneeth_catalog"


class Counter:
    def __init__(self, client):
        self._client = client
        self.n = 0

    def __call__(self, sql):
        self.n += 1
        return self._client.sql.execute_query(sql, statement_timeout_s=300)


def session(settings, registry_schema: str):
    domain = DomainSession(_FakeSessionMgr())
    domain.settings["registry"] = {
        "catalog": CATALOG,
        "schema": registry_schema,
        "volume": "OntoBricksRegistry",
    }
    domain.databricks["host"] = settings.databricks_host
    return domain


def params(probe: WarehouseSourceProbe, schema: str):
    rows = probe._execute(
        f"SELECT question_id, value FROM {CATALOG}.{schema}.gold_params"
    )
    return {r["question_id"]: r["value"] for r in rows}


def scenario(args) -> dict:
    settings = get_settings()
    sess = session(settings, args.registry_schema)
    registry = RegistryService.from_context(sess, settings)
    probe_exec = Counter(get_databricks_client(sess, settings))
    probe = WarehouseSourceProbe(probe_exec)
    schema = f"ontobricks_evo_tpch_{args.tier.lower()}"
    doc = tpch_document(CATALOG, schema, args.variant)
    doc["info"]["name"] = f"EvoTpch{args.tier}{args.variant.title()}"
    folder = sanitize_domain_folder(doc["info"]["name"])
    ok, msg = registry.store.write_version(folder, "1", doc)
    assert ok, msg
    svc = EvolutionService(registry, folder)
    qs = questions(params(probe, schema))
    out = {
        "tier": args.tier,
        "variant": args.variant,
        "folder": folder,
        "target": args.target,
    }
    run = svc.create_run(
        "1",
        qs[args.target],
        question=args.target,
        owner="live",
        idempotency_key=uuid.uuid4().hex,
    )
    pipe = EvolutionPipeline(svc, probe)
    t0, q0 = time.time(), probe_exec.n
    run = pipe.assess(run["id"], run["revision"])
    cov = run["coverage"]
    out["coverage"] = {
        "seconds": round(time.time() - t0, 1),
        "queries": probe_exec.n - q0,
        "ready": cov["ready"],
        "decision_hint": cov["decision_hint"],
        "gap": cov.get("gap"),
        "not_resolved": {k: v for k, v in cov["states"].items() if v != "RESOLVED"},
        "join_chosen": {
            k: (cov.get("join") or {}).get("chosen", {}).get(k)
            for k in ("source_column", "target_column", "evidence")
        },
        "join_candidates": len((cov.get("join") or {}).get("candidates") or []),
    }
    if cov["decision_hint"] != "PROPOSE":
        return out
    t0, q0 = time.time(), probe_exec.n
    try:
        run = pipe.propose(run["id"], run["revision"])
    except OntoBricksError as exc:
        out["proposal"] = {"error": exc.message}
        return out
    val = run["proposal"]["validation"]
    ops = run["proposal"]["alternatives"][0]["operations"]
    out["proposal"] = {
        "seconds": round(time.time() - t0, 1),
        "queries": probe_exec.n - q0,
        "passed": val["passed"],
        "failures": val["failures"],
        "mapping_sql": next(
            (
                o["params"].get("sql_query")
                for o in ops
                if o["op"] == "set_relationship_mapping"
            ),
            None,
        ),
    }
    if not val["passed"] or args.no_evaluate:
        return out
    builder = CandidateBuilder(settings, sess.settings)
    evaluator = EvaluationService(svc, builder, probe, settings=settings)
    body = {
        "gold_table": f"{CATALOG}.{schema}.gold_answers",
        "target_question_id": args.target,
        "checks": [{"question_id": q, "intent": qs[q]} for q in args.checks],
    }
    t0, q0 = time.time(), probe_exec.n
    run = evaluator.prepare(run["id"], run["revision"], body)
    run = evaluator.execute(run["id"])
    ev = run["evaluation"]
    out["evaluation"] = {
        "seconds": round(time.time() - t0, 1),
        "probe_queries": probe_exec.n - q0,
        "decision": ev.get("decision"),
        "reasons": ev.get("reasons"),
        "error": ev.get("error"),
        "builds": [(b["arm"], b["triple_count"]) for b in ev.get("builds") or []],
        "questions": [
            {
                "id": q["question_id"],
                "outcome": q["outcome"],
                "expected": len(q["expected_ids"] or []),
                "parent": [
                    q["parent"]["status"],
                    len(q["parent"]["ids"]),
                    q["parent"]["truncated"],
                ],
                "candidate": [
                    q["candidate"]["status"],
                    len(q["candidate"]["ids"]),
                    q["candidate"]["truncated"],
                ],
            }
            for q in ev.get("questions") or []
        ],
    }
    if not args.keep_tables:
        out["dropped"] = len(
            builder.drop(sess, (run.get("manifest") or {}).get("builds") or [])
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default="S")
    ap.add_argument("--variant", default="base")
    ap.add_argument("--target", default="orders_by_supplier")
    ap.add_argument("--checks", nargs="*", default=[])
    ap.add_argument("--registry-schema", default="ontobricks_evo_registry")
    ap.add_argument("--no-evaluate", action="store_true")
    ap.add_argument("--keep-tables", action="store_true")
    args = ap.parse_args()
    print(json.dumps(scenario(args), indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
