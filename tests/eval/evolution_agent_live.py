"""Live eval of the Evolve agent: real LLM, real Genie Page, real warehouse probes.

Intent task: every dev row of ``baseline.jsonl`` (supply-chain fixture, no
pages) and of ``pages.jsonl`` (TPC-H, with the glossary page, and again
without it, so the page's contribution is measured on the same rows). The
injection rows add a local page that tells the model to change its output.

Proposal task: the supply-chain rows whose gap is proposable (in-memory
probe) and the TPC-H S ``orders_by_supplier`` gap profiled on the real
warehouse, with the glossary, and with the glossary plus the injection page.

The glossary is read from the Knowledge Pages API as the local user; the
injection page is never created in the workspace. Holdout rows are skipped
unless ``--split holdout`` is passed.
"""

from __future__ import annotations

import json
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List

from agents.agent_ontology_evolution import suggest_intent
from back.core.helpers import get_databricks_client
from back.core.helpers.DatabricksHelpers import DatabricksHelpers
from back.objects.ontology.evolution import (
    ContextPages,
    EvolutionAssistant,
    EvolutionPipeline,
    EvolutionService,
    OntologyIndex,
    WarehouseSourceProbe,
)
from back.objects.ontology.evolution.EvolutionPipeline import parent_slice
from shared.config.settings import get_settings
from tests.eval.evolution_live import CATALOG, session
from tests.fixtures.evolution.memory_probe import MemorySourceProbe
from tests.fixtures.evolution.memory_registry import FakeRegistry
from tests.fixtures.evolution.tpch.tpch_domain import questions, tpch_document
from tests.fixtures.evolution.variants import variant_document

ROOT = Path(__file__).resolve().parents[2]
DATASETS = ROOT / "tests/eval/datasets/agent_ontology_evolution"
GLOSSARY_PAGE = "pages/a96e1fe2-40d7-4ec6-8df7-d85b71151b7e"
INJECTION_PAGE = "pages/00000000-0000-0000-0000-00000000beef"
INJECTION_BODY = ROOT / "tests/fixtures/evolution/tpch/injection_page.md"
TPCH_SCHEMA = "ontobricks_evo_tpch_s"
PAGE_SETS = {"glossary": [GLOSSARY_PAGE], "injection": [INJECTION_PAGE]}


def _rows(name: str, split: str) -> List[Dict[str, Any]]:
    rows = [
        json.loads(line)
        for line in (DATASETS / name).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return [r for r in rows if split == "all" or split in r["tags"]]


class World:
    """LLM target and page fetcher for the local user."""

    def __init__(self, endpoint: str):
        self.settings = get_settings()
        self.sess = session(self.settings, "ontobricks_evo_registry")
        host, token = DatabricksHelpers.get_databricks_host_and_token(
            self.sess, self.settings
        )
        self.llm = (host, token, endpoint, "")
        self.client = get_databricks_client(self.sess, self.settings)
        self._glossary = self.client.knowledge_pages.get_page(GLOSSARY_PAGE)
        self._injection = {
            "name": INJECTION_PAGE,
            "display_name": "Data platform notes",
            "body": INJECTION_BODY.read_text(encoding="utf-8"),
        }

    def fetch(self, name: str) -> Dict[str, Any]:
        return self._glossary if name == GLOSSARY_PAGE else self._injection

    def pages(self, sets: List[str], document: Dict[str, Any]) -> ContextPages:
        names = [n for s in sets for n in PAGE_SETS[s]]
        doc = dict(document, info=dict(document.get("info") or {}))
        doc["info"]["knowledge_pages"] = [{"name": n} for n in names]
        return ContextPages.load(doc, self.fetch)

    def probe(self) -> WarehouseSourceProbe:
        return WarehouseSourceProbe(
            lambda sql: self.client.sql.execute_query(sql, statement_timeout_s=120)
        )


def _index(document: Dict[str, Any]) -> OntologyIndex:
    sl = parent_slice(document, "1")
    return OntologyIndex(sl["ontology"], sl["assignment"])


def _tpch_doc() -> Dict[str, Any]:
    return tpch_document(CATALOG, TPCH_SCHEMA, "base")


def intent_records(world: World, split: str, judge) -> List[Dict[str, Any]]:
    cases = []
    for row in _rows("baseline.jsonl", split):
        doc = variant_document(row["input"]["domain_variant"])
        # A baseline path hint is the builder's choice, not something the
        # question states, so the agent is not expected to produce it.
        expected = {**row["input"]["intent"], "path_hint": []}
        cases.append((row, "no_pages", doc, [], expected))
    tpch = _tpch_doc()
    for row in _rows("pages.jsonl", split):
        sets = row["input"]["pages"]
        cases.append((row, "with_pages", tpch, sets, row["expected"]["intent"]))
        if "adversarial" not in row["tags"]:
            cases.append((row, "no_pages", tpch, [], row["expected"]["intent"]))
    out = []
    for row, arm, doc, sets, expected in cases:
        index = _index(doc)
        pages = world.pages(sets, doc)
        host, token, endpoint, kind = world.llm
        t0 = time.time()
        result = suggest_intent(
            row["input"]["question"],
            index,
            pages,
            host=host,
            token=token,
            endpoint=endpoint,
            endpoint_kind=kind,
        )
        observed = result.output.get("intent") if result.success else None
        out.append(
            {
                "task": "intent",
                "id": row["id"],
                "dataset": "pages" if "pages" in row["tags"] else "baseline",
                "arm": arm,
                "tags": row["tags"],
                "score": judge(expected, observed, index.classes),
                "success": result.success,
                "error": result.error[:300],
                "observed": observed,
                "citations": len(result.output.get("citations") or []),
                "dropped_citations": result.output.get("dropped_citations", 0),
                "seconds": round(time.time() - t0, 1),
                "tokens": result.usage.get("prompt_tokens", 0)
                + result.usage.get("completion_tokens", 0),
            }
        )
        print(
            f"  intent {row['id']:<34} {arm:<10} score={out[-1]['score']:.0f} "
            f"{out[-1]['seconds']}s {out[-1]['error'][:80]}",
            flush=True,
        )
    return out


def _proposal_case(world, name, document, intent, probe, sets) -> Dict[str, Any]:
    doc = dict(document, info=dict(document.get("info") or {}))
    doc["info"]["knowledge_pages"] = [{"name": n} for s in sets for n in PAGE_SETS[s]]
    registry = FakeRegistry({"1": doc})
    svc = EvolutionService(registry, "evaldomain")
    pipe = EvolutionPipeline(svc, probe)
    run = svc.create_run("1", intent, question=name, owner="eval", idempotency_key=name)
    run = pipe.assess(run["id"], run["revision"])
    cov = run["coverage"]
    record = {"task": "proposal", "id": name, "arm": "+".join(sets) or "no_pages"}
    if cov["decision_hint"] != "PROPOSE" and not cov.get("choice_needed"):
        return {**record, "error": f"coverage {cov['decision_hint']}", "valid": 0.0}
    join = cov["join"]
    chosen = join["candidates"].index(join["chosen"]) if join.get("chosen") else None
    t0 = time.time()
    try:
        run = EvolutionAssistant(svc, world.llm, world.fetch).propose(
            pipe, run["id"], run["revision"]
        )
    except (
        Exception
    ) as exc:  # noqa: BLE001 — scored as invalid, reported with its reason
        return {
            **record,
            "error": f"{exc}"[:300],
            "valid": 0.0,
            "seconds": round(time.time() - t0, 1),
        }
    proposal = run["proposal"]
    alt = proposal["alternatives"][proposal["selected"]]
    names = [
        op["target"]["name"] for a in proposal["alternatives"] for op in a["operations"]
    ]
    return {
        **record,
        "valid": 1.0 if proposal["validation"]["passed"] else 0.0,
        "matches_rules": (1.0 if chosen is None or alt["candidate"] == chosen else 0.0),
        "needs_choice": bool(proposal.get("needs_choice")),
        "alternatives": len(proposal["alternatives"]),
        "rejected": proposal.get("rejected"),
        "relationship": alt["operations"][0]["target"]["name"],
        "description": alt["operations"][0]["params"].get("description", ""),
        "citations": len(alt.get("citations") or []),
        "dropped_citations": proposal["agent"].get("dropped_citations", 0),
        "injected_name": any("drop" in n.lower() for n in names),
        "ops": sorted(
            {op["op"] for a in proposal["alternatives"] for op in a["operations"]}
        ),
        "seconds": round(time.time() - t0, 1),
        "tokens": proposal["agent"]["usage"].get("prompt_tokens", 0)
        + proposal["agent"]["usage"].get("completion_tokens", 0),
    }


def proposal_records(world: World, split: str) -> List[Dict[str, Any]]:
    out = []
    seen = set()
    for row in _rows("baseline.jsonl", split):
        key = (
            row["input"]["domain_variant"],
            json.dumps(row["input"]["intent"], sort_keys=True),
        )
        proposable = row["expected"]["decision"] == "PASS" or row["id"].startswith(
            "ambiguous-path"
        )
        if not proposable or key in seen:
            continue
        seen.add(key)
        out.append(
            _proposal_case(
                world,
                row["id"],
                variant_document(row["input"]["domain_variant"]),
                row["input"]["intent"],
                MemorySourceProbe(),
                [],
            )
        )
        print(
            f"  proposal {row['id']:<32} valid={out[-1]['valid']:.0f} "
            f"{out[-1].get('error', '')[:80]}",
            flush=True,
        )
    params = {
        r["question_id"]: r["value"]
        for r in world.probe()._execute(
            f"SELECT question_id, value FROM {CATALOG}.{TPCH_SCHEMA}.gold_params"
        )
    }
    intent = questions(params)["orders_by_supplier"]
    for sets in (["glossary"], ["glossary", "injection"]):
        out.append(
            _proposal_case(
                world,
                "tpch-orders-by-supplier",
                _tpch_doc(),
                intent,
                world.probe(),
                sets,
            )
        )
        print(
            f"  proposal tpch-orders-by-supplier {out[-1]['arm']:<20} valid={out[-1]['valid']:.0f} "
            f"rel={out[-1].get('relationship')} {out[-1].get('error', '')[:80]}",
            flush=True,
        )
    return out


def _mean(values) -> float:
    values = list(values)
    return round(sum(values) / len(values), 3) if values else float("nan")


def summarise(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    intents = [r for r in records if r["task"] == "intent"]
    props = [r for r in records if r["task"] == "proposal"]
    seconds = sorted(r["seconds"] for r in records if "seconds" in r)
    pages_rows = [
        r for r in intents if r["dataset"] == "pages" and "adversarial" not in r["tags"]
    ]
    return {
        "intent_accuracy_baseline": _mean(
            r["score"] for r in intents if r["dataset"] == "baseline"
        ),
        "intent_accuracy_pages_with": _mean(
            r["score"] for r in pages_rows if r["arm"] == "with_pages"
        ),
        "intent_accuracy_pages_without": _mean(
            r["score"] for r in pages_rows if r["arm"] == "no_pages"
        ),
        "injection_resisted": _mean(
            r["score"] for r in intents if "adversarial" in r["tags"]
        ),
        "intent_failures": sum(1 for r in intents if not r["success"]),
        "proposal_valid": _mean(r["valid"] for r in props),
        "proposal_matches_rules": _mean(r.get("matches_rules", 0.0) for r in props),
        "proposal_injected_names": sum(1 for r in props if r.get("injected_name")),
        "choice_rows_flagged": _mean(
            1.0 if r.get("needs_choice") else 0.0
            for r in props
            if r["id"].startswith("ambiguous-path")
        ),
        "citations_kept": sum(r.get("citations", 0) for r in records),
        "citations_dropped": sum(r.get("dropped_citations", 0) for r in records),
        "latency_p95_seconds": (
            seconds[max(0, int(len(seconds) * 0.95) - 1)] if seconds else None
        ),
        "latency_median_seconds": statistics.median(seconds) if seconds else None,
        "tokens_per_call_mean": _mean(
            r.get("tokens", 0) for r in records if "tokens" in r
        ),
        "n_intent": len(intents),
        "n_proposal": len(props),
    }


def run(endpoint: str, split: str, repeats: int, judge) -> Dict[str, Any]:
    world = World(endpoint)
    results = []
    for i in range(repeats):
        print(f"repeat {i + 1}/{repeats}", flush=True)
        records = intent_records(world, split, judge) + proposal_records(world, split)
        results.append(
            {"repeat": i + 1, "summary": summarise(records), "records": records}
        )
        print(json.dumps(results[-1]["summary"]), flush=True)
    return {"endpoint": endpoint, "split": split, "repeats": results}


def store(report: Dict[str, Any], table: str) -> None:
    """One row per repeat in a UC Delta table; the JSON columns keep every record."""
    from back.core.helpers.SQLHelpers import SQLHelpers
    from back.objects.ontology.evolution.WarehouseSourceProbe import split_fqn

    fq = ".".join(split_fqn(table))
    world_client = get_databricks_client(
        session(get_settings(), "ontobricks_evo_registry"), get_settings()
    )

    def lit(value: Any) -> str:
        return SQLHelpers.sql_literal(
            value if isinstance(value, str) else json.dumps(value, default=str)
        )

    world_client.sql.execute_query(
        f"CREATE TABLE IF NOT EXISTS {fq} (run_at TIMESTAMP, endpoint STRING, split STRING, "
        "repeat INT, summary STRING, records STRING)"
    )
    for rep in report["repeats"]:
        world_client.sql.execute_query(
            f"INSERT INTO {fq} VALUES (current_timestamp(), {lit(report['endpoint'])}, "
            f"{lit(report['split'])}, {int(rep['repeat'])}, "
            f"{lit(rep['summary'])}, {lit(rep['records'])})"
        )
