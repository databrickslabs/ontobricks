"""Smoke test: PGE pipeline on CDM V1.1 maternity ontology.

Runs the new Planner/Generator/Evaluator pipeline against the live
fe-vm-fiifi-cdm-demo workspace. Compares per-item PASS/FAIL to the
V1.1 baseline (17 entities + 18 relationships already in registry).

Usage from repo root with env vars set:

    .venv/bin/python scripts/smoke_pge.py [--items N] [--no-critic]

--items N         restrict to the first N entities (default: all 17, plus
                  all relationships whose endpoints are mapped)
--no-critic       skip the semantic critic stage 2 (faster, cheaper)
--scope=entities  only run entities (skip relationships)
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

# Make ``src/`` importable without a packaged install.
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

# Route the OntoBricks loggers (which use back.core.logging.get_logger) to
# stdout at INFO so per-iteration agent traces appear in the smoke output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(name)s | %(message)s",
    stream=sys.stdout,
)

from agents.agent_mapping_pge.engine import run_agent  # noqa: E402
from back.core.databricks.DatabricksClient import DatabricksClient  # noqa: E402

REGISTRY_JSON = "/tmp/V1_1.json"
LLM_ENDPOINT = "databricks-claude-opus-4-7"


def load_v1_1():
    with open(REGISTRY_JSON) as f:
        doc = json.load(f)
    v = doc["versions"]["1_1"]
    return v["ontology"], v["metadata"], v["assignment"]


def to_agent_shape(ontology):
    """Convert V1.1 ontology (classes + properties) to the agent's expected
    {entities, relationships} shape.
    """
    classes = ontology.get("classes", [])
    properties = ontology.get("properties", [])

    name_to_uri = {c["name"]: c["uri"] for c in classes if c.get("uri")}

    def resolve(short_or_uri):
        if not short_or_uri:
            return short_or_uri
        if short_or_uri.startswith("http"):
            return short_or_uri
        return name_to_uri.get(short_or_uri, short_or_uri)

    entities = []
    for c in classes:
        entities.append(
            {
                "uri": c.get("uri", ""),
                "name": c.get("name", ""),
                "label": c.get("label", ""),
                "comment": c.get("comment", ""),
                "parent": c.get("parent", ""),
                "attributes": list(c.get("dataProperties", [])),
            }
        )

    relationships = []
    for p in properties:
        if p.get("type") != "ObjectProperty":
            continue
        relationships.append(
            {
                "uri": p.get("uri", ""),
                "name": p.get("name", ""),
                "label": p.get("label", p.get("name", "")),
                "comment": p.get("comment", ""),
                "domain": resolve(p.get("domain", "")),
                "range": resolve(p.get("range", "")),
            }
        )
    return {"entities": entities, "relationships": relationships}


def filter_agent_ontology(agent_ont, item_limit, scope):
    entities = agent_ont["entities"]
    relationships = agent_ont["relationships"]
    if item_limit is not None:
        entities = entities[:item_limit]
    kept_uris = {e["uri"] for e in entities}
    if scope == "entities":
        relationships = []
    else:
        relationships = [
            r for r in relationships
            if r["domain"] in kept_uris and r["range"] in kept_uris
        ]
    return {"entities": entities, "relationships": relationships}


def on_step(msg, pct):
    print(f"  [{pct:3d}%] {msg}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--items", type=int, default=None, help="Cap entity count")
    parser.add_argument("--no-critic", action="store_true", help="Skip semantic critic")
    parser.add_argument(
        "--scope", choices=["all", "entities"], default="all",
    )
    args = parser.parse_args()

    print(f"=== PGE smoke test — endpoint={LLM_ENDPOINT} ===")
    print(f"items={args.items}, no-critic={args.no_critic}, scope={args.scope}")
    print()

    print("Loading V1.1 ontology…")
    raw_ont, metadata, baseline = load_v1_1()
    agent_ont = to_agent_shape(raw_ont)
    ontology = filter_agent_ontology(agent_ont, args.items, args.scope)
    print(f"  ontology: {len(ontology['entities'])} entities, {len(ontology['relationships'])} relationships")
    print(f"  metadata: {len(metadata.get('tables', []))} tables")
    print(f"  baseline: {len(baseline.get('entities', []))} entity mappings + "
          f"{len(baseline.get('relationships', []))} relationships")
    print()

    client = DatabricksClient()
    print(f"DatabricksClient: host={client.host}, warehouse={client.warehouse_id}")
    print()

    print("Invoking run_agent…")
    t0 = time.time()
    result = run_agent(
        host=client.host,
        token=client.token,
        endpoint_name=LLM_ENDPOINT,
        client=client,
        metadata=metadata,
        ontology=ontology,
        documents=[],
        on_step=on_step,
        skip_semantic_critic=args.no_critic,
    )
    elapsed = time.time() - t0
    print()
    print(f"=== Run finished in {elapsed:.1f}s ===")
    print(f"success={result.success}, iterations={result.iterations}, error={result.error!r}")
    print(f"usage={result.usage}")
    print()

    print("Per-item run log:")
    for entry in result.mapping_run_log:
        attempts = len(entry.get("attempts", []))
        print(f"  {entry['kind']:<13}  {entry['item']:<60}  "
              f"attempts={attempts}  final={entry['final_status']}")
    print()

    print(f"entity_mappings: {len(result.entity_mappings)} / {len(ontology['entities'])} "
          f"(baseline {len(baseline.get('entities', []))})")
    print(f"relationship_mappings: {len(result.relationship_mappings)} / "
          f"{len(ontology['relationships'])} (baseline {len(baseline.get('relationships', []))})")

    # Dump the full result for inspection
    out = {
        "success": result.success,
        "iterations": result.iterations,
        "error": result.error,
        "usage": result.usage,
        "stats": result.stats,
        "entity_mappings": result.entity_mappings,
        "relationship_mappings": result.relationship_mappings,
        "source_model": result.source_model,
        "mapping_evaluations": result.mapping_evaluations,
        "mapping_run_log": result.mapping_run_log,
        "steps": [{"step_type": s.step_type, "tool_name": s.tool_name, "duration_ms": s.duration_ms} for s in result.steps],
    }
    out_path = REPO_ROOT / "logs" / f"smoke_pge_{int(t0)}.json"
    out_path.parent.mkdir(exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\nFull result written to {out_path}")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
