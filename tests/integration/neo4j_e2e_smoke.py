"""End-to-end smoke test that exercises the actual Neo4jStore class
from the feature/neo4j-graphdb-skeleton branch against the live Aura
instance Ryan provisioned.

What this proves (that the UI flow couldn't):
- Bolt connection + basic auth works against neo4j+s://b4810af7...
- create_table emits the SPO uniqueness constraint
- insert_triples writes via UNWIND/MERGE
- count_triples + query_triples + find_subjects_by_type return correct results
- table_exists via SHOW CONSTRAINTS works
- (Optional) drop_table cleans up — we leave the data in for Browser screenshots

Run: python3 neo4j_e2e_smoke.py
"""
import os
import sys
from pathlib import Path

# Add the OntoBricks src/ to PYTHONPATH so we use the EXACT code from the PR
ROOT = Path("/Users/hugues.journeau/Documents/CODE/ontobricks/branches/develop")
sys.path.insert(0, str(ROOT / "src"))

# Aura credentials (gitignored in briefs/)
CREDS_FILE = Path.home() / "Documents/CODE/ontobricks/briefs/2026-05M-12/5/neo4j_connection_details.txt"
creds = {}
for line in CREDS_FILE.read_text().splitlines():
    if "=" in line and not line.startswith("#"):
        k, v = line.split("=", 1)
        creds[k.strip()] = v.strip()

engine_config = {
    "uri": creds["NEO4J_URI"],
    "database": creds["NEO4J_DATABASE"],
    "auth_method": "basic",
    "username": creds["NEO4J_USERNAME"],
    "password": creds["NEO4J_PASSWORD"],
}

# Import the actual production Neo4jStore from the PR branch
from back.core.graphdb.neo4j.Neo4jStore import Neo4jStore  # noqa: E402

store = Neo4jStore(db_name="pr47_smoke_2026_06_09", engine_config=engine_config)

TABLE = "pr47_smoke_2026_06_09"
print(f"=== E2E smoke test against {engine_config['uri']} ===")

# Step 0 — clean any prior run so the test is deterministic
print("\n[0] drop_table() to ensure a clean state")
store.drop_table(TABLE)

# Step 1 — create the SPO uniqueness constraint
print("\n[1] create_table(...) → creates SPO uniqueness constraint")
store.create_table(TABLE)
print(f"     table_exists() → {store.table_exists(TABLE)}")

# Step 2 — insert a small cust360-style triple set using FULL W3C URIs
# (matches what the OntoBricks R2RML pipeline actually emits)
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
EX = "http://example.com/"
triples = [
    {"subject": f"{EX}customer/C1", "predicate": RDF_TYPE,        "object": f"{EX}Customer"},
    {"subject": f"{EX}customer/C1", "predicate": RDFS_LABEL,       "object": "Alice"},
    {"subject": f"{EX}customer/C1", "predicate": f"{EX}livesIn",   "object": f"{EX}city/Paris"},
    {"subject": f"{EX}customer/C1", "predicate": f"{EX}buys",      "object": f"{EX}product/P1"},
    {"subject": f"{EX}customer/C2", "predicate": RDF_TYPE,        "object": f"{EX}Customer"},
    {"subject": f"{EX}customer/C2", "predicate": RDFS_LABEL,       "object": "Bob"},
    {"subject": f"{EX}customer/C2", "predicate": f"{EX}livesIn",   "object": f"{EX}city/Lyon"},
    {"subject": f"{EX}product/P1",  "predicate": RDF_TYPE,        "object": f"{EX}Product"},
    {"subject": f"{EX}product/P1",  "predicate": RDFS_LABEL,       "object": "Widget"},
    {"subject": f"{EX}city/Paris",  "predicate": RDF_TYPE,        "object": f"{EX}City"},
    {"subject": f"{EX}city/Lyon",   "predicate": RDF_TYPE,        "object": f"{EX}City"},
]
print(f"\n[2] insert_triples(n={len(triples)})")
n = store.insert_triples(TABLE, triples)
print(f"     inserted: {n}")

# Step 3 — verify count
count = store.count_triples(TABLE)
print(f"\n[3] count_triples() → {count}")
assert count == len(triples), f"expected {len(triples)}, got {count}"

# Step 4 — read them back
print(f"\n[4] query_triples() — sample of first 3:")
for t in store.query_triples(TABLE)[:3]:
    print(f"     {t}")

# Step 5 — named query: find subjects by type
print(f"\n[5] find_subjects_by_type(rdf:type=http://example.com/Customer)")
subs = store.find_subjects_by_type(TABLE, "http://example.com/Customer", limit=10)
print(f"     → {subs}")
assert "http://example.com/customer/C1" in subs and "http://example.com/customer/C2" in subs

# Step 6 — named query: aggregate stats
print(f"\n[6] get_aggregate_stats()")
stats = store.get_aggregate_stats(TABLE)
for k, v in stats.items():
    print(f"     {k}: {v}")
assert stats["total"] == len(triples)
assert stats["type_assertion_count"] == 5  # 5 rdf:type triples
assert stats["label_count"] == 3            # 3 rdfs:label triples

# Step 7 — get_status
print(f"\n[7] get_status()")
status = store.get_status(TABLE)
print(f"     {status}")
assert status["format"] == "neo4j"

# Step 8 — get_entity_metadata for the two customers
print(f"\n[8] get_entity_metadata(C1, C2)")
md = store.get_entity_metadata(TABLE, ["http://example.com/customer/C1", "http://example.com/customer/C2"])
for r in md:
    print(f"     {r}")

# Step 9 — expand neighbors of C1
print(f"\n[9] expand_entity_neighbors({{C1}}) — typed entities only")
neigh = store.expand_entity_neighbors(TABLE, {"http://example.com/customer/C1"})
print(f"     → {neigh}")

# Step 10 — leave the data in place for Neo4j Browser screenshot
print(f"\n[10] data left in place at label :Triple:{TABLE} for Browser screenshot")
print(f"     Cypher to view: MATCH (t:Triple:{TABLE}) RETURN t")
store.close()
print("\n=== ALL ASSERTIONS PASSED ===")
