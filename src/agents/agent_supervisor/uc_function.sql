-- ============================================================================
-- assess_domain_complexity — deterministic engine-routing UC function
-- ============================================================================
-- Registered as a Unity Catalog function and added to the Agent Bricks
-- Supervisor as a tool. The supervisor calls this FIRST with the domain's
-- source metadata + generated ontology, then routes the mapping task to the
-- PGE or the simple engine per the returned `recommended_engine`.
--
-- This is a self-contained Python mirror of
-- `agents/agent_supervisor/complexity.py`. The weights/thresholds below MUST be
-- kept in sync with that module (the unit test `test_uc_function_parity`
-- guards the shared constants). Self-contained because UC Python functions run
-- sandboxed and cannot import the application package.
--
-- Replace ${CATALOG} / ${SCHEMA} at deploy time (see mas.py).
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ${CATALOG}.${SCHEMA}.assess_domain_complexity(
    metadata_json STRING COMMENT 'Domain source metadata: {"tables":[{"name","columns":[...]}]}',
    ontology_json STRING COMMENT 'Generated ontology: {"classes":[...],"properties":[...]} or agent shape'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Deterministically score a domain''s mapping complexity and recommend the PGE or simple engine. Returns JSON {score, tier, recommended_engine, signals, rationale}.'
AS $$
import json, re

W_TABLES, W_CLASSES, W_RELS, W_CROSS, W_HET = 0.20, 0.20, 0.15, 0.30, 0.15
SAT_TABLES, SAT_CLASSES, SAT_RELS = 5, 12, 10
THRESHOLD = 0.45
ID_RE = re.compile(r"(^|_)(id|no|number|key|code|nhs|mrn|uuid)$")

def _norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())

def _convention(name):
    if not name:
        return "other"
    if "_" in name:
        return "upper_snake" if name.isupper() else "snake"
    if name.isupper():
        return "upper"
    if name[0].islower() and any(c.isupper() for c in name):
        return "camel"
    if name.islower():
        return "lower"
    return "other"

try:
    meta = json.loads(metadata_json) if metadata_json else {}
except Exception:
    meta = {}
try:
    onto = json.loads(ontology_json) if ontology_json else {}
except Exception:
    onto = {}

tables = meta.get("tables") or []
def _cols(t):
    return t.get("columns") or [c.get("name") if isinstance(c, dict) else c for c in (t.get("schema") or [])]

n_tables = len(tables)
n_columns = sum(len(_cols(t) or []) for t in tables)
classes = onto.get("classes") or onto.get("entities") or []
rels = onto.get("properties") or onto.get("relationships") or []
# object properties only, if registry shape mixes data+object properties
n_classes = len(classes)
n_rels = len([r for r in rels if (r.get("type") in (None, "object", "ObjectProperty")) or "range" in r]) if rels and isinstance(rels[0], dict) else len(rels)

# cross-source: an id-like column shared across >=2 tables
id_spread = {}
for t in tables:
    seen = set()
    for c in (_cols(t) or []):
        name = c if isinstance(c, str) else c.get("name", "")
        raw = (name or "").lower()
        key = _norm(name)
        if ID_RE.search(raw) and key and key not in seen:
            id_spread[key] = id_spread.get(key, 0) + 1
            seen.add(key)
shared = [k for k, v in id_spread.items() if v >= 2]
cross = 0.0
if n_tables >= 2 and shared:
    cross = min(0.5 + 0.5 * (max(id_spread[k] for k in shared) / n_tables), 1.0)

# heterogeneity: distinct naming conventions across feeds
convs = set()
for t in tables:
    for c in (_cols(t) or []):
        convs.add(_convention(c if isinstance(c, str) else c.get("name", "")))
convs.discard("other")
het = 0.0 if (n_tables < 2 or not convs) else min((len(convs) - 1) / 3.0, 1.0)

score = (W_TABLES * min(n_tables / SAT_TABLES, 1.0)
         + W_CLASSES * min(n_classes / SAT_CLASSES, 1.0)
         + W_RELS * min(n_rels / SAT_RELS, 1.0)
         + W_CROSS * cross + W_HET * het)
tier = "complex" if score >= THRESHOLD else "simple"
engine = "pge" if tier == "complex" else "simple"
return json.dumps({
    "score": round(score, 4),
    "tier": tier,
    "recommended_engine": engine,
    "signals": {"n_tables": n_tables, "n_columns": n_columns, "n_classes": n_classes,
                "n_relationships": n_rels, "cross_source": round(cross, 4),
                "heterogeneity": round(het, 4)},
    "rationale": f"score {round(score,2)} ({tier}); recommend {engine} engine",
})
$$;
