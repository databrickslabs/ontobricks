"""Round-trip parity test for the Neo4j typed-node model.

The safety net for the whole PR: write a set of SPO triples through the typed
property-graph write path, read them back through the reconstruction read path,
and assert the triple set is identical. If this holds, the KG view / GraphQL /
reasoning layers (which all consume the SPO contract) see no behavioural change.

Uses an in-memory fake that interprets the specific Cypher our WriteOps/ReadOps
emit — enough to model nodes (uri-keyed), labels, properties, relationships, and
the :__GraphSchema node — without a live Neo4j.
"""

import re
from typing import Any, Dict, List

import pytest

from back.core.graphdb.constants import RDF_TYPE, RDFS_LABEL

pytestmark = pytest.mark.unit

NS = "https://databricks-ontology.com/InsurBricks/"


class FakeGraph:
    """Minimal in-memory graph that answers the Cypher WriteOps/ReadOps emit.

    Not a general Cypher engine — it pattern-matches the exact statements this
    backend produces. Nodes are dicts keyed by uri with ``labels`` (set) and
    ``props`` (dict); relationships are (subj_uri, reltype, obj_uri) tuples;
    the schema node is stored separately.
    """

    def __init__(self) -> None:
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.rels: set = set()
        self.schema: Dict[str, Dict[str, str]] = {}
        self.constraints: set = set()

    # The connection interface used by the ops: run(cypher, **params) -> list[dict]
    def run(self, cypher: str, **params: Any) -> List[Dict[str, Any]]:
        c = " ".join(cypher.split())  # normalise whitespace

        # ---- schema map load ----
        if c.startswith("MATCH (s:`__GraphSchema` {graph: $g}) RETURN"):
            g = params["g"]
            s = self.schema.get(g)
            if not s:
                return []
            return [{"label_map": s.get("label_map"), "reltype_map": s.get("reltype_map"), "prop_map": s.get("prop_map")}]
        # ---- schema map save ----
        if c.startswith("MERGE (s:`__GraphSchema` {graph: $g}) SET"):
            self.schema[params["g"]] = {
                "label_map": params["lm"],
                "reltype_map": params["rm"],
                "prop_map": params["pm"],
            }
            return []

        # ---- node MERGE + SET props + optional labels ----
        m = re.match(r"UNWIND \$rows AS r MERGE \(n:`([^`]+)` \{uri: r\.uri\}\) SET n \+= r\.props(.*)", c)
        if m:
            marker = m.group(1)               # MERGE label applies to the node (real Neo4j semantics)
            extra = m.group(2)
            set_labels = re.findall(r":`([^`]+)`", extra)
            for row in params["rows"]:
                node = self.nodes.setdefault(row["uri"], {"uri": row["uri"], "labels": set(), "props": {}})
                node["props"].update(row.get("props") or {})
                node["labels"].add(marker)
                node["labels"].update(set_labels)
            return []

        # ---- relationship MERGE (batched: one UNWIND per reltype) ----
        m = re.match(
            r"UNWIND \$rows AS r MATCH \(s:`[^`]+` \{uri: r\.s\}\), \(o:`[^`]+` \{uri: r\.o\}\) "
            r"MERGE \(s\)-\[:`([^`]+)`\]->\(o\)",
            c,
        )
        if m:
            reltype = m.group(1)
            for row in params["rows"]:
                self.rels.add((row["s"], reltype, row["o"]))
            return []

        # ---- constraint create ----
        if c.startswith("CREATE CONSTRAINT"):
            m = re.search(r"CREATE CONSTRAINT (\w+)", c)
            if m:
                self.constraints.add(m.group(1))
            return []

        # ---- read: nodes with labels + props ----
        m = re.match(r"MATCH \(n:`([^`]+)`\) (WHERE n\.uri IN \$uris )?RETURN n\.uri AS uri, labels\(n\) AS labels, properties\(n\) AS props", c)
        if m:
            marker = m.group(1)
            uris = params.get("uris")
            out = []
            for node in self.nodes.values():
                if marker not in node["labels"]:
                    continue
                if uris is not None and node["uri"] not in uris:
                    continue
                out.append({
                    "uri": node["uri"],
                    "labels": list(node["labels"]),
                    "props": dict(node["props"]),
                })
            return out

        # ---- read: outgoing relationships ----
        m = re.match(r"MATCH \(n:`([^`]+)`\)-\[rel\]->\(m:`[^`]+`\) (WHERE n\.uri IN \$uris )?RETURN n\.uri AS subject, type\(rel\) AS reltype, m\.uri AS object", c)
        if m:
            uris = params.get("uris")
            out = []
            for (s, rt, o) in self.rels:
                if uris is not None and s not in uris:
                    continue
                out.append({"subject": s, "reltype": rt, "object": o})
            return out

        # ---- count_triples: node aggregate (type-labels + props incl. name) ----
        if c.startswith("MATCH (n:`") and "sum(size(classes)) AS type_triples" in c:
            marker = params["marker"]
            schema = params["schema"]
            type_triples = 0
            prop_triples = 0
            for node in self.nodes.values():
                if marker not in node["labels"]:
                    continue
                type_triples += len(
                    [x for x in node["labels"] if x not in (marker, schema)]
                )
                prop_triples += len([k for k in node["props"] if k != "uri"])
            return [
                {
                    "type_triples": type_triples,
                    "prop_triples": prop_triples,
                    "nodes": sum(1 for n in self.nodes.values() if marker in n["labels"]),
                }
            ]

        # ---- count_triples: relationship count ----
        if re.match(r"MATCH \(:`([^`]+)`\)-\[r\]->\(:`[^`]+`\) RETURN count\(r\) AS rels", c):
            return [{"rels": len(self.rels)}]

        raise AssertionError(f"FakeGraph got unhandled Cypher: {c[:200]}")


def _insurbricks_triples() -> List[Dict[str, str]]:
    cust = f"{NS}Customer/CUST-1007"
    pol = f"{NS}Policy/POL-20008"
    return [
        {"subject": cust, "predicate": RDF_TYPE, "object": f"{NS}Customer"},
        {"subject": cust, "predicate": RDFS_LABEL, "object": "Marie Lefebvre"},
        {"subject": cust, "predicate": f"{NS}city", "object": "Paris"},
        {"subject": cust, "predicate": f"{NS}country", "object": "FR"},
        {"subject": cust, "predicate": f"{NS}holds", "object": pol},
        {"subject": pol, "predicate": RDF_TYPE, "object": f"{NS}Policy"},
        {"subject": pol, "predicate": RDFS_LABEL, "object": "POL-20008"},
        {"subject": pol, "predicate": f"{NS}line", "object": "MOTOR"},
    ]


def _norm(triples: List[Dict[str, str]]) -> set:
    return {(t["subject"], t["predicate"], t["object"]) for t in triples}


class TestRoundTripParity:
    def test_write_then_read_reproduces_triples(self):
        from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        fake = FakeGraph()
        w = Neo4jWriteOps(fake)   # ops call conn.run — FakeGraph.run satisfies it
        r = Neo4jReadOps(fake)

        original = _insurbricks_triples()
        w.create_table("InsurBricks_V1")
        w.insert_triples("InsurBricks_V1", original)

        read_back = r.query_triples("InsurBricks_V1")

        assert _norm(read_back) == _norm(original)

    def test_count_matches_triple_count(self):
        from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        fake = FakeGraph()
        w = Neo4jWriteOps(fake)
        r = Neo4jReadOps(fake)
        original = _insurbricks_triples()
        w.insert_triples("InsurBricks_V1", original)
        # Both the reconstructed triple list AND the aggregate count_triples
        # (type-labels + literal props incl. name + rels) must equal the input
        # length — R5: verify count math matches the round-trip, not just each other.
        assert len(r.query_triples("InsurBricks_V1")) == len(original)
        assert r.count_triples("InsurBricks_V1") == len(original)

    def test_get_triples_for_subjects_scopes_correctly(self):
        from back.core.graphdb.neo4j.Neo4jWriteOps import Neo4jWriteOps
        from back.core.graphdb.neo4j.Neo4jReadOps import Neo4jReadOps

        fake = FakeGraph()
        w = Neo4jWriteOps(fake)
        r = Neo4jReadOps(fake)
        w.insert_triples("InsurBricks_V1", _insurbricks_triples())

        cust = f"{NS}Customer/CUST-1007"
        subj_triples = r.get_triples_for_subjects("InsurBricks_V1", [cust])
        # all returned triples must have the customer as subject
        assert subj_triples
        assert all(t["subject"] == cust for t in subj_triples)
        # includes the holds edge, the type, the name, and the literals
        preds = {t["predicate"] for t in subj_triples}
        assert f"{NS}holds" in preds
        assert RDF_TYPE in preds
        assert RDFS_LABEL in preds
        assert f"{NS}city" in preds
