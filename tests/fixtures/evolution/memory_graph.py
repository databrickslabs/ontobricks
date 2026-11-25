"""In-memory triple store for one Evolve arm, built from the fixture rows.

Mirrors the R2RML the Delta build generates for the fixture: subjects
``<base>/<Class>/<id>``, ``rdf:type`` the class URI, data and object
predicates ``<base>/<name>`` (the base URI with ``#`` normalised to ``/``),
so the compiled path SQL runs against it exactly as it does against a
built ``…_graph`` view.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

from back.objects.ontology.evolution import ArmSqlQuery, OntologyIndex
from tests.fixtures.evolution.memory_probe import MemorySourceProbe, table_rows

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"


def _norm(base: str) -> str:
    return base.rstrip("/").rstrip("#") + "/"


def sqlite_execute(triples: List[Dict[str, str]]):
    """``execute`` over an in-memory SQLite ``t(subject, predicate, object)``.

    The compiled path SQL is portable, so the unit tests run the exact
    statements the warehouse would.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (subject TEXT, predicate TEXT, object TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [(x["subject"], x["predicate"], x["object"]) for x in triples],
    )
    conn.row_factory = sqlite3.Row

    def execute(sql: str):
        return [dict(r) for r in conn.execute(sql).fetchall()]

    return execute


def _failing(sql: str):
    raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] arm graph")


def arm_triples(
    domain: Any, catalog: str = "cat", schema: str = "fix"
) -> List[Dict[str, str]]:
    ontology, assignment = domain.ontology, domain.assignment
    base = _norm(ontology["base_uri"])
    index = OntologyIndex(ontology, assignment)
    prefix = f"{catalog}.{schema}."
    triples: List[Dict[str, str]] = []
    for name, cls in index.classes.items():
        entity = index.entity(name)
        if not entity or not entity["table"].startswith(prefix):
            continue
        for row in table_rows(entity["table"][len(prefix) :]):
            subject = f"{base}{name}/{row[entity['id_column']]}"
            triples.append(
                {"subject": subject, "predicate": RDF_TYPE, "object": cls["uri"]}
            )
            label = entity["label_column"]
            if label and row.get(label) is not None:
                triples.append(
                    {
                        "subject": subject,
                        "predicate": RDFS_LABEL,
                        "object": str(row[label]),
                    }
                )
            for attr, column in entity["attributes"].items():
                # Like the Delta build: a label-column attribute is only rdfs:label.
                if column != label and row.get(column) is not None:
                    triples.append(
                        {
                            "subject": subject,
                            "predicate": base + attr,
                            "object": str(row[column]),
                        }
                    )
    probe = MemorySourceProbe(catalog, schema)
    for rel_name, rel in index.relationships.items():
        mapping = index.relationship_mapping(rel_name)
        if not mapping:
            continue
        for src, tgt in probe.run_pairs(mapping["sql_query"]):
            triples.append(
                {
                    "subject": f"{base}{rel['domain']}/{src}",
                    "predicate": base + rel_name,
                    "object": f"{base}{rel['range']}/{tgt}",
                }
            )
    return triples


def memory_query_factory(fail_arms=()):
    """``query_factory`` for EvaluationService: compiled SQL over in-memory triples."""

    def factory(domain):
        arm = "parent" if domain.info["name"].endswith("p") else "candidate"
        execute = _failing if arm in fail_arms else sqlite_execute(arm_triples(domain))
        return ArmSqlQuery(domain, None, execute=execute, table="t")

    return factory
