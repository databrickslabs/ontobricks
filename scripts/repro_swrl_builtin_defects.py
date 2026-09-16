"""Standalone repro harness for the OntoBricks SWRL attribute-rule defect.

Usage:
  python swrl_repro.py --src <repo>/src --table <schema.table> --expected N [--fixture]

Runs the editor-generated rule
    Sailing(?x) ^ nights(?x, ?nights) ^ swrlb:greaterThanOrEqual(?nights, 10)
      -> LongSailing(?x)
through SWRLEngine imported from --src, against a Postgres triple table.
--fixture (re)creates a minimal 3-sailing dataset (nights 5/10/15) whose
correct answer is 2. Prints the generated SQL and the inferred count.
"""

import argparse
import re
import sys

import psycopg
from psycopg.rows import dict_row

PG = "host=localhost port=5433 dbname=ontobricks_registry user=genai_user password=changeme"
BASE = "https://databricks-ontology.com/PCP#"
DATA = "https://databricks-ontology.com/PCP/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

RULE = {
    "name": "LongSailingRule",
    "antecedent": "Sailing(?x) ∧ nights(?x, ?nights) ∧ swrlb:greaterThanOrEqual(?nights, 10)",
    "consequent": "LongSailing(?x)",
    "enabled": True,
}

ONTOLOGY = {
    "base_uri": BASE,
    "classes": [
        {
            "name": "Sailing",
            "uri": BASE + "Sailing",
            "dataProperties": [{"name": "nights", "uri": BASE + "nights"}],
        },
        {"name": "LongSailing", "uri": BASE + "LongSailing", "dataProperties": []},
    ],
    "properties": [],
}


class PgStore:
    """Minimal store: just enough surface for SWRLEngine."""

    def __init__(self, conn):
        self._conn = conn

    def sql_table_reference(self, table_name):
        return table_name

    def execute_query(self, sql):
        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            return cur.fetchall()


def make_fixture(conn):
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS swrl_repro CASCADE")
        cur.execute("CREATE SCHEMA swrl_repro")
        cur.execute(
            "CREATE TABLE swrl_repro.triples (subject text, predicate text, object text)"
        )
        rows = []
        for sid, nights in (("S1", "5"), ("S2", "10"), ("S3", "15")):
            subj = f"{BASE}Sailing/{sid}"
            rows.append((subj, RDF_TYPE, BASE + "Sailing"))
            rows.append((subj, DATA + "nights", nights))
        cur.executemany(
            "INSERT INTO swrl_repro.triples VALUES (%s, %s, %s)", rows
        )
    conn.commit()


def validate_rule_is_safe(rule):
    body_vars = set(re.findall(r"\?\w+", rule["antecedent"]))
    head_vars = set(re.findall(r"\?\w+", rule["consequent"]))
    assert head_vars <= body_vars, f"unsafe rule: unbound head vars {head_vars - body_vars}"
    return sorted(body_vars), sorted(head_vars)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--fixture", action="store_true")
    args = ap.parse_args()

    sys.path.insert(0, args.src)
    from back.core.reasoning.SWRLEngine import SWRLEngine
    from back.core.reasoning.SWRLSQLTranslator import SWRLSQLTranslator

    # Harness scaffolding: the stub store is not a GraphDBBackend, so mimic
    # what a (fixed) Lakebase backend returns — a Postgres-dialect translator.
    # On pristine code SWRLSQLTranslator() ignores the kwargs-free init and
    # behaves identically, keeping the red leg honest.
    try:
        translator_for_store = SWRLSQLTranslator(dialect="postgres")
    except TypeError:  # pristine signature has no dialect
        translator_for_store = SWRLSQLTranslator()
    SWRLEngine._get_translator = staticmethod(
        lambda store, table_name="": translator_for_store
    )

    body, head = validate_rule_is_safe(RULE)
    print(f"rule is safe SWRL: body vars {body}, head vars {head}")

    conn = psycopg.connect(PG)
    if args.fixture:
        make_fixture(conn)
        print("fixture created: 3 sailings, nights 5/10/15")

    store = PgStore(conn)
    engine = SWRLEngine(ONTOLOGY)

    translator = translator_for_store
    params = {
        "antecedent": RULE["antecedent"],
        "consequent": RULE["consequent"],
        "base_uri": BASE,
        "uri_map": engine._build_uri_map(),
    }
    print("--- generated SQL ---")
    print(translator.build_inference_sql(store.sql_table_reference(args.table), params))
    print("---------------------")

    result = engine.execute_rules([RULE], store, args.table)
    got = len(result.inferred_triples)
    verdict = "PASS" if got == args.expected else "FAIL"
    print(f"inferred: {got}  expected: {args.expected}  -> {verdict}")
    for t in result.inferred_triples[:5]:
        print("  ", t.subject, "->", t.object)


if __name__ == "__main__":
    main()
