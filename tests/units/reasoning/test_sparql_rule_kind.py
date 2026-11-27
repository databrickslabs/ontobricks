"""A Complex rule's kind is read from its query: CONSTRUCT infers, DELETE removes.

The editor mirrors the rule in ``_sparqlKindOf`` (JavaScript); the parity test
runs both over the same samples so the label, the Preview box and the engine
can never disagree.
"""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from back.core.reasoning.rules.rule_kind import (
    NOT_A_RULE_MESSAGE,
    RULE_KIND_CONSTRUCT,
    RULE_KIND_REMOVAL,
    sparql_rule_kind,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
BUSINESS_RULES_JS = REPO_ROOT / "src/front/static/ontology/js/ontology-business-rules.js"

PFX = "PREFIX : <http://ex.org/onto#>\n"
SAMPLES = [
    (PFX + "CONSTRUCT { ?c a :VIP } WHERE { ?c a :Customer . ?c :balance ?b }", "construct"),
    ("  construct { ?a a :B } WHERE { ?a a :C }", "construct"),
    ("BASE <http://x/>\nPREFIX : <http://x#>\nCONSTRUCT { ?a a :B } WHERE { ?a a :C }", "construct"),
    (PFX + "DELETE WHERE { ?m :meterOf :Customer_42 }", "removal"),
    (PFX + "DELETE { ?c :hasMeter ?m } WHERE { ?c :status \"terminated\" ; :hasMeter ?m }", "removal"),
    ("DELETE{ ?a :b ?c } WHERE { ?a :b ?c }", "removal"),
    (PFX + "DELETE DATA { :m1 :meterOf :Customer_42 }", "removal"),
    (PFX + "INSERT DATA { :a :b :c }", None),
    (PFX + "INSERT { ?c :status \"closed\" } WHERE { ?c :status \"terminated\" }", None),
    (PFX + "SELECT * WHERE { ?a ?b ?c }", None),
    ("", None),
    (None, None),
    ("DELETEX { }", None),
    ("CONSTRUCTED { }", None),
]


@pytest.mark.parametrize("query, kind", SAMPLES)
def test_the_first_keyword_after_the_prologue_is_the_kind(query, kind):
    assert sparql_rule_kind(query) == kind


def test_the_kind_constants_are_the_stored_words():
    assert (RULE_KIND_CONSTRUCT, RULE_KIND_REMOVAL) == ("construct", "removal")


def test_a_neither_form_query_gets_a_clear_message():
    assert NOT_A_RULE_MESSAGE.startswith("A Complex rule must start with CONSTRUCT")


@pytest.mark.skipif(shutil.which("node") is None, reason="node is required for the parity check")
def test_the_editor_reads_the_kind_the_same_way():
    source = BUSINESS_RULES_JS.read_text(encoding="utf-8")
    script = (
        "const fs = require('fs');\n"
        "global.window = global;\n"
        "global.document = { addEventListener() {}, getElementById: () => null };\n"
        "eval(fs.readFileSync(process.argv[1], 'utf8'));\n"
        "const M = window.BusinessRulesModule;\n"
        "const samples = JSON.parse(process.argv[2]);\n"
        "process.stdout.write(JSON.stringify(samples.map(q => M._sparqlKindOf(q))));\n"
    )
    queries = [q for q, _ in SAMPLES]
    completed = subprocess.run(
        ["node", "-e", script, str(BUSINESS_RULES_JS), json.dumps(queries)],
        capture_output=True, text=True, timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout) == [kind for _, kind in SAMPLES]
    assert "_sparqlKindOf(query) {" in source
