# OWL import fidelity (#194) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make OWL/Turtle import→export keep the ontology header, external IRIs,
language tags, and non-OWL annotations from [#194](https://github.com/databrickslabs/ontobricks/issues/194)
without turning Studio into a general RDF editor.

**Architecture:** Parser fills URI fields + optional lang + `rdf_extras` leftover
triples. Session stores `ontology["rdf_extras"]`. Generator prefers stored IRIs
and appends leftover triples whose subject still exists. Consumed studio
predicates (`rdf:type`, `rdfs:label/comment/subClassOf/domain/range`,
`skos:altLabel`, `ontobricks:*`) are never leftover.

**Tech Stack:** rdflib, `OntologyParser` / `OntologyGenerator` / `OntologyImport`,
pytest (`uv run --frozen pytest`).

## Global Constraints

- Version folder: `changelogs/v0.9.0/` (`pyproject.toml` version `0.9.0`).
- Changelog author prefix: `benoitcayladbx`; filename `YYYY-MM-DD.log`; English.
- Tests: `uv run --frozen pytest -q -m "not scenario"` (`--frozen` mandatory).
- Logging: `%-style`, `get_logger(__name__)`, English templates.
- Errors: `OntoBricksError` hierarchy; no `{'success': False}` from services.
- Class-first: leftover helpers live on `OntologyRdfExtras` (one public class
  per file). Do not add module-level functions.
- Do not edit leftover annotations in Studio in this slice.
- Do not store the original Turtle blob.
- Existing `test_workflow_owl_roundtrip.py` / `test_owl_parser.py` /
  `test_owl_generator.py` must stay green.

**Spec:** `.planning/owl-import-fidelity/SPEC.md`

---

### Task 1: Golden failing test for #194 `test.ttl`

**Files:**
- Create: `tests/units/ontology/test_owl_import_fidelity.py`
- Create (fixture string in the test file; no extra asset required)

**Interfaces:**
- Consumes: `OntologyOwl.parse_owl`, `OntologyOwl.generate_owl`
- Produces: failing assertions that later tasks turn green one by one

- [ ] **Step 1: Write the failing test**

```python
# tests/units/ontology/test_owl_import_fidelity.py
"""Round-trip fidelity for GitHub issue #194."""

import pytest
from rdflib import DCTERMS, OWL, RDF, RDFS, Graph, Literal, URIRef
from rdflib.namespace import SKOS, XSD, Namespace

from back.objects.ontology.OntologyOwl import OntologyOwl

PROV = Namespace("http://www.w3.org/ns/prov#")
EX = Namespace("http://example.org/onto#")
OTHER = URIRef("http://example.org/other-onto#Party")

ISSUE_194_TTL = """@prefix ex:      <http://example.org/onto#> .
@prefix owl:     <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:    <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:     <http://www.w3.org/2001/XMLSchema#> .
@prefix skos:    <http://www.w3.org/2004/02/skos/core#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix prov:    <http://www.w3.org/ns/prov#> .

<http://example.org/onto> a owl:Ontology ;
    rdfs:label "Test Ontology"@en ;
    dcterms:description "Used to test import fidelity."@en ;
    dcterms:source <http://example.org/other-onto> ;
    owl:versionInfo "1.0" .

ex:Customer a owl:Class ;
    rdfs:label "Customer"@en ;
    skos:definition "A party that buys goods."@en ;
    skos:editorialNote "Review naming."@en ;
    dcterms:created "2026-01-01"^^xsd:date ;
    prov:wasDerivedFrom <http://example.org/other-onto#Party> ;
    rdfs:subClassOf <http://example.org/other-onto#Party> .

ex:hasName a owl:DatatypeProperty ;
    rdfs:domain ex:Customer ;
    rdfs:range xsd:string ;
    skos:definition "The customer's legal name."@en .
"""


def _roundtrip() -> Graph:
    info, classes, properties, constraints, swrl, axioms, expressions, groups = (
        OntologyOwl.parse_owl(ISSUE_194_TTL, extract_advanced=True)
    )
    turtle = OntologyOwl.generate_owl(
        {
            "base_uri": info.get("namespace") or info.get("uri"),
            "name": info.get("label") or info.get("name") or "",
            "classes": classes,
            "properties": properties,
            "rdf_extras": info.get("rdf_extras") or {},
        },
        constraints=constraints,
        swrl_rules=swrl,
        axioms=axioms,
        expressions=expressions,
        groups=groups,
    )
    g = Graph()
    g.parse(data=turtle, format="turtle")
    return g


@pytest.mark.unit
class TestIssue194ImportFidelity:
    def test_ontology_header_and_annotations(self):
        g = _roundtrip()
        onto = URIRef("http://example.org/onto")
        assert (onto, RDF.type, OWL.Ontology) in g
        assert (onto, RDFS.label, Literal("Test Ontology", lang="en")) in g
        assert (
            onto,
            DCTERMS.description,
            Literal("Used to test import fidelity.", lang="en"),
        ) in g
        assert (onto, DCTERMS.source, URIRef("http://example.org/other-onto")) in g
        assert (onto, OWL.versionInfo, Literal("1.0")) in g

    def test_class_external_parent_and_skos_dcterms_prov(self):
        g = _roundtrip()
        cls = EX.Customer
        assert (cls, RDF.type, OWL.Class) in g
        assert (cls, RDFS.label, Literal("Customer", lang="en")) in g
        assert (
            cls,
            SKOS.definition,
            Literal("A party that buys goods.", lang="en"),
        ) in g
        assert (
            cls,
            SKOS.editorialNote,
            Literal("Review naming.", lang="en"),
        ) in g
        assert (cls, DCTERMS.created, Literal("2026-01-01", datatype=XSD.date)) in g
        assert (cls, PROV.wasDerivedFrom, OTHER) in g
        assert (cls, RDFS.subClassOf, OTHER) in g

    def test_property_skos_definition(self):
        g = _roundtrip()
        prop = EX.hasName
        assert (prop, RDF.type, OWL.DatatypeProperty) in g
        assert (
            prop,
            SKOS.definition,
            Literal("The customer's legal name.", lang="en"),
        ) in g
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_import_fidelity.py -q`

Expected: FAIL (missing DCTERMS/SKOS/PROV triples and/or language tags; possible
label `"Test Ontology"` already present if generator is fed `info["label"]` in
this harness — header assertions may still fail on `@en`).

- [ ] **Step 3: Do not implement production code in this task**

Leave the test failing. Later tasks turn individual asserts green.

---

### Task 2: Header label + language tags on consumed literals

**Files:**
- Modify: `src/back/core/w3c/owl/OntologyParser.py` (`get_ontology_info`,
  `get_classes`, `get_properties`)
- Modify: `src/back/core/w3c/owl/OntologyGenerator.py` (`generate`, `_add_class`,
  `_add_property`)
- Modify: `src/back/objects/ontology/OntologyImport.py`
  (`apply_parsed_owl_to_domain`)
- Test: `tests/units/ontology/test_owl_import_fidelity.py` (header label/lang)
- Test: `tests/units/ontology/test_owl_parser.py` (existing label assertions)

**Interfaces:**
- Consumes: `get_ontology_info()` keys `uri`, `label`, `comment`, `namespace`
- Produces: extra keys `label_lang`, `comment_lang` on info/class/property dicts;
  `apply_parsed_owl_to_domain` uses `ontology_info.get("label")` before `name`

- [ ] **Step 1: Extend parser tests for lang + import name**

```python
def test_ontology_label_keeps_language_tag():
    ttl = '''@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
<http://example.org/onto> a owl:Ontology ;
    rdfs:label "Test Ontology"@en .
'''
    info = OntologyParser(ttl).get_ontology_info()
    assert info["label"] == "Test Ontology"
    assert info["label_lang"] == "en"
```

Add a unit on `OntologyImport.apply_parsed_owl_to_domain` with a stub domain
(follow existing tests in `tests/units/ontology/test_ontology_service.py`) that
`ontology["name"] == "Test Ontology"` when `ontology_info` has `label` and no
`name`.

- [ ] **Step 2: Run to verify fail**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_parser.py -q -k language`

Expected: FAIL (`label_lang` missing).

- [ ] **Step 3: Parser — capture lang**

In `get_ontology_info` / class / property label and comment loops, use the
rdflib Literal:

```python
label = None
label_lang = None
for lbl in self.graph.objects(onto, RDFS.label):
    label = str(lbl)
    label_lang = getattr(lbl, "language", None)
    break
```

Include `label_lang` / `comment_lang` in the returned dicts (`None` if absent).

- [ ] **Step 4: Import — prefer `label`**

In `apply_parsed_owl_to_domain`:

```python
file_name = (
    ontology_info.get("label")
    or ontology_info.get("name")
    or ""
)
if name_fallback_to_domain:
    default_name = self._domain.info.get("name", "")
    resolved_name = file_name or default_name
else:
    resolved_name = file_name
```

- [ ] **Step 5: Generator — emit lang**

When adding `rdfs:label` / `rdfs:comment`:

```python
lang = cls.get("label_lang") or None
self.graph.add((class_uri, RDFS.label, Literal(label, lang=lang or None)))
```

Same for ontology header in `generate()` using `ontology_name` + optional
`self.label_lang` passed from `data` (add optional `label_lang` on
`OntologyGenerator.__init__`, default `None`; `OntologyOwl.generate_owl`
forwards `data.get("label_lang")`).

- [ ] **Step 6: Re-run tests**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_parser.py tests/units/ontology/test_owl_generator.py tests/units/ontology/test_owl_import_fidelity.py::TestIssue194ImportFidelity::test_ontology_header_and_annotations -q`

Expected: header **label** assert passes; DCTERMS/versionInfo still fail.

---

### Task 3: Preserve class/property/parent/domain/range IRIs

**Files:**
- Modify: `src/back/core/w3c/owl/OntologyParser.py` (`get_classes`, `get_properties`)
- Modify: `src/back/core/w3c/owl/OntologyGenerator.py` (`_add_class`, `_add_property`,
  `_add_data_property_for_class`)
- Test: `tests/units/ontology/test_owl_import_fidelity.py`
- Test: add cases in `tests/units/ontology/test_owl_parser.py` /
  `test_owl_generator.py`

**Interfaces:**
- Produces: `parent_uri`, `domain_uri`, `range_uri` (strings, may be empty)
- Generator: subject URI = `cls.get("uri")` if http(s), else `base_uri + name`

- [ ] **Step 1: Failing parser/generator tests**

```python
def test_external_subclass_keeps_full_uri():
    ttl = ISSUE_194_TTL  # or a 4-line extract
    classes = {c["name"]: c for c in OntologyParser(ttl).get_classes()}
    assert classes["Customer"]["parent"] == "Party"
    assert classes["Customer"]["parent_uri"] == "http://example.org/other-onto#Party"

def test_generate_emits_external_parent_uri():
    gen = OntologyGenerator(
        base_uri="http://example.org/onto#",
        ontology_name="Test Ontology",
        classes=[{
            "name": "Customer",
            "uri": "http://example.org/onto#Customer",
            "label": "Customer",
            "parent": "Party",
            "parent_uri": "http://example.org/other-onto#Party",
        }],
        properties=[],
    )
    g = Graph(); g.parse(data=gen.generate(), format="turtle")
    assert (
        URIRef("http://example.org/onto#Customer"),
        RDFS.subClassOf,
        URIRef("http://example.org/other-onto#Party"),
    ) in g
```

- [ ] **Step 2: Run to verify fail**

Expected: `parent_uri` KeyError / missing triple (local `…#Party` instead).

- [ ] **Step 3: Parser stores full IRIs**

When reading `rdfs:subClassOf` for a named parent:

```python
parent_uri = str(parent_cls)
parent = self._extract_local_name(parent_uri)
```

Put both on the class dict (`parent_uri` `""` when no named parent).

For properties, keep local `domain`/`range` for Studio and add:

```python
"domain_uri": str(dom) if dom is not None else "",
"range_uri": str(rng) if rng is not None else "",
```

- [ ] **Step 4: Generator prefers stored IRIs**

```python
class_uri = URIRef(cls["uri"]) if str(cls.get("uri", "")).startswith(("http://", "https://")) else URIRef(self.base_uri + class_name)
```

Parent:

```python
parent_ref = (cls.get("parent_uri") or parent).strip()
if parent_ref.startswith("http://") or parent_ref.startswith("https://"):
    parent_uri = URIRef(parent_ref)
else:
    parent_uri = URIRef(self.base_uri + parent_ref)
```

Same pattern for property subject and domain/range (`domain_uri` / `range_uri`).

- [ ] **Step 5: Re-run**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_import_fidelity.py::TestIssue194ImportFidelity::test_class_external_parent_and_skos_dcterms_prov -q`

Expected: `rdfs:subClassOf` other-onto **passes**; SKOS/DCTERMS/PROV still fail.

---

### Task 4: `OntologyRdfExtras` leftover extract + emit

**Files:**
- Create: `src/back/core/w3c/owl/OntologyRdfExtras.py`
- Modify: `src/back/core/w3c/owl/__init__.py` (export the class)
- Modify: `src/back/core/w3c/owl/OntologyParser.py` (call extract after classes/properties)
- Modify: `src/back/core/w3c/owl/OntologyGenerator.py` (bind prefixes + add triples)
- Modify: `src/back/objects/ontology/OntologyOwl.py` (`parse_owl` attaches extras
  onto `ontology_info["rdf_extras"]`; `generate_owl` forwards `data["rdf_extras"]`)
- Modify: `src/back/objects/ontology/OntologyImport.py` (persist
  `ontology["rdf_extras"]`)
- Test: `tests/units/ontology/test_owl_rdf_extras.py`
- Test: `tests/units/ontology/test_owl_import_fidelity.py` (should go green)

**Interfaces:**
- Produces:

```python
class OntologyRdfExtras:
    CONSUMED_PREDICATES: frozenset[str]
    def extract(self, graph, *, ontology_uri: str, class_uris: set[str], property_uris: set[str]) -> dict
    def apply(self, graph, extras: dict, *, live_subjects: set[str]) -> None
```

`extract` return shape: `{"prefixes": dict[str, str], "triples": list[dict]}`.
Each triple: `s`, `p`, `o`, `o_kind` (`"literal"` | `"uri"`), `lang`, `datatype`
(strings or `None`).

- [ ] **Step 1: Failing unit tests for extras**

```python
# tests/units/ontology/test_owl_rdf_extras.py
from rdflib import Graph, Literal, URIRef, RDF, RDFS, OWL
from rdflib.namespace import SKOS
from back.core.w3c.owl.OntologyRdfExtras import OntologyRdfExtras

def test_skos_definition_is_leftover_rdfs_label_is_not():
    g = Graph()
    cls = URIRef("http://example.org/onto#Customer")
    g.add((cls, RDF.type, OWL.Class))
    g.add((cls, RDFS.label, Literal("Customer")))
    g.add((cls, SKOS.definition, Literal("A party that buys goods.", lang="en")))
    extras = OntologyRdfExtras().extract(
        g,
        ontology_uri="http://example.org/onto",
        class_uris={str(cls)},
        property_uris=set(),
    )
    preds = {t["p"] for t in extras["triples"]}
    assert str(SKOS.definition) in preds
    assert str(RDFS.label) not in preds
    assert str(RDF.type) not in preds
```

```python
def test_apply_skips_deleted_subject():
    from rdflib import Graph
    extras = {
        "prefixes": {},
        "triples": [{
            "s": "http://example.org/onto#Gone",
            "p": str(SKOS.definition),
            "o": "x",
            "o_kind": "literal",
            "lang": None,
            "datatype": None,
        }],
    }
    g = Graph()
    OntologyRdfExtras().apply(g, extras, live_subjects=set())
    assert len(g) == 0
```

- [ ] **Step 2: Run to verify fail**

Expected: `ImportError` for `OntologyRdfExtras`.

- [ ] **Step 3: Implement `OntologyRdfExtras`**

Consumed predicates (string IRIs):

- `http://www.w3.org/1999/02/22-rdf-syntax-ns#type`
- `http://www.w3.org/2000/01/rdf-schema#label`
- `http://www.w3.org/2000/01/rdf-schema#comment`
- `http://www.w3.org/2000/01/rdf-schema#subClassOf`
- `http://www.w3.org/2000/01/rdf-schema#domain`
- `http://www.w3.org/2000/01/rdf-schema#range`
- `http://www.w3.org/2004/02/skos/core#altLabel`

Skip any predicate starting with `http://ontobricks.com/schema#`.

Skip blank-node subjects and blank-node objects.

`extract` prefixes: iterate `graph.namespace_manager.namespaces()`, drop
`owl/rdf/rdfs/xsd/xml` (generator already binds those). Keep `skos`, `dcterms`,
`prov`, and any other custom prefix.

`apply`: for each triple, if `s` not in `live_subjects` continue; skip if `p`
is consumed (defense in depth). Bind prefixes with `graph.bind`.

- [ ] **Step 4: Wire parser**

After collecting classes/properties, `OntologyParser.get_rdf_extras()`:

```python
def get_rdf_extras(self) -> dict:
    info = self.get_ontology_info()
    classes = self.get_classes()
    properties = self.get_properties()
    return OntologyRdfExtras().extract(
        self.graph,
        ontology_uri=info["uri"],
        class_uris={c["uri"] for c in classes if c.get("uri")},
        property_uris={p["uri"] for p in properties if p.get("uri")},
    )
```

Avoid double-walking if expensive: `parse_owl` already called `get_classes`;
have `parse_owl` pass those lists into extras extract instead of calling
`get_rdf_extras()` independently.

In `OntologyOwl.parse_owl`, after building the tuple:

```python
rdf_extras = OntologyRdfExtras().extract(
    parser.graph,
    ontology_uri=ontology_info["uri"],
    class_uris={c.get("uri") for c in classes if c.get("uri")},
    property_uris={p.get("uri") for p in properties if p.get("uri")},
)
ontology_info["rdf_extras"] = rdf_extras
```

- [ ] **Step 5: Persist on import**

In `apply_parsed_owl_to_domain` `update(...)` add:

```python
"rdf_extras": ontology_info.get("rdf_extras") or {},
```

RDFS ingest (`apply_parsed_rdfs_to_domain`): same key if RDFS parser later
gains extras; for this slice OWL path only is required.

- [ ] **Step 6: Wire generator**

`OntologyGenerator.__init__` takes `rdf_extras: dict | None = None`.
`OntologyOwl.generate_owl` passes `data.get("rdf_extras")`.

At end of `generate()`, before serialize:

```python
live = {str(s) for s in self.graph.subjects()}
OntologyRdfExtras().apply(self.graph, self.rdf_extras or {}, live_subjects=live)
```

Bind leftover prefixes **before** serialize.

- [ ] **Step 7: Green #194 tests**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_import_fidelity.py tests/units/ontology/test_owl_rdf_extras.py tests/units/ontology/test_owl_parser.py tests/units/ontology/test_owl_generator.py tests/units/ontology/test_workflow_owl_roundtrip.py -q`

Expected: PASS.

---

### Task 5: Import path uses extras (session + generate_owl from domain)

**Files:**
- Modify: `src/back/objects/ontology/Ontology.py` (`generate_owl` wrapper — pass
  `rdf_extras` from `s.ontology`)
- Modify: `src/back/objects/ontology/OntologyOwl.py` if the domain `generate_owl`
  does not already pass the full ontology dict
- Test: extend `tests/units/ontology/test_owl_import_fidelity.py` with a parse →
  dict → `generate_owl(domain_ontology_dict)` path (no Flask session required)

**Interfaces:**
- Consumes: `ontology["rdf_extras"]` from session JSON
- Produces: export Turtle including leftovers when user clicks Generate/Export

- [ ] **Step 1: Trace `Ontology.generate_owl`**

Confirm `src/back/objects/ontology/Ontology.py` / `OntologyOwl.generate_owl`
receives the domain ontology dict. If it only forwards `base_uri/name/classes/properties`,
add `rdf_extras` and `label_lang`.

- [ ] **Step 2: Test**

```python
def test_generate_owl_reads_rdf_extras_from_config():
    info, classes, properties, *_rest = OntologyOwl.parse_owl(
        ISSUE_194_TTL, extract_advanced=True
    )
    turtle = OntologyOwl.generate_owl({
        "base_uri": info["namespace"],
        "name": info["label"],
        "label_lang": info.get("label_lang"),
        "classes": classes,
        "properties": properties,
        "rdf_extras": info["rdf_extras"],
    })
    g = Graph(); g.parse(data=turtle, format="turtle")
    assert (EX.Customer, SKOS.definition, Literal("A party that buys goods.", lang="en")) in g
```

- [ ] **Step 3: Implement the forward**

`generate_owl` signature stays `(data, constraints=..., ...)`. Read extras from
`data`.

- [ ] **Step 4: Run**

Run: `uv run --frozen pytest tests/units/ontology/test_owl_import_fidelity.py -q`

Expected: PASS.

---

### Task 6: Docs + changelog + full test run

**Files:**
- Modify: `docs/user-guide.md` (Ontology Import section, ~line 2376)
- Modify: `changelogs/v0.9.0/benoitcayladbx_YYYY-MM-DD.log` (create or append;
  today from `date +%F`)
- Optional: `docs/features.md` one-line Import/Export bullet

- [ ] **Step 1: User-guide paragraph**

Add under OWL import:

> OntoBricks keeps the ontology IRI and label from the file, full IRIs for
> external `rdfs:subClassOf` / domain / range, language tags on labels, and
> extra annotations on the ontology, classes, and properties (SKOS, DCTERMS,
> PROV, `owl:versionInfo`, and any other non-consumed predicate). Those extra
> triples are restored on OWL export. They are not shown or edited in Studio.
> Deleting a class drops its leftover triples.

State explicitly: Turtle **text** (comment layout, prefix order) is not
bit-preserved.

- [ ] **Step 2: Changelog section (English)**

Title: `Preserve non-OWL triples and external IRIs on OWL import`

Context: GitHub #194 — import rebuilt from the studio model and dropped
SKOS/DCTERMS/PROV plus rewrote external parents and the ontology label.

- [ ] **Step 3: Full unit/integration run**

Run: `uv run --frozen pytest -q -m "not scenario"`

Expected: all pass. Paste the summary line into the changelog `Tests:` field.

---

## Out of scope (do not do in this plan)

- Designer UI for leftover triples.
- RDFS/SKOS-as-concept importer extras (unless it already shares `OntologyParser`).
- Individuals / blank-node annotation graphs / `owl:imports` network fetch.
- Bit-exact source Turtle serialization.
