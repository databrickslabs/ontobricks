# SPEC: OWL import fidelity (GitHub #194)

> **Scope:** Parse → session JSON → generate round-trip for OWL/Turtle.
> Not an LLM/agent change. Studio UI does **not** gain editors for leftover
> annotations in this slice.

## 1. Purpose

Importing Turtle today projects RDF onto the visual OWL model and **discards**
anything the model does not know (`skos:definition`, DCTERMS, PROV,
`owl:versionInfo`, language tags, external `rdfs:subClassOf` IRIs). Export
rebuilds a new graph from `base_uri + localName`. Enterprise files are not
the ontology that was imported.

This slice ([#194](https://github.com/databrickslabs/ontobricks/issues/194))
makes import/export **graph-faithful for named-resource annotations and IRIs**
while keeping Studio as an OWL class/property editor.

## 2. Non-goals (this slice)

- Bit-exact Turtle text (prefix order, blank-node labels, comments).
- Showing leftover annotations in Designer / entity panels.
- Storing the original file verbatim beside the model (diverges on first edit).
- A closed allowlist of SKOS/DCTERMS/PROV only (would miss the next vocabulary).
- Changing industry importers (FIBO/CDISC/IOF/FHIR) beyond sharing parser/generator.

## 3. Success criteria

Given the `test.ttl` in #194, after `parse_owl` → `apply_parsed_owl_to_domain`
→ `generate_owl`, an RDFLib graph compare (isomorphic on the named triples
below) holds:

1. Ontology IRI stays `http://example.org/onto` (hash is only the **namespace**).
2. Ontology `rdfs:label` is `"Test Ontology"` (language tag `@en` kept), **not**
   the domain/project name.
3. `owl:versionInfo`, `dcterms:description`, `dcterms:source` remain on the
   ontology resource.
4. Class `ex:Customer` keeps `skos:definition`, `skos:editorialNote`,
   `dcterms:created`, `prov:wasDerivedFrom`, and
   `rdfs:subClassOf <http://example.org/other-onto#Party>` (full external IRI).
5. Property `ex:hasName` keeps `skos:definition`.
6. Prefixes `skos`, `dcterms`, `prov` are bound on export (not required to
   match source CURIEs character-for-character).

Existing Studio round-trips (`tests/units/ontology/test_workflow_owl_roundtrip.py`,
parser/generator unit tests) stay green. Classes created in Designer with no
`rdf_extras` export exactly as today.

## 4. Design

### 4.1 Header (`label` vs `name`)

`OntologyParser.get_ontology_info()` already returns `label`.
`OntologyImport.apply_parsed_owl_to_domain` must use
`ontology_info.get("label")` (then `name`) before falling back to the domain
name. Persist `ontology["name"]` from the file label when present.

### 4.2 URI fields (consumed structure)

Keep Studio local names (`name`, `parent`, `domain`, `range`). Add/keep full
IRIs and **use them on generate**:

| JSON field | Meaning |
|---|---|
| `cls["uri"]` / `prop["uri"]` | Resource IRI; generator prefers this over `base_uri + name` |
| `cls["parent_uri"]` | Full `rdfs:subClassOf` IRI when parent is named (not a restriction) |
| `prop["domain_uri"]` / `prop["range_uri"]` | Full domain/range IRIs |

Parser already stores `uri`. It currently stores `parent` as a local name
only — also store `parent_uri`. Same for property domain/range.

Generator: if `parent` starts with `http` **or** `parent_uri` is set, emit that
URI. Same for class/property subject.

### 4.3 Leftover triples (`rdf_extras`)

Session key `ontology["rdf_extras"]`:

```json
{
  "prefixes": {
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "dcterms": "http://purl.org/dc/terms/",
    "prov": "http://www.w3.org/ns/prov#"
  },
  "triples": [
    {
      "s": "http://example.org/onto#Customer",
      "p": "http://www.w3.org/2004/02/skos/core#definition",
      "o": "A party that buys goods.",
      "o_kind": "literal",
      "lang": "en",
      "datatype": null
    }
  ]
}
```

A triple is leftover when:

- Subject is a **named** URI (not a blank node).
- Subject is the ontology IRI, an extracted class, or an extracted property
  (ignore other named individuals / unused resources in this slice).
- Predicate is **not** in the consumed set (below).
- Object may be URI or literal (including lang/datatype). Skip blank-node
  objects (restrictions stay on the constraint/axiom path).

**Consumed predicates** (never leftover; already in the studio model):

- `rdf:type`
- `rdfs:label`, `rdfs:comment`, `rdfs:subClassOf`, `rdfs:domain`, `rdfs:range`
- `skos:altLabel`
- anything under `http://ontobricks.com/schema#`

OWL restriction / axiom triples with blank nodes stay on the existing
constraint/axiom extractors.

On export: generate the studio graph first, then add leftover triples whose
**subject still exists** in the generated graph (drop extras for deleted
classes). Bind leftover prefixes.

### 4.4 Language tags on consumed literals

Store optional `label_lang` / `comment_lang` on ontology info, classes, and
properties. Generator emits `Literal(value, lang=...)` when set. Default
(Designer-created, no lang) stays a plain literal.

### 4.5 Data flow

```
Turtle → OntologyParser (info, classes, properties, constraints, …, rdf_extras)
      → OntologyImport.apply_parsed_owl_to_domain (persist rdf_extras)
      → OntologyGenerator.generate (IRIs + rdf_extras)
      → Turtle
```

## 5. Failure modes

- Duplicate triples if a leftover predicate is later added to the consumed
  set without stripping stored extras — migration: on generate, skip leftover
  whose predicate is consumed.
- User deletes a class: extras for that subject are omitted (not an error).
- User changes a class local name but URI stays: extras still attach (URI key).
- User changes URI in designer: extras on the old URI are dropped. Acceptable
  in this slice (no UI to retarget extras).

## 6. Testing

Golden file: #194 `test.ttl` in
`tests/units/ontology/test_owl_import_fidelity.py`.
Parse → generate → `Graph.parse` both sides; assert required triples with
`in graph`. Keep existing round-trip tests.

## 7. Docs

User-facing: `docs/user-guide.md` Ontology Import — state that OWL import
preserves non-OWL annotations and external IRIs on named classes/properties,
and that those extras are not edited in Studio.
