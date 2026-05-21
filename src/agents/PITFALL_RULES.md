# Ontology Pitfall Rules

The assistant MUST proactively avoid the following pitfalls when creating or modifying any
ontology. Check each applicable rule before proposing or applying changes, and warn the user
if a requested change would introduce one of these pitfalls.

---

## P1 — Logical Issues

### P1.1 Parent disjoint with children

A class is declared disjoint from one of its own subclasses.
This is a contradiction: no individual can simultaneously belong to both a class and its
subclass if they are disjoint, making the subclass unsatisfiable (always empty).

**Rule:** Never declare a class disjoint from any of its own subclasses or descendants.

---

### P1.2 Entity as subclass of both parent and grandparent

A class is explicitly declared as a direct subclass of both a parent class and one of that
parent's ancestors. The declaration to the ancestor is redundant because subclass transitivity
already implies it, and it can mislead reasoners.

**Rule:** When setting inheritance, do not add a direct `subClassOf` link to any ancestor that
is already implied by the existing parent chain.

---

### P1.3 Logical inconsistencies

Axioms in the ontology produce unsatisfiable classes — e.g., a class is simultaneously defined
as a subclass of two disjoint classes, or a restriction forces contradictory types. An OWL
reasoner would flag these classes as equivalent to `owl:Nothing`.

**Rule:** Before applying structural changes (parent assignment, disjointness, restrictions),
verify that no class is forced to be a subclass of two mutually disjoint classes.

---

## P2 — Structural Issues

### P2.1 Not connected hierarchies

The ontology contains isolated class trees with no common root other than `owl:Thing`.
Disconnected hierarchies often indicate that separate sub-ontologies were merged without
alignment, or that top-level grouping concepts are missing.

**Rule:** When adding new top-level classes, consider whether a shared grouping parent already
exists or should be created to keep the hierarchy connected.

---

### P2.2 Single subclass parent

A class has exactly one direct subclass. This is often a sign of unnecessary intermediate
nodes: if a parent has only one child, the hierarchy could usually be flattened without loss
of semantics, unless the parent is used in axioms independently.

**Rule:** Avoid creating intermediate parent classes that have only a single child unless the
parent is required by a constraint, restriction, or domain/range axiom.

---

### P2.3 Superfluous disjointness

Two classes are declared disjoint when one is already a subclass of the other, or when
disjointness is already implied by the hierarchy. The explicit disjointness assertion is
redundant and may create unintended logical side-effects.

**Rule:** Do not add an explicit disjointness axiom between a class and any of its
subclasses, or between siblings whose disjointness is already entailed.

---

### P2.4 Single subproperty parent

A property has exactly one direct sub-property. Mirrors P2.2 for properties: if a property
hierarchy node has only one child, it may be a superfluous intermediate that adds no
modelling value.

**Rule:** Avoid creating intermediate property parents with a single child unless the parent
is independently required by a constraint or axiom.

---

### P2.5 Range/Domain expansion

A sub-property declares a domain or range that is broader than its parent property's
domain/range. This violates the inheritance contract: a sub-property should restrict
(narrow) rather than expand the domain/range it inherits.

**Rule:** When adding or updating a property's domain/range, ensure that sub-properties
declare domains/ranges that are equal to or narrower than those of their parent property.

---

### P2.6 Possible hierarchy among properties

Two or more properties have names so similar that one may be a specialisation of the other,
yet no `rdfs:subPropertyOf` link is declared between them. This check flags candidate pairs
worth reviewing for a missing sub-property relationship.

**Rule:** When naming new properties, check whether an existing property already covers a
more general form of the same relationship, and declare the sub-property link if appropriate.

---

## P3 — Redundancy / Naming Issues

### P3.1 Properties replicating standard RDF ones

The ontology defines custom properties that duplicate well-known RDF/RDFS/OWL vocabulary
(e.g., a custom `hasLabel` property when `rdfs:label` already exists). Using standard
vocabulary improves interoperability and avoids redundant machinery.

**Rule:** Do not add custom properties that replicate `rdfs:label`, `rdfs:comment`,
`rdf:type`, `owl:sameAs`, or other standard vocabulary terms.

---

### P3.2 Range in property title

A property name encodes its range type (e.g., `hasPersonName`, `containsEvent`). Embedding
the range in the label couples naming to structure: renaming or changing the range requires
renaming the property, and it conflates two distinct modelling concerns.

**Rule:** Property names should express the relationship semantics only. Do not embed the
range class name inside the property name.

---

### P3.3 Domain in property title

A property name encodes its domain class (e.g., `personHasName`, `orderContainsItem`). Same
fragility as P3.2: naming should express the relationship semantics, not repeat structural
information already captured by `rdfs:domain` declarations.

**Rule:** Property names should express the relationship semantics only. Do not embed the
domain class name inside the property name.

---

## P4 — Semantic Issues

### P4.1 Overly generic classes

Classes whose labels are very broad, generic terms (e.g., `Thing`, `Entity`, `Object`,
`Item`) carry little semantic specificity. These often indicate placeholder concepts that
were never refined or that model a concept already covered by `owl:Thing`.

**Rule:** Class names should be specific enough to convey domain meaning. Reject or flag
names like `Thing`, `Object`, `Item`, `Element`, `Data`, or `Entity` unless there is a
strong domain-specific justification.

---

### P4.2 Synonyms in superclasses

A class has multiple superclasses whose labels are synonyms or near-synonyms. Declaring
redundant synonymous parents inflates the hierarchy without adding distinctions, and may
cause unintended logical entailments.

**Rule:** When assigning multiple parent classes, verify that the parents are semantically
distinct and do not express the same concept under different names.

---

### P4.3 Conflicting hierarchy

Sibling classes (sharing the same parent) have labels that are antonyms of each other.
Antonymous siblings often indicate a modelling conflict or that the classes should instead
be declared disjoint explicitly.

**Rule:** If two sibling classes represent opposite or mutually exclusive concepts, make
their disjointness explicit rather than leaving it implicit.

---

### P4.4 Subclasses with same semantics as superclasses

A subclass has a name or label that is semantically nearly identical to its parent. If the
subclass adds no new semantics, the hierarchy level is unnecessary.

**Rule:** A subclass must add meaningful distinctions relative to its parent. Do not create
a subclass whose name or definition is essentially a restatement of the parent.

---

### P4.5 Synonyms in properties

Two or more properties in the ontology have labels that are synonyms or highly semantically
similar, suggesting they may model the same relationship and one could be merged or aliased
via `owl:equivalentProperty`.

**Rule:** Before adding a new property, check existing properties for semantic overlap.
Prefer `owl:equivalentProperty` or consolidation over adding a near-duplicate.

---

### P4.6 Inverse properties not declared

A pair of properties appears to be semantically inverse (e.g., `hasPart` / `isPartOf`) but
no `owl:inverseOf` axiom links them. Declaring inverses allows reasoners to infer both
directions automatically.

**Rule:** When adding a directional relationship, consider whether its inverse is also
meaningful in the domain. If so, either add the inverse property or declare
`owl:inverseOf`.

---

### P4.7 DataProperties that can become ObjectProperties

A datatype property has values that look like class names or identifiers of existing classes
(e.g., a `hasType` string property whose values match class labels). These are candidates
for promotion to object properties pointing to the named classes.

**Rule:** Prefer object properties over string data properties when the intended range is
a named class in the ontology. Data properties should hold literal values (strings, numbers,
dates), not class references.
