"""Centralised constants for the reasoning package.

All shared URIs, compiled regexes, operator maps, and vocabulary sets
used across the reasoning engines live here as a single source of truth.
"""

import re

from back.core.triplestore.constants import RDF_TYPE, RDFS_LABEL  # noqa: F401

# ---------------------------------------------------------------------------
# OWL RL reasoner constants
# ---------------------------------------------------------------------------

OWLRL_PROVENANCE = "owlrl"

AXIOMATIC_PREFIXES = (
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "http://www.w3.org/2000/01/rdf-schema#",
    "http://www.w3.org/2002/07/owl#",
    "http://www.w3.org/2001/XMLSchema#",
)

TAUTOLOGICAL_PREDICATES = {
    "http://www.w3.org/2002/07/owl#sameAs",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf",
    "http://www.w3.org/2000/01/rdf-schema#subPropertyOf",
    "http://www.w3.org/2002/07/owl#equivalentClass",
    "http://www.w3.org/2002/07/owl#equivalentProperty",
}

NOISE_TYPES = {
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2002/07/owl#NamedIndividual",
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/2002/07/owl#FunctionalProperty",
    "http://www.w3.org/2002/07/owl#InverseFunctionalProperty",
    "http://www.w3.org/2002/07/owl#TransitiveProperty",
    "http://www.w3.org/2002/07/owl#SymmetricProperty",
    "http://www.w3.org/2002/07/owl#Thing",
    "http://www.w3.org/2002/07/owl#Ontology",
    "http://www.w3.org/2000/01/rdf-schema#Resource",
    "http://www.w3.org/2000/01/rdf-schema#Class",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
}

# ---------------------------------------------------------------------------
# SWRL parsing regexes
# ---------------------------------------------------------------------------

SWRL_ATOM_RE = re.compile(r"([A-Za-z_][\w.]*)\(([^)]+)\)")
NEGATED_ATOM_RE = re.compile(r"not\(\s*([A-Za-z_][\w.]*)\(([^)]+)\)\s*\)")

# ---------------------------------------------------------------------------
# Aggregate rule constants
# ---------------------------------------------------------------------------

AGG_FUNCTIONS = {"count", "sum", "avg", "min", "max"}

AGG_OPERATORS = {
    "lt": "<",
    "gt": ">",
    "eq": "=",
    "lte": "<=",
    "gte": ">=",
    "neq": "<>",
}

# ---------------------------------------------------------------------------
# Decision table operator maps
# ---------------------------------------------------------------------------

DT_STRING_OPS = {"eq", "neq", "startsWith", "endsWith", "contains"}
DT_NUMERIC_OPS = {"gt", "gte", "lt", "lte"}

DT_OP_SQL = {
    "eq": "= {v}",
    "neq": "<> {v}",
    "gt": "> {v}",
    "gte": ">= {v}",
    "lt": "< {v}",
    "lte": "<= {v}",
    "startsWith": "LIKE CONCAT({v}, '%%')",
    "endsWith": "LIKE CONCAT('%%', {v})",
    "contains": "LIKE CONCAT('%%', {v}, '%%')",
    "any": None,
}

DT_OP_CYPHER = {
    "eq": "= {v}",
    "neq": "<> {v}",
    "gt": "> {v}",
    "gte": ">= {v}",
    "lt": "< {v}",
    "lte": "<= {v}",
    "startsWith": "STARTS WITH {v}",
    "endsWith": "ENDS WITH {v}",
    "contains": "CONTAINS {v}",
    "any": None,
}

# ---------------------------------------------------------------------------
# SPARQL CONSTRUCT regexes
# ---------------------------------------------------------------------------

CONSTRUCT_RE = re.compile(
    r"CONSTRUCT\s*\{([^}]+)\}\s*WHERE\s*\{(.+)\}\s*$",
    re.IGNORECASE | re.DOTALL,
)

TRIPLE_PATTERN_RE = re.compile(
    r"(\?\w+|<[^>]+>|\w*:\w+|\w+)"
    r"\s+"
    r"(\?\w+|<[^>]+>|\w*:\w+|a)"
    r"\s+"
    r"(\?\w+|<[^>]+>|\w*:\w+|\"[^\"]*\"|\d+)"
)

# ---------------------------------------------------------------------------
# Well-known namespace prefix map (rdf, rdfs, owl, xsd)
# ---------------------------------------------------------------------------

NS_PREFIX_MAP = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

# ---------------------------------------------------------------------------
# Constraint-check skip types
# ---------------------------------------------------------------------------

CONSTRAINT_SKIP_TYPES = {
    "transitive",
    "symmetric",
    "asymmetric",
    "reflexive",
    "irreflexive",
}
