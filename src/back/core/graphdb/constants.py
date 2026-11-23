"""Graph DB package constants."""

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"

# Which graph a read targets: the Original Graph (virtual Delta relation built
# from the mapping) or the Certified Graph (Context backend). ``None`` keeps the
# store / relation resolution of a build from the mapping.
GRAPH_VARIANT_ORIGINAL = "original"
GRAPH_VARIANT_CERTIFIED = "certified"
GRAPH_VARIANTS = (GRAPH_VARIANT_ORIGINAL, GRAPH_VARIANT_CERTIFIED)

# Certified enrichment kind whose rows replace the base values of their
# (subject, predicate) in ``…_certified`` (set-attribute business rules).
CERT_KIND_OVERRIDE = "override"
