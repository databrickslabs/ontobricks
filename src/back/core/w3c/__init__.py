"""W3C standards — OWL, RDFS, R2RML, SPARQL, SHACL utilities."""
from back.core.w3c.owl import OntologyGenerator, OntologyParser
from back.core.w3c.rdfs import RDFSParser
from back.core.w3c.r2rml import R2RMLGenerator, generate_r2rml_from_config, R2RMLParser, parse_r2rml_content
from back.core.w3c.sparql import (
    DIALECT_SPARK,
    execute_local_query,
    extract_r2rml_mappings,
    ProjectQueryService,
    SparqlQueryRunner,
    SparqlTranslator,
    translate_sparql_to_spark,
)
from back.core.w3c.shacl import (
    QUALITY_CATEGORIES,
    resolve_prop_uri,
    SHACLGenerator,
    SHACLParser,
    SHACLService,
)

__all__ = [
    "DIALECT_SPARK",
    "OntologyGenerator",
    "OntologyParser",
    "RDFSParser",
    "R2RMLGenerator",
    "generate_r2rml_from_config",
    "R2RMLParser",
    "parse_r2rml_content",
    "execute_local_query",
    "extract_r2rml_mappings",
    "translate_sparql_to_spark",
    "ProjectQueryService",
    "SparqlQueryRunner",
    "SparqlTranslator",
    "SHACLService",
    "SHACLGenerator",
    "SHACLParser",
    "QUALITY_CATEGORIES",
    "resolve_prop_uri",
]
