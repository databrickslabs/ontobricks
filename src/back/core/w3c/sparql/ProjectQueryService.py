"""SPARQL validation, execution, and samples over project JSON (R2RML in assignment)."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from back.core.errors import InfrastructureError, ValidationError
from back.core.logging import get_logger
from back.core.w3c.sparql.SparqlQueryRunner import SparqlQueryRunner
from back.objects.project.payload import resolve_project_slice

logger = get_logger(__name__)


class ProjectQueryService:
    """Validate and run SPARQL against project slice data; build sample queries."""

    @staticmethod
    def validate_sparql_query(query: str) -> Tuple[bool, Optional[str]]:
        """Validate SPARQL query syntax.

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            from rdflib.plugins.sparql import prepareQuery

            prepareQuery(query)
            return True, None
        except Exception as e:
            logger.exception("Failed to validate SPARQL query: %s", e)
            return False, str(e)

    @staticmethod
    def execute_sparql_query(
        project_data: Dict[str, Any],
        query: str,
        limit: int = 100,
        engine: str = "local",
    ) -> Dict[str, Any]:
        """Execute a SPARQL query against project data.

        Raises:
            ValidationError: No R2RML mapping or unsupported engine.
            InfrastructureError: Query execution failure.
        """
        sl = resolve_project_slice(project_data)
        assignment = sl["assignment"]
        r2rml = assignment.get("r2rml_output", "")

        if not r2rml:
            raise ValidationError("No R2RML mapping found. Generate R2RML first.")

        if engine != "local":
            raise ValidationError("Spark engine requires active Databricks connection")

        try:
            result = SparqlQueryRunner.execute_local_query(
                query=query,
                rdf_content=r2rml,
                limit=limit,
            )
            if not result.get("success"):
                raise InfrastructureError(
                    result.get("message", "Query execution failed"),
                )
            return result
        except (ValidationError, InfrastructureError):
            raise
        except Exception as e:
            logger.exception("Failed to execute SPARQL query: %s", e)
            raise InfrastructureError("SPARQL query execution failed", detail=str(e))

    @staticmethod
    def generate_sample_queries(project_data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate sample SPARQL queries based on the ontology."""
        sl = resolve_project_slice(project_data)
        ontology = sl["ontology"]
        classes = ontology.get("classes", [])
        properties = ontology.get("properties", [])

        samples: List[Dict[str, str]] = []

        samples.append(
            {
                "name": "Select All Triples",
                "description": "Get all triples in the knowledge graph",
                "query": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 100",
            }
        )

        for cls in classes[:5]:
            uri = cls.get("uri", "")
            name = cls.get("name", cls.get("localName", "Unknown"))
            if uri:
                samples.append(
                    {
                        "name": f"Get all {name}",
                        "description": f"Retrieve all instances of {name}",
                        "query": f"SELECT ?instance WHERE {{\n  ?instance a <{uri}>\n}} LIMIT 100",
                    }
                )

        for prop in properties[:3]:
            uri = prop.get("uri", "")
            name = prop.get("name", prop.get("localName", "Unknown"))
            if uri:
                samples.append(
                    {
                        "name": f"{name} relationships",
                        "description": f"Find all {name} relationships",
                        "query": (
                            f"SELECT ?subject ?object WHERE {{\n"
                            f"  ?subject <{uri}> ?object\n}} LIMIT 100"
                        ),
                    }
                )

        return samples
