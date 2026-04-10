"""Shared RDF parsing utilities."""
from rdflib import Graph

from back.core.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_FORMATS = ("turtle", "xml", "n3", "nt", "json-ld")


def parse_rdf_flexible(
    data: str,
    formats: tuple = _DEFAULT_FORMATS,
) -> Graph:
    """Try parsing *data* as RDF in each format until one succeeds.

    Args:
        data: The serialised RDF content.
        formats: Ordered tuple of rdflib format names to try.

    Returns:
        A parsed :class:`rdflib.Graph`.

    Raises:
        ValueError: If none of the formats succeed.
    """
    graph = Graph()
    last_exc = None
    for fmt in formats:
        try:
            graph.parse(data=data, format=fmt)
            return graph
        except Exception as exc:
            logger.debug("RDF parse attempt failed for format %s", fmt, exc_info=True)
            last_exc = exc
            graph = Graph()
    raise ValueError(
        f"Could not parse RDF content (tried {', '.join(formats)})"
    ) from last_exc
