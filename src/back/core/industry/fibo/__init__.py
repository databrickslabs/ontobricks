"""FIBO (Financial Industry Business Ontology) import service."""

from back.core.industry.fibo.FiboImportService import FiboImportService

# Backward-compat wrappers
get_fibo_catalog = FiboImportService.get_fibo_catalog
fetch_and_parse_fibo = FiboImportService.fetch_and_parse_fibo

__all__ = [
    "FiboImportService",
    "get_fibo_catalog",
    "fetch_and_parse_fibo",
]
