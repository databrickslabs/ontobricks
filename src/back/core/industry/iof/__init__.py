"""IOF (Industrial Ontologies Foundry) import service."""

from back.core.industry.iof.IofImportService import IofImportService

# Backward-compat wrappers
get_iof_catalog = IofImportService.get_iof_catalog
fetch_and_parse_iof = IofImportService.fetch_and_parse_iof

__all__ = [
    "IofImportService",
    "get_iof_catalog",
    "fetch_and_parse_iof",
]
