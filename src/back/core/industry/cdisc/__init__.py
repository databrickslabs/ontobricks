"""CDISC (Clinical Data Interchange Standards Consortium) RDF import service."""

from back.core.industry.cdisc.CdiscImportService import CdiscImportService

# Backward-compat wrappers
get_cdisc_catalog = CdiscImportService.get_cdisc_catalog
fetch_and_parse_cdisc = CdiscImportService.fetch_and_parse_cdisc

__all__ = [
    "CdiscImportService",
    "get_cdisc_catalog",
    "fetch_and_parse_cdisc",
]
