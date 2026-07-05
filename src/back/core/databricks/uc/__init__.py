"""Unity Catalog integration — metadata, volumes, identifier safety, domain I/O."""

from .identifiers import (
    UC_IDENTIFIER_RE,
    quote_uc_fqn,
    quote_uc_identifier,
    validate_uc_identifier,
)
from .VolumeFileService import VolumeFileService
from .UnityCatalog import UnityCatalog
from .MetadataService import MetadataService
from .UCDomainIO import UCDomainIO

__all__ = [
    "UnityCatalog",
    "UCDomainIO",
    "VolumeFileService",
    "MetadataService",
    "UC_IDENTIFIER_RE",
    "quote_uc_fqn",
    "quote_uc_identifier",
    "validate_uc_identifier",
]
