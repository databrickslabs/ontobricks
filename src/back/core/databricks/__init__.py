"""Databricks integration layer — typed facades for every API surface."""

from back.core.databricks.DatabricksAuth import DatabricksAuth  # noqa: F401
from back.core.databricks.LakebaseAuth import (  # noqa: F401
    LakebaseAuth,
    get_lakebase_auth,
)
from back.core.databricks.DatabricksClient import DatabricksClient  # noqa: F401
from back.core.databricks.SQLWarehouse import SQLWarehouse  # noqa: F401
from back.core.databricks.UnityCatalog import UnityCatalog  # noqa: F401
from back.core.databricks.VolumeFileService import VolumeFileService  # noqa: F401
from back.core.databricks.WorkspaceService import WorkspaceService  # noqa: F401
from back.core.databricks.DashboardService import DashboardService  # noqa: F401
from back.core.databricks.MetadataService import MetadataService  # noqa: F401
from back.core.databricks.UCDomainIO import UCDomainIO  # noqa: F401

# Backward-compatible wrappers for previously module-level functions
is_databricks_app = DatabricksAuth.is_databricks_app
normalize_host = DatabricksAuth.normalize_host
get_workspace_host = DatabricksAuth.get_workspace_host
build_metadata_dict = MetadataService.build_metadata_dict
validate_metadata = MetadataService.validate_metadata
has_metadata = MetadataService.has_metadata
get_catalog_schema_from_metadata = MetadataService.get_catalog_schema_from_metadata
extract_catalog_schema_from_full_name = (
    MetadataService.extract_catalog_schema_from_full_name
)
list_domains_from_uc = UCDomainIO.list_domains
load_domain_from_uc = UCDomainIO.load_domain

__all__ = [
    "DatabricksAuth",
    "LakebaseAuth",
    "get_lakebase_auth",
    "DatabricksClient",
    "SQLWarehouse",
    "UnityCatalog",
    "VolumeFileService",
    "WorkspaceService",
    "DashboardService",
    "MetadataService",
    "UCDomainIO",
    "is_databricks_app",
    "normalize_host",
    "get_workspace_host",
    "build_metadata_dict",
    "validate_metadata",
    "has_metadata",
    "get_catalog_schema_from_metadata",
    "extract_catalog_schema_from_full_name",
    "list_domains_from_uc",
    "load_domain_from_uc",
]
