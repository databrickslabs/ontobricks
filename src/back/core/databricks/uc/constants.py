"""Constants for Unity Catalog REST / Files API calls."""

from back.core.databricks.constants import API_PREFIX, _REQUEST_TIMEOUT

FS_FILES_PATH = f"{API_PREFIX}/fs/files"
FS_DIRS_PATH = f"{API_PREFIX}/fs/directories"

__all__ = ["FS_FILES_PATH", "FS_DIRS_PATH", "_REQUEST_TIMEOUT"]
