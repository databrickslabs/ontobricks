"""Shared constants for the Databricks integration layer."""

_OAUTH_TOKEN_TTL = 3000  # 50 minutes (tokens usually last 1 hour)
_SQL_SOCKET_TIMEOUT = 30
_REQUEST_TIMEOUT = 30

_LAKEVIEW_PAGE_SIZE = 1000
_LAKEVIEW_MAX_PAGES = 20
_LEGACY_PAGE_SIZE = 250
_LEGACY_MAX_PAGES = 20

API_PREFIX = "/api/2.0"
FS_FILES_PATH = f"{API_PREFIX}/fs/files"
FS_DIRS_PATH = f"{API_PREFIX}/fs/directories"
SQL_WAREHOUSES_PATH = f"{API_PREFIX}/sql/warehouses"
SCIM_ME_PATH = f"{API_PREFIX}/preview/scim/v2/Me"
SCIM_USERS_PATH = f"{API_PREFIX}/preview/scim/v2/Users"
SCIM_GROUPS_PATH = f"{API_PREFIX}/preview/scim/v2/Groups"
PERMISSIONS_APPS_PATH = f"{API_PREFIX}/permissions/apps"
LAKEVIEW_DASHBOARDS_PATH = f"{API_PREFIX}/lakeview/dashboards"
LEGACY_DASHBOARDS_PATH = f"{API_PREFIX}/preview/sql/dashboards"
