"""Databricks client facade.

``DatabricksClient`` composes every domain-specific service class
so that call sites that need "a bit of everything" can use a single
entry point.  Prefer importing the individual service classes directly
for narrower, more testable dependencies.
"""
from typing import Optional

from .DatabricksAuth import DatabricksAuth
from .SQLWarehouse import SQLWarehouse
from .UnityCatalog import UnityCatalog
from .VolumeFileService import VolumeFileService
from .WorkspaceService import WorkspaceService
from .DashboardService import DashboardService


class DatabricksClient:
    """Thin facade that composes all Databricks service classes.

    Every sub-service is accessible as an attribute::

        client = DatabricksClient(host=…, token=…, warehouse_id=…)
        client.sql.execute_query("SELECT 1")
        client.catalog.get_catalogs()
        client.volumes.read_file("/Volumes/…")
        client.workspace.get_current_user_email()
        client.dashboards.get_dashboards()
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> None:
        self.auth = DatabricksAuth(host=host, token=token, warehouse_id=warehouse_id)
        self.sql = SQLWarehouse(self.auth)
        self.catalog = UnityCatalog(self.auth)
        self.volumes = VolumeFileService(auth=self.auth)
        self.workspace = WorkspaceService(self.auth)
        self.dashboards = DashboardService(self.auth)

    @property
    def host(self) -> str:
        return self.auth.host

    @property
    def token(self) -> str:
        return self.auth.token

    @property
    def warehouse_id(self) -> str:
        return self.auth.warehouse_id

    @property
    def is_app_mode(self) -> bool:
        return self.auth.is_app_mode

    def has_valid_auth(self) -> bool:
        return self.auth.has_valid_auth()

    def test_connection(self):
        return self.sql.test_connection()

    def execute_query(self, query):
        return self.sql.execute_query(query)

    def execute_statement(self, statement):
        return self.sql.execute_statement(statement)

    def create_or_replace_view(self, catalog, schema, view_name, select_sql):
        return self.sql.create_or_replace_view(catalog, schema, view_name, select_sql)

    def create_or_replace_table_from_query(self, catalog, schema, table_name, select_sql):
        return self.sql.create_or_replace_table_from_query(catalog, schema, table_name, select_sql)

    def get_warehouses(self):
        return self.sql.get_warehouses()

    def get_catalogs(self):
        return self.catalog.get_catalogs()

    def get_schemas(self, catalog):
        return self.catalog.get_schemas(catalog)

    def get_tables(self, catalog, schema):
        return self.catalog.get_tables(catalog, schema)

    def get_table_columns(self, catalog, schema, table):
        return self.catalog.get_table_columns(catalog, schema, table)

    def get_table_comment(self, catalog, schema, table):
        return self.catalog.get_table_comment(catalog, schema, table)

    def get_volumes(self, catalog, schema):
        return self.catalog.get_volumes(catalog, schema)

    def list_volumes(self, catalog, schema):
        return self.catalog.list_volumes(catalog, schema)

    def create_volume(self, catalog, schema, volume_name):
        return self.catalog.create_volume(catalog, schema, volume_name)

    def get_dashboards(self):
        return self.dashboards.get_dashboards()

    def get_dashboard_parameters(self, dashboard_id):
        return self.dashboards.get_dashboard_parameters(dashboard_id)

    def get_current_user_email(self):
        return self.workspace.get_current_user_email()

    def list_workspace_users(self, max_results=500):
        return self.workspace.list_users(max_results)

    def list_workspace_groups(self):
        return self.workspace.list_groups()

    def search_users(self, query, max_results=50):
        return self.workspace.search_users(query, max_results)

    def search_groups(self, query, max_results=50):
        return self.workspace.search_groups(query, max_results)

    def get_app_permissions(self, app_name):
        return self.workspace.get_app_permissions(app_name)

    def list_app_principals(self, app_name):
        return self.workspace.list_app_principals(app_name)

    def get_oauth_token(self):
        """Return an OAuth token from the Databricks auth layer."""
        return self.auth.get_oauth_token()

    def get_auth_headers(self):
        """Return authorization headers for REST API calls."""
        return self.auth.get_auth_headers()

    def get_sql_connection_params(self):
        """Return connection parameters for SQL Warehouse connections."""
        return self.auth.get_sql_connection_params()
