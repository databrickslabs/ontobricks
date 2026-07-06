"""Shared technical access layer for Lakebase (Postgres).

Single home for the Lakebase connection machinery used by the two
independent Lakebase databases in OntoBricks — the registry store and the
graph triple store. Consumers import the pool / auth / grant primitives from
here and supply their own connection coordinates (auth + schema + database).
"""

from back.core.databricks.lakebase.grants import (  # noqa: F401
    grant_can_use_on_project,
    grant_schema_privileges,
    grant_uc_catalog,
    resolve_app_service_principals,
)
from back.core.databricks.lakebase.LakebaseAuth import (  # noqa: F401
    BranchLakebaseAuth,
    LakebaseAuth,
    get_lakebase_auth,
)
from back.core.databricks.lakebase.LakebaseConnectionPool import (  # noqa: F401
    LakebaseConnectionError,
    LakebaseConnectionPool,
    get_lakebase_pool,
    lakebase_cursor,
)
from back.core.databricks.lakebase.psycopg_gate import require_psycopg  # noqa: F401

__all__ = [
    # connection
    "LakebaseConnectionError",
    "LakebaseConnectionPool",
    "get_lakebase_pool",
    "lakebase_cursor",
    "require_psycopg",
    # auth
    "LakebaseAuth",
    "BranchLakebaseAuth",
    "get_lakebase_auth",
    # grants
    "resolve_app_service_principals",
    "grant_can_use_on_project",
    "grant_schema_privileges",
    "grant_uc_catalog",
]
