"""Application settings (environment / .env) via Pydantic Settings.

Used across the codebase (HTML routes, objects, external ``api`` package, FastAPI).
"""

from pydantic_settings import BaseSettings
from pydantic import AliasChoices, ConfigDict, Field
from functools import lru_cache
import os


def _get_default_session_dir() -> str:
    """Get the default session directory based on environment."""
    if os.getenv("DATABRICKS_APP_PORT"):
        return "/tmp/ontobricks_session"
    return "./fastapi_session"


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # App settings
    secret_key: str = "dev-secret-key-change-in-prod"

    # Databricks settings
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_catalog: str = "main"
    databricks_schema: str = "default"
    databricks_triplestore_table: str = ""
    databricks_sql_warehouse_id: str = ""

    @property
    def sql_warehouse_id(self) -> str:
        """Alias used by resolve_warehouse_id()."""
        return self.databricks_sql_warehouse_id

    # Domain Registry — storage backend
    # ``auto`` (default) prefers Lakebase when the Apps runtime has bound
    # the Postgres ``database`` resource (PG* env vars present) AND the
    # optional ``psycopg`` extra is installed; otherwise falls back to
    # ``volume``. Operators can pin the choice with
    # ``REGISTRY_BACKEND=volume`` or ``REGISTRY_BACKEND=lakebase``.
    # ``volume`` keeps the original JSON-on-UC-Volume layout.
    # ``lakebase`` stores registry-shaped data in a Postgres schema on
    # Databricks Lakebase; binaries (documents/, *.lbug.tar.gz) stay on
    # the Volume regardless of this choice. Resolution lives in
    # :func:`back.objects.registry.resolve_default_backend`.
    registry_backend: str = "auto"

    # Domain Registry (single Volume for all domains) — used by both
    # backends for binary artifacts and by the volume backend for
    # everything else.
    registry_volume_path: str = ""
    registry_catalog: str = ""
    registry_schema: str = ""
    registry_volume: str = "OntoBricksRegistry"

    # Lakebase: Postgres schema where the registry tables live.
    # Connection parameters (PGHOST/PGPORT/PGDATABASE/PGUSER) come from
    # the Databricks App database resource binding at runtime; the OAuth
    # token used as password is minted by ``LakebaseAuth`` via the
    # workspace SDK.
    lakebase_schema: str = "ontobricks_registry"

    # Lakebase: optional override of the Postgres database name. When
    # empty (the default), the Lakebase backend uses ``PGDATABASE`` as
    # auto-injected by the Apps runtime. Setting this picks a different
    # database on the *same* bound Lakebase instance — useful when the
    # admin wants to change the registry database without redeploying
    # the bundle. The service principal must have ``CONNECT`` on the
    # target database. The JWT scope is per-instance so no token
    # re-mint is needed.
    lakebase_database: str = ""

    # Databricks App name (for permission management).
    # Reads ``ONTOBRICKS_APP_NAME`` first (explicit override, e.g. via .env
    # for local dev), then falls back to ``DATABRICKS_APP_NAME`` which the
    # Databricks Apps runtime auto-injects as the deployed app's name
    # (e.g. ``ontobricks`` for prod, ``ontobricks-dev`` for the sandbox).
    # This lets the same ``app.yaml`` and source tree power multiple
    # Databricks App deployments without requiring a per-app override.
    ontobricks_app_name: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ONTOBRICKS_APP_NAME",
            "DATABRICKS_APP_NAME",
        ),
    )

    # Session settings - use /tmp in Databricks Apps
    session_dir: str = _get_default_session_dir()
    session_max_age: int = 86400  # 24 hours

    model_config = ConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        # ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER`` and
        # ``DATABASE_INSTANCE_NAME`` are consumed directly via
        # ``os.environ`` by :class:`back.core.databricks.LakebaseAuth`
        # — they don't need to be Pydantic fields. ``ignore`` keeps
        # the .env file tolerant of extra Lakebase-related entries.
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
