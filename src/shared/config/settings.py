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
    databricks_triplestore_table: str = ""
    databricks_sql_warehouse_id: str = ""

    @property
    def sql_warehouse_id(self) -> str:
        """Alias used by resolve_warehouse_id()."""
        return self.databricks_sql_warehouse_id

    # Domain Registry (single Volume for all domains) — used solely for
    # domain-scoped binary artefacts (the documents/ uploads imported
    # by the ontology designer). Structured registry data (domains,
    # versions, permissions, schedules, global config) lives in
    # Lakebase as of v0.4.0.
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

    # Lakebase: branch within the project to connect to.
    # In production the Apps runtime resolves the branch implicitly via
    # the ``database`` resource binding (PGHOST already encodes the
    # branch endpoint). In local dev, set this together with
    # ``LAKEBASE_PROJECT`` so ``LakebaseAuth`` can resolve the
    # endpoint hostname without requiring the raw URL.
    lakebase_branch: str = "main"

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

    # Knowledge-graph analytics safety guard: the maximum number of triples
    # loaded into memory for the NetworkX centrality / structure analysis.
    # Graphs larger than this are rejected before compute (the Analytics page
    # warns up-front using the known triple count, so the user never waits for
    # a background task only to see it fail). Raise with care — the full triple
    # set is held in memory during the computation.
    analytics_max_triples: int = Field(
        default=500_000,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_MAX_TRIPLES",
            "analytics_max_triples",
        ),
    )

    model_config = ConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        # ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER`` and
        # ``LAKEBASE_PROJECT`` are consumed directly via
        # ``os.environ`` by :class:`back.core.databricks.LakebaseAuth`
        # — they don't need to be Pydantic fields. ``ignore`` keeps
        # the .env file tolerant of extra Lakebase-related entries.
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
