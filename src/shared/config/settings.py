"""Application settings (environment / .env) via Pydantic Settings.

Used across the codebase (HTML routes, objects, external ``api`` package, FastAPI).
"""
from pydantic_settings import BaseSettings
from pydantic import ConfigDict
from functools import lru_cache
import os


def _get_default_session_dir() -> str:
    """Get the default session directory based on environment."""
    if os.getenv('DATABRICKS_APP_PORT'):
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

    # Project Registry (single Volume for all projects)
    registry_volume_path: str = ""
    registry_catalog: str = ""
    registry_schema: str = ""
    registry_volume: str = "OntoBricksRegistry"

    # Databricks App name (for permission management)
    ontobricks_app_name: str = ""

    # Session settings - use /tmp in Databricks Apps
    session_dir: str = _get_default_session_dir()
    session_max_age: int = 86400  # 24 hours

    model_config = ConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
