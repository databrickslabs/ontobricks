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

    # Knowledge-graph analytics: the maximum number of triples loaded into
    # memory for the NetworkX centrality / structure analysis. This is the
    # switch point between the two compute modes, not a hard rejection —
    # graphs above it fall back to engine-side SQL aggregation (see
    # ``analytics_pushdown_enabled``). Raise with care: the full triple set is
    # held in memory during the in-memory computation.
    analytics_max_triples: int = Field(
        default=500_000,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_MAX_TRIPLES",
            "analytics_max_triples",
        ),
    )

    # Whether graphs over ``analytics_max_triples`` fall back to the SQL
    # pushdown path. Disable to restore the previous behaviour of rejecting
    # oversized graphs outright.
    analytics_pushdown_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_PUSHDOWN_ENABLED",
            "analytics_pushdown_enabled",
        ),
    )

    # How many top-ranked nodes per metric the pushdown path returns. The
    # Analytics page "Top N" selector is capped well below this, so the
    # returned set is always a superset of what the UI can chart while keeping
    # the persisted payload bounded on graphs of any size.
    analytics_top_n: int = Field(
        default=100,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_TOP_N",
            "analytics_top_n",
        ),
    )

    # Whether an oversized graph may offload PageRank / connected components /
    # clustering / sampled betweenness+closeness to the serverless Lakeflow job
    # (``resources/graph_analytics.job.yml``). Opt-in, because it only works
    # once the bundle carrying that job has been deployed and the graph data is
    # reachable from Spark as a Unity Catalog table. With this off, those
    # metrics stay unavailable above the cap rather than being computed.
    #
    # This is the *deployment default* only. The effective value comes from
    # :meth:`DatabricksHelpers.resolve_analytics_job_enabled`, which prefers the
    # admin toggle in Settings → Global when one has been set. Read that
    # resolver rather than this field.
    analytics_job_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_ENABLED",
            "analytics_job_enabled",
        ),
    )

    # Deployed name of that job. Empty means "derive it from the app name" as
    # ``<app>-graph-analytics``, matching the bundle. A bundle deployed in
    # development mode prefixes the name with ``[dev <user>] ``, which the
    # runner's lookup allows for.
    analytics_job_name: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_NAME",
            "analytics_job_name",
        ),
    )

    # Unity Catalog schema (``catalog.schema``) that holds the job's per-node
    # output tables. Empty means "use the registry catalog/schema".
    analytics_job_output_schema: str = Field(
        default="",
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_OUTPUT_SCHEMA",
            "analytics_job_output_schema",
        ),
    )

    # How long to follow a job run before giving up on it. The run itself is
    # not cancelled on timeout — the task simply stops waiting and says so.
    analytics_job_timeout_s: int = Field(
        default=3600,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_TIMEOUT_S",
            "analytics_job_timeout_s",
        ),
    )

    # PageRank power iterations the job runs. 20 fixes the top-N ordering;
    # raise it if you need converged absolute scores rather than a ranking.
    analytics_job_pagerank_iterations: int = Field(
        default=20,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_PAGERANK_ITERATIONS",
            "analytics_job_pagerank_iterations",
        ),
    )

    # Source nodes sampled for betweenness and closeness (Brandes-Pich pivots).
    # Exact betweenness is O(V*E), which is not viable at the sizes this job
    # exists for, so both are estimated from a sample and labelled as
    # approximate in the UI. This is the job's dominant cost — the intermediate
    # BFS holds one row per (pivot, reachable node), so 128 pivots over a
    # 1M-node graph is a ~128M-row shuffle. 0 skips both metrics; a value at or
    # above the node count makes them exact.
    analytics_job_pivots: int = Field(
        default=64,
        validation_alias=AliasChoices(
            "ONTOBRICKS_ANALYTICS_JOB_PIVOTS",
            "analytics_job_pivots",
        ),
    )

    model_config = ConfigDict(
        env_prefix="",
        case_sensitive=False,
        env_file=".env",
        # ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER`` and
        # ``LAKEBASE_PROJECT`` are consumed directly via
        # ``os.environ`` by :class:`back.core.databricks.lakebase.LakebaseAuth`
        # — they don't need to be Pydantic fields. ``ignore`` keeps
        # the .env file tolerant of extra Lakebase-related entries.
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
