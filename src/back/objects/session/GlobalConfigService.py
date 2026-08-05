"""
Global configuration service for OntoBricks.

Manages instance-level settings (shared across sessions). Persisted via
the active :class:`back.objects.registry.store.RegistryStore`:

- Volume backend → ``.global_config.json`` on the UC Volume.
- Lakebase backend → ``global_config`` row on Postgres.

Includes **graph_engine** / **graph_engine_config** with warehouse_id,
default_base_uri, registry ``backend``, Lakebase ``schema`` name, etc.

In local (non-App) mode the same persistence applies when a registry
exists; env vars and fallbacks cover bootstrap and unconfigured
deployments.
"""

import time
from typing import Any, Dict, Optional, Tuple

from back.core.logging import get_logger
from back.objects.registry.registry_cache import set_registry_cache_ttl

logger = get_logger(__name__)

_CACHE_TTL = 300  # seconds — admin-only settings rarely change

# When a backend fetch fails but we still hold a previous (non-empty)
# cache, keep serving it for up to this many seconds before giving up.
# This stops a single Lakebase outage from cascading into multi-second
# request hangs across every endpoint that resolves the graph engine.
_STALE_CACHE_TTL = 30 * 60  # 30 minutes


class GlobalConfigService:
    """Read/write instance-wide configuration via the active store."""

    def __init__(self):
        self._cache: Optional[Dict[str, Any]] = None
        self._cache_ts: float = 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _store_for(host: str, token: str, registry_cfg: Dict[str, str]):
        """Build the Lakebase :class:`RegistryStore` for *registry_cfg*.

        ``host``/``token`` are kept on the signature for backwards
        compatibility with the many call sites that thread them through;
        the Lakebase store sources its credentials from the
        ``PG*`` env vars + Lakebase JWT, so they are ignored here.
        """
        from back.objects.registry import RegistryCfg
        from back.objects.registry.store import RegistryFactory

        del host, token
        cfg = RegistryCfg.from_dict(registry_cfg)
        return RegistryFactory.from_cfg(cfg)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        *,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Load and cache the global config from the active store."""
        now = time.time()
        if (
            not force
            and self._cache is not None
            and (now - self._cache_ts) < _CACHE_TTL
        ):
            return self._cache

        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return self._empty()

        try:
            store = self._store_for(host, token, registry_cfg)
            data = store.load_global_config()
            if data:
                self._cache = data
                self._cache_ts = now
                if "registry_cache_ttl" in data:
                    set_registry_cache_ttl(int(data["registry_cache_ttl"]))
                logger.info(
                    "Loaded global config (backend=%s)", store.backend
                )
                return data
        except Exception as e:
            logger.warning("Error loading global config: %s", e)
            # Stale-while-revalidate: if we held a non-empty cache that's
            # still within the stale window, keep serving it rather than
            # falling back to ``_empty()`` and forcing every downstream
            # endpoint to re-hit the backend on the next request.
            if (
                self._cache
                and (now - self._cache_ts) < _STALE_CACHE_TTL
            ):
                logger.info(
                    "Serving stale global config (age=%.1fs)",
                    now - self._cache_ts,
                )
                return self._cache

        empty = self._empty()
        self._cache = empty
        self._cache_ts = now
        return empty

    def get(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        key: str,
        default: str = "",
    ) -> str:
        """Return a single value from the global config."""
        data = self.load(host, token, registry_cfg)
        return data.get(key, default)

    def get_warehouse_id(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured SQL Warehouse ID (or empty string)."""
        return self.get(host, token, registry_cfg, "warehouse_id")

    def get_default_base_uri(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured default base URI domain."""
        return self.get(host, token, registry_cfg, "default_base_uri")

    def get_default_emoji(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured default class icon."""
        return self.get(host, token, registry_cfg, "default_emoji")

    def get_navbar_logo(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured navbar logo as a ``data:`` URL.

        Empty string means "no custom logo" — the UI falls back to the
        bundled default (``static/global/img/favicon.svg``).
        """
        return self.get(host, token, registry_cfg, "navbar_logo")

    def get_use_cloud_fetch(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> bool:
        """Return whether CloudFetch is globally enabled.

        Defaults to ``True`` when the key is absent so existing deployments
        keep CloudFetch enabled unless an admin explicitly disables it.
        """
        raw = self.load(host, token, registry_cfg).get("use_cloud_fetch", True)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return True

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def _save(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        updates: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """Merge *updates* into the global config and persist via the store."""
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return (
                False,
                "Registry not configured — set catalog and schema in Settings first",
            )

        data = self.load(host, token, registry_cfg, force=True)
        data["version"] = data.get("version", 1)
        data.update(updates)

        try:
            store = self._store_for(host, token, registry_cfg)
            ok, msg = store.save_global_config(data)
            if not ok:
                logger.error("Failed to write global config: %s", msg)
                return False, f"Failed to save global config: {msg}"
            self._cache = data
            self._cache_ts = time.time()
            logger.info(
                "Saved global config updates %s (backend=%s)",
                list(updates.keys()),
                store.backend,
            )
            return True, "Global configuration saved"
        except Exception as e:
            logger.exception("Error saving global config: %s", e)
            return False, str(e)

    def set_warehouse_id(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        warehouse_id: str,
    ) -> Tuple[bool, str]:
        """Persist a new SQL Warehouse ID in the global config file."""
        return self._save(host, token, registry_cfg, {"warehouse_id": warehouse_id})

    def set_default_base_uri(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        base_uri: str,
    ) -> Tuple[bool, str]:
        """Persist a new default base URI domain in the global config file."""
        return self._save(host, token, registry_cfg, {"default_base_uri": base_uri})

    def set_default_emoji(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        emoji: str,
    ) -> Tuple[bool, str]:
        """Persist a new default class icon in the global config file."""
        return self._save(host, token, registry_cfg, {"default_emoji": emoji})

    def set_use_cloud_fetch(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        enabled: bool,
    ) -> Tuple[bool, str]:
        """Persist global CloudFetch on/off toggle in the global config file."""
        return self._save(host, token, registry_cfg, {"use_cloud_fetch": bool(enabled)})

    def set_navbar_logo(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        data_url: str,
    ) -> Tuple[bool, str]:
        """Persist the navbar logo as a ``data:`` URL (empty string clears it)."""
        return self._save(host, token, registry_cfg, {"navbar_logo": data_url or ""})

    ALLOWED_GRAPH_ENGINES = ("lakebase",)

    def get_graph_engine(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured graph DB engine name.

        Currently always resolves to ``"lakebase"``.  Extra engines plug
        in by adding their key to :data:`ALLOWED_GRAPH_ENGINES` and
        registering a backend class under
        :class:`back.core.graphdb.GraphDBFactory`.
        """
        val = self.get(host, token, registry_cfg, "graph_engine", "lakebase")
        return val if val in self.ALLOWED_GRAPH_ENGINES else "lakebase"

    def set_graph_engine(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        engine: str,
    ) -> Tuple[bool, str]:
        """Persist a new graph DB engine selection in the global config file."""
        engine = (engine or "").strip().lower()
        if engine not in self.ALLOWED_GRAPH_ENGINES:
            return (
                False,
                f"Unknown graph engine '{engine}'. Allowed: {', '.join(self.ALLOWED_GRAPH_ENGINES)}",
            )
        return self._save(host, token, registry_cfg, {"graph_engine": engine})

    def get_graph_engine_config(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> Dict[str, Any]:
        """Return the engine-specific configuration dict (free-form JSON)."""
        data = self.load(host, token, registry_cfg)
        cfg = data.get("graph_engine_config")
        return cfg if isinstance(cfg, dict) else {}

    def set_graph_engine_config(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        config: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """Persist the engine-specific configuration dict."""
        if not isinstance(config, dict):
            return False, "graph_engine_config must be a JSON object"
        from back.core.graphdb.lakebase.LakebaseBase import validate_engine_config_keys

        ok_keys, msg_keys = validate_engine_config_keys(config)
        if not ok_keys:
            return False, msg_keys
        return self._save(host, token, registry_cfg, {"graph_engine_config": config})

    def get_registry_cache_ttl(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> int:
        """Return the configured registry cache TTL in seconds."""
        val = self.get(host, token, registry_cfg, "registry_cache_ttl", "")
        if val and str(val).isdigit():
            return int(val)
        from back.objects.registry.registry_cache import get_registry_cache_ttl

        return get_registry_cache_ttl()

    def set_registry_cache_ttl(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        ttl: int,
    ) -> Tuple[bool, str]:
        """Persist a new registry cache TTL (seconds) in the global config file."""
        ttl = max(10, int(ttl))
        set_registry_cache_ttl(ttl)
        return self._save(host, token, registry_cfg, {"registry_cache_ttl": ttl})

    def get_edit_lock_ttl_s(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> Optional[int]:
        """Return the admin-set edit-lock lease TTL (seconds), or ``None``.

        ``None`` means "not set in the global config" — the caller
        (:meth:`EditLockService._ttl_seconds`) then falls back to the
        ``ONTOBRICKS_EDIT_LOCK_TTL_S`` env var / built-in default. Deliberately
        absent from :meth:`_empty` so an unconfigured / failed load yields the
        empty sentinel rather than masking the env fallback. ``0`` is a valid
        stored value (disables the lease).
        """
        val = self.get(host, token, registry_cfg, "edit_lock_ttl_s", "")
        s = str(val).strip()
        if s.lstrip("-").isdigit():
            return max(0, int(s))
        return None

    def set_edit_lock_ttl_s(
        self,
        host: str,
        token: str,
        registry_cfg: Dict[str, str],
        ttl_s: int,
    ) -> Tuple[bool, str]:
        """Persist the edit-lock lease TTL (seconds; ``0`` disables the lease)."""
        return self._save(
            host, token, registry_cfg, {"edit_lock_ttl_s": max(0, int(ttl_s))}
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _empty() -> Dict[str, Any]:
        return {
            "version": 1,
            "warehouse_id": "",
            "default_base_uri": "",
            "default_emoji": "",
            "navbar_logo": "",
            "use_cloud_fetch": True,
            "registry_cache_ttl": 300,
            "graph_engine": "lakebase",
            "graph_engine_config": {},
        }


global_config_service = GlobalConfigService()
