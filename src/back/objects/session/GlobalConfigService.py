"""
Global configuration service for OntoBricks.

Manages instance-level settings (shared across sessions). Persisted via
the active :class:`back.objects.registry.store.RegistryStore`:

- Volume backend → ``.global_config.json`` on the UC Volume.
- Lakebase backend → ``global_config`` row on Postgres.

Admins (CAN MANAGE) control **warehouse_id** (SQL warehouse for UC),
**default_base_uri**, **default_emoji**, the registry ``backend``, and
the Lakebase ``schema`` name.

In local (non-App) mode the same persistence applies when a registry
exists; env vars and fallbacks cover bootstrap and unconfigured
deployments.
"""

import time
from typing import Any, Dict, Optional, Tuple

from back.core.logging import get_logger
from back.objects.registry.registry_cache import set_registry_cache_ttl

logger = get_logger(__name__)

_CACHE_TTL = 60  # seconds


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
        """Build the right :class:`RegistryStore` for *registry_cfg*."""
        from back.objects.registry import RegistryCfg
        from back.objects.registry.store import RegistryFactory

        cfg = RegistryCfg.from_dict(registry_cfg)
        return RegistryFactory.from_cfg(cfg, host=host, token=token)

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
                if "schedule_history" in data:
                    del data["schedule_history"]
                    logger.info("Stripped legacy schedule_history from global config")
                for _sched in (data.get("schedules") or {}).values():
                    _sched.pop("registry_cfg", None)
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
        data.pop("schedule_history", None)
        data.update(updates)
        for _sched in (data.get("schedules") or {}).values():
            _sched.pop("registry_cfg", None)

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

    ALLOWED_GRAPH_ENGINES = ("ladybug",)

    def get_graph_engine(
        self, host: str, token: str, registry_cfg: Dict[str, str]
    ) -> str:
        """Return the globally configured graph DB engine name."""
        val = self.get(host, token, registry_cfg, "graph_engine", "ladybug")
        return val if val in self.ALLOWED_GRAPH_ENGINES else "ladybug"

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
            "registry_cache_ttl": 300,
            "graph_engine": "ladybug",
            "graph_engine_config": {},
        }


global_config_service = GlobalConfigService()
