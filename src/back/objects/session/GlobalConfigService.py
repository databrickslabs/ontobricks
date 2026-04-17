"""
Global configuration service for OntoBricks.

Manages instance-level settings (shared across sessions) in the registry UC
volume as ``.global_config.json``. Admins (CAN MANAGE) control **warehouse_id**
(SQL warehouse for UC), **default_base_uri**, and **default_emoji**.

In local (non-App) mode the same file applies when a registry exists; env vars
and fallbacks cover bootstrap and unconfigured deployments.
"""
import json
import time
from typing import Any, Dict, Optional, Tuple

from back.core.logging import get_logger
from back.core.databricks import VolumeFileService
from back.objects.registry.registry_cache import set_registry_cache_ttl

logger = get_logger(__name__)

_CONFIG_FILENAME = ".global_config.json"
_CACHE_TTL = 60  # seconds


class GlobalConfigService:
    """Read/write instance-wide configuration stored in the registry UC Volume."""

    def __init__(self):
        self._cache: Optional[Dict[str, Any]] = None
        self._cache_ts: float = 0.0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _config_path(registry_cfg: Dict[str, str]) -> str:
        from back.objects.registry import RegistryCfg
        c = RegistryCfg.from_dict(registry_cfg)
        return f"/Volumes/{c.catalog}/{c.schema}/{c.volume}/{_CONFIG_FILENAME}"

    @staticmethod
    def _new_uc(host: str, token: str) -> VolumeFileService:
        return VolumeFileService(host=host, token=token)

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
        """Load and cache the global config from the registry volume."""
        now = time.time()
        if (
            not force
            and self._cache is not None
            and (now - self._cache_ts) < _CACHE_TTL
        ):
            return self._cache

        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return self._empty()

        path = self._config_path(registry_cfg)
        try:
            uc = self._new_uc(host, token)
            ok, content, msg = uc.read_file(path)
            if ok and content:
                data = json.loads(content)
                if "schedule_history" in data:
                    del data["schedule_history"]
                    logger.info("Stripped legacy schedule_history from global config")
                for _sched in (data.get("schedules") or {}).values():
                    _sched.pop("registry_cfg", None)
                self._cache = data
                self._cache_ts = now
                if "registry_cache_ttl" in data:
                    set_registry_cache_ttl(int(data["registry_cache_ttl"]))
                logger.info("Loaded global config from %s", path)
                return data
            logger.debug("Global config not found or empty at %s: %s", path, msg)
        except Exception as e:
            logger.warning("Error loading global config: %s", e)

        empty = self._empty()
        self._cache = empty
        self._cache_ts = now
        return empty

    def get(
        self, host: str, token: str, registry_cfg: Dict[str, str], key: str, default: str = ""
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
        """Merge *updates* into the global config and persist to UC Volume."""
        if not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
            return False, "Registry not configured — set catalog and schema in Settings first"

        data = self.load(host, token, registry_cfg, force=True)
        data["version"] = data.get("version", 1)
        data.pop("schedule_history", None)
        data.update(updates)
        for _sched in (data.get("schedules") or {}).values():
            _sched.pop("registry_cfg", None)

        path = self._config_path(registry_cfg)
        try:
            uc = self._new_uc(host, token)
            ok, msg = uc.write_file(path, json.dumps(data, indent=2), overwrite=True)
            if not ok:
                logger.error("Failed to write global config to %s: %s", path, msg)
                return False, f"Failed to save global config: {msg}"
            self._cache = data
            self._cache_ts = time.time()
            logger.info("Saved global config updates %s to %s", list(updates.keys()), path)
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
            return False, f"Unknown graph engine '{engine}'. Allowed: {', '.join(self.ALLOWED_GRAPH_ENGINES)}"
        return self._save(host, token, registry_cfg, {"graph_engine": engine})

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
        }


global_config_service = GlobalConfigService()
