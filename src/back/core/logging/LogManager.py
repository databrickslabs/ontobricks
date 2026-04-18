import logging
import logging.config
import os
from pathlib import Path
from typing import Optional

from shared.config.constants import (
    APP_LOGGER_NAME,
    DEFAULT_LOG_FILE,
    DEFAULT_LOG_LEVEL,
    LOG_BACKUP_COUNT,
    LOG_MAX_BYTES,
)


class LogManager:
    """Centralised logging manager (singleton).

    Encapsulates log directory resolution, dict-config construction,
    and logger creation.  All state is held on the instance so callers
    can query the current configuration after setup.
    """

    _instance: Optional["LogManager"] = None
    _DATABRICKS_APP_LOG_CANDIDATES = ["/local_disk0/logs", "/tmp/logs"]

    def __init__(self) -> None:
        self._level: str = DEFAULT_LOG_LEVEL
        self._log_dir: Optional[str] = None
        self._log_file: Optional[str] = None
        self._log_path: Optional[str] = None
        self._configured: bool = False

    # ------------------------------------------------------------------
    # Singleton access
    # ------------------------------------------------------------------

    @classmethod
    def instance(cls) -> "LogManager":
        """Return the module-wide singleton, creating it on first call."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def level(self) -> str:
        """Current log level name (e.g. ``"DEBUG"``)."""
        return self._level

    @property
    def log_path(self) -> Optional[str]:
        """Absolute path to the rotating log file, or ``None`` before setup."""
        return self._log_path

    @property
    def is_configured(self) -> bool:
        """``True`` once :meth:`setup` has been called at least once."""
        return self._configured

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def setup(
        self,
        level: Optional[str] = None,
        log_dir: Optional[str] = None,
        log_file: Optional[str] = None,
    ) -> None:
        """Apply the dictConfig and log a one-line confirmation.

        Parameters
        ----------
        level : str, optional
            Python log level name. Falls back to ``LOG_LEVEL`` env var,
            then the default from ``global_config``.
        log_dir : str, optional
            Directory for the rotating log file.
            Falls back to ``LOG_DIR`` env var, then platform-dependent default.
        log_file : str, optional
            Log filename. Falls back to ``LOG_FILE`` env var, then
            ``ontobricks.log``.
        """
        if self._configured:
            return

        resolved_level = (level or os.getenv("LOG_LEVEL", DEFAULT_LOG_LEVEL)).upper()
        config = self._build_config(
            level=resolved_level,
            log_dir=log_dir,
            log_file=log_file,
        )
        logging.config.dictConfig(config)

        self._level = resolved_level
        self._log_dir = os.path.dirname(config["handlers"]["file"]["filename"])
        self._log_file = os.path.basename(config["handlers"]["file"]["filename"])
        self._log_path = config["handlers"]["file"]["filename"]
        self._configured = True

        logger = logging.getLogger(APP_LOGGER_NAME)
        logger.info(
            "Logging configured — level=%s, console=stdout, file=%s",
            resolved_level,
            self._log_path,
        )

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """Return a child logger under the application namespace.

        Module prefixes ``back.``, ``front.``, ``shared.``, ``api.``, and
        the legacy ``app.`` are replaced by the application logger name so
        all modules share the same hierarchy.
        """
        if name is None:
            return logging.getLogger(APP_LOGGER_NAME)
        for prefix in ("back.", "front.", "shared.", "api.", "app."):
            if name.startswith(prefix):
                name = f"{APP_LOGGER_NAME}.{name[len(prefix):]}"
                break
        return logging.getLogger(name)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_log_dir(log_dir: Optional[str] = None) -> str:
        """Return a writable log directory, creating it if needed.

        On Databricks Apps the container filesystem is restricted, so we
        try several candidate paths and fall back to ``/tmp/logs``.
        """
        candidates: list[str] = []

        if log_dir:
            candidates.append(log_dir)
        elif os.getenv("LOG_DIR"):
            candidates.append(os.getenv("LOG_DIR"))
        elif os.getenv("DATABRICKS_APP_PORT"):
            candidates.extend(LogManager._DATABRICKS_APP_LOG_CANDIDATES)
        else:
            candidates.append(os.path.join(os.getcwd(), "logs"))

        for d in candidates:
            try:
                Path(d).mkdir(parents=True, exist_ok=True)
                return d
            except OSError:
                continue

        fallback = "/tmp/logs"
        Path(fallback).mkdir(parents=True, exist_ok=True)
        return fallback

    @classmethod
    def _build_config(
        cls,
        level: str = "INFO",
        log_dir: Optional[str] = None,
        log_file: Optional[str] = None,
    ) -> dict:
        """Build a logging dict-config (log4J-style hierarchy)."""
        resolved_dir = cls._resolve_log_dir(log_dir)
        resolved_file = log_file or os.getenv("LOG_FILE", DEFAULT_LOG_FILE)
        log_path = os.path.join(resolved_dir, resolved_file)

        return {
            "version": 1,
            "disable_existing_loggers": False,
            # ── Formatters ──────────────────────────────────────────
            "formatters": {
                "detailed": {
                    "format": (
                        "%(asctime)s | %(levelname)-8s | %(name)s | "
                        "%(module)s.%(funcName)s:%(lineno)d | %(message)s"
                    ),
                    "datefmt": "%Y-%m-%dT%H:%M:%S%z",
                },
                "console": {
                    "format": (
                        "%(levelname)-8s | %(name)s | "
                        "%(module)s.%(funcName)s:%(lineno)d | %(message)s"
                    ),
                },
                "brief": {
                    "format": "%(levelname)-8s | %(message)s",
                },
            },
            # ── Handlers ────────────────────────────────────────────
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level,
                    "formatter": "console",
                    "stream": "ext://sys.stdout",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "level": level,
                    "formatter": "detailed",
                    "filename": log_path,
                    "maxBytes": LOG_MAX_BYTES,
                    "backupCount": LOG_BACKUP_COUNT,
                    "encoding": "utf-8",
                },
            },
            # ── Loggers ─────────────────────────────────────────────
            "loggers": {
                APP_LOGGER_NAME: {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn": {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn.access": {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "uvicorn.error": {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "fastapi": {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
                "apscheduler": {
                    "level": level,
                    "handlers": ["console", "file"],
                    "propagate": False,
                },
            },
            # ── Root logger (catch-all) ────────────────────────────
            "root": {
                "level": "WARNING",
                "handlers": ["console", "file"],
            },
        }
