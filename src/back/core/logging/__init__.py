"""Centralized Logging Configuration — log4J-style via dictConfig.

Usage:
    from back.core.logging import setup_logging, get_logger
    setup_logging()
    logger = get_logger(__name__)
"""

import logging
from typing import Optional

from back.core.logging.LogManager import LogManager  # noqa: F401


def setup_logging(
    level: Optional[str] = None,
    log_dir: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """Apply the dictConfig — delegates to :meth:`LogManager.setup`."""
    LogManager.instance().setup(level=level, log_dir=log_dir, log_file=log_file)


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a child logger — delegates to :meth:`LogManager.get_logger`."""
    return LogManager.instance().get_logger(name)


__all__ = ["LogManager", "setup_logging", "get_logger"]
