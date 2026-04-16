"""Frontend menu configuration loader.

Loads menu_config.json and exposes it for Jinja2 templates.
"""
import json
import os
from functools import lru_cache
from typing import Optional

from back.core.logging import get_logger

logger = get_logger(__name__)

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "menu_config.json")


@lru_cache(maxsize=1)
def get_menu_config() -> dict:
    """Load and cache the menu configuration from JSON."""
    try:
        with open(_CONFIG_PATH, encoding="utf-8") as fh:
            config = json.load(fh)
        logger.debug("Menu config loaded: %d menus", len(config.get("menus", [])))
        return config
    except FileNotFoundError:
        logger.error("Menu config not found at %s", _CONFIG_PATH)
        return {"brand": {"label": "OntoBricks", "route": "/"}, "menus": [], "utility_links": []}
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in menu config: %s", exc)
        return {"brand": {"label": "OntoBricks", "route": "/"}, "menus": [], "utility_links": []}


def get_menu_by_id(menu_id: str) -> Optional[dict]:
    """Return a single menu entry by its id, or None."""
    for menu in get_menu_config().get("menus", []):
        if menu["id"] == menu_id:
            return menu
    return None
