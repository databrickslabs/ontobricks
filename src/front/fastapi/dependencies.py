"""
FastAPI Dependencies - Shared across all routers

These dependencies handle common concerns like session, templates, and configuration.
"""
from fastapi import Request
from fastapi.templating import Jinja2Templates
from jinja2 import pass_context
import os
import random
import time as _time

from back.core.logging import get_logger
from shared.config.constants import APP_VERSION

logger = get_logger(__name__)

_asset_version = f"{APP_VERSION}.{int(_time.time())}"

# ===========================================
# Templates Configuration
# ===========================================

# Templates -- consolidated under front/templates/
_src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_template_dir = os.path.join(_src_dir, "front", "templates")
if not os.path.isdir(_template_dir):
    _template_dir = os.path.join(_src_dir, "app", "frontend", "home", "templates")

templates = Jinja2Templates(directory=_template_dir)

_partials_dir = os.path.join(_template_dir, "partials")
if os.path.isdir(_partials_dir) and _partials_dir not in templates.env.loader.searchpath:
    templates.env.loader.searchpath.append(_partials_dir)
    for _sub in ("layout", "ontology", "mapping", "dtwin", "project"):
        _sub_dir = os.path.join(_partials_dir, _sub)
        if os.path.isdir(_sub_dir) and _sub_dir not in templates.env.loader.searchpath:
            templates.env.loader.searchpath.append(_sub_dir)


# ===========================================
# Custom Jinja2 Functions
# ===========================================

@pass_context
def url_for(context: dict, endpoint: str, **values) -> str:
    """
    Jinja2 url_for function for templates.
    
    Supports:
    - url_for('static', filename='css/main.css') -> /static/css/main.css
    - url_for('home.home_page') -> /
    """
    request: Request = context.get("request")
    
    if endpoint == 'static':
        filename = values.get('filename', '')
        return f"/static/{filename}?v={_asset_version}"
    
    # For other endpoints, try to use request.url_for
    try:
        return str(request.url_for(endpoint, **values))
    except Exception:
        logger.debug("url_for fallback for endpoint '%s': route not found", endpoint)
        return f"/{endpoint}"


def range_filter(start_or_end: int, end: int = None) -> range:
    """Range filter for Jinja2 - accepts range(n) or range(start, end)."""
    if end is None:
        return range(start_or_end)
    return range(start_or_end, end)


def random_filter(seq):
    """Random filter for Jinja2 - returns random element from sequence."""
    return random.choice(list(seq))


@pass_context
def get_user_email(context: dict) -> str:
    """Return current user email from request.state (set by PermissionMiddleware)."""
    request: Request = context.get("request")
    if request and hasattr(request.state, 'user_email'):
        return request.state.user_email
    return ""


@pass_context
def get_user_role(context: dict) -> str:
    """Return current user role from request.state (set by PermissionMiddleware)."""
    request: Request = context.get("request")
    if request and hasattr(request.state, 'user_role'):
        return request.state.user_role
    return ""


# Add custom globals to Jinja2 environment
templates.env.globals['url_for'] = url_for
templates.env.globals['range'] = range_filter
templates.env.globals['get_user_email'] = get_user_email
templates.env.globals['get_user_role'] = get_user_role

# Menu configuration available in all templates as {{ menu_config }}
from front.config import get_menu_config, get_menu_by_id
templates.env.globals['menu_config'] = get_menu_config()
templates.env.globals['get_menu'] = get_menu_by_id

# App version from pyproject.toml (single source of truth) as {{ app_version }}
from shared.config.constants import MAX_NOTIFICATIONS
templates.env.globals['app_version'] = APP_VERSION
templates.env.globals['asset_version'] = _asset_version
templates.env.globals['max_notifications'] = MAX_NOTIFICATIONS

# Add custom filters
templates.env.filters['random'] = random_filter


# ===========================================
# Configuration Dependencies
# ===========================================
