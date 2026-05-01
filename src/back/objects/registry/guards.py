"""FastAPI dependencies for declarative permission gates.

The middleware (:mod:`shared.fastapi.main`) resolves the caller's app
and domain roles and stores them on ``request.state``. These guards
turn that into per-route declarative checks::

    @router.post(
        "/teams",
        dependencies=[Depends(require(ROLE_ADMIN))],
    )
    async def teams_save_batch(...): ...

Two scopes are supported:

* ``scope="app"`` (default) reads ``request.state.user_role`` -- the
  Databricks-App-level role (``admin`` / ``app_user`` / ``none``).
* ``scope="domain"`` reads ``request.state.user_domain_role`` and
  falls back to ``user_role`` so admins (who bypass the domain gate)
  still pass.

Below-min callers raise :class:`back.core.errors.AuthorizationError`
which surfaces as a structured 403 via the global exception handler.
"""

from __future__ import annotations

from typing import Callable

from fastapi import Request

from back.core.errors import AuthorizationError
from back.objects.registry.PermissionService import role_level


_VALID_SCOPES = ("app", "domain")


def require(min_role: str, *, scope: str = "app") -> Callable[[Request], str]:
    """Build a FastAPI dependency that enforces a minimum role.

    Args:
        min_role: One of the ``ROLE_*`` constants from
            :mod:`back.objects.registry`. The caller's resolved role
            must be at this level or higher (per
            :func:`role_level`).
        scope: ``"app"`` to gate on the app-level role,
            ``"domain"`` to gate on the loaded domain's role.

    Returns:
        A dependency callable that returns the caller's resolved role
        (so handlers can read it directly from the dep result if
        needed) and raises :class:`AuthorizationError` otherwise.
    """
    if scope not in _VALID_SCOPES:
        raise ValueError(
            f"require(scope=...) must be one of {_VALID_SCOPES!r}, got {scope!r}"
        )

    min_level = role_level(min_role)

    def _dep(request: Request) -> str:
        if scope == "domain":
            actual = (
                getattr(request.state, "user_domain_role", None)
                or getattr(request.state, "user_role", "")
                or ""
            )
        else:
            actual = getattr(request.state, "user_role", "") or ""

        if role_level(actual) < min_level:
            raise AuthorizationError(
                f"This action requires the '{min_role}' role or higher"
            )
        return actual

    _dep.__name__ = f"require_{scope}_{min_role}"
    return _dep
