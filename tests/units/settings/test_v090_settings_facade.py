"""SettingsService facade (0.9.0 Extract Class refactor).

Covers the admin gate (``require_admin_error``), ``_resolve_context``,
``is_registry_locked`` and the contract that every one-line delegator
forwards *all* its arguments to the extracted settings class and returns
that class' result unchanged.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import re
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from back.core.errors import AuthorizationError
from back.objects.domain.settings.SettingsService import SettingsService

pytestmark = pytest.mark.unit

_ss = importlib.import_module("back.objects.domain.settings.SettingsService")


# ---------------------------------------------------------------------------
# require_admin_error
# ---------------------------------------------------------------------------


class TestRequireAdminError:
    def test_local_mode_skips_permission_check(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: False)
        perm = MagicMock()
        monkeypatch.setattr(_ss, "permission_service", perm)

        assert SettingsService.require_admin_error("u@x", "tok", MagicMock(), MagicMock()) is None
        perm.is_admin.assert_not_called()

    def test_app_mode_admin_passes(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        monkeypatch.setattr(
            SettingsService,
            "_resolve_context",
            staticmethod(lambda sm, st: (MagicMock(), "https://h", "sp-token", {})),
        )
        perm = MagicMock()
        perm.is_admin.return_value = True
        monkeypatch.setattr(_ss, "permission_service", perm)
        settings = MagicMock(ontobricks_app_name="ontobricks-app")

        SettingsService.require_admin_error("admin@x", "user-tok", MagicMock(), settings)

        perm.is_admin.assert_called_once_with(
            "admin@x", "https://h", "sp-token", "ontobricks-app", user_token="user-tok"
        )

    def test_app_mode_non_admin_raises_authorization_error(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        monkeypatch.setattr(
            SettingsService,
            "_resolve_context",
            staticmethod(lambda sm, st: (MagicMock(), "h", "t", {})),
        )
        perm = MagicMock()
        perm.is_admin.return_value = False
        monkeypatch.setattr(_ss, "permission_service", perm)

        with pytest.raises(AuthorizationError, match="Only admins"):
            SettingsService.require_admin_error("viewer@x", "", MagicMock(), MagicMock())


# ---------------------------------------------------------------------------
# _resolve_context / is_registry_locked
# ---------------------------------------------------------------------------


class TestResolveContext:
    def test_returns_domain_host_token_and_registry_dict(self, monkeypatch):
        domain = MagicMock(name="domain")
        settings = MagicMock()
        monkeypatch.setattr(_ss, "get_domain", lambda sm: domain)
        monkeypatch.setattr(
            _ss, "get_databricks_host_and_token", lambda d, s: ("https://h", "tok")
        )
        rcfg_cls = MagicMock()
        rcfg_cls.from_domain.return_value.as_dict.return_value = {"catalog": "c"}
        monkeypatch.setattr(_ss, "RegistryCfg", rcfg_cls)

        result = SettingsService._resolve_context(MagicMock(), settings)

        assert result == (domain, "https://h", "tok", {"catalog": "c"})
        rcfg_cls.from_domain.assert_called_once_with(domain, settings)


class TestIsRegistryLocked:
    def test_unlocked_outside_databricks_app(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: False)
        monkeypatch.setenv("PGHOST", "pg.example")
        assert SettingsService.is_registry_locked(MagicMock()) is False

    def test_locked_in_app_when_pghost_injected(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        monkeypatch.setenv("PGHOST", "pg.example")
        assert SettingsService.is_registry_locked(MagicMock()) is True

    def test_unlocked_in_app_without_pghost(self, monkeypatch):
        monkeypatch.setattr(_ss, "is_databricks_app", lambda: True)
        monkeypatch.delenv("PGHOST", raising=False)
        assert SettingsService.is_registry_locked(MagicMock()) is False


# ---------------------------------------------------------------------------
# Delegators
# ---------------------------------------------------------------------------

_IMPORT_RE = re.compile(r"from (back\.objects\.domain\.[\w.]+) import (\w+)")


def _discover_delegators():
    """(facade_name, module_path, class_name) for every one-line delegator."""
    found = []
    for name, raw in vars(SettingsService).items():
        if not isinstance(raw, staticmethod):
            continue
        func = raw.__func__
        src = inspect.getsource(func)
        match = _IMPORT_RE.search(src)
        if not match or match.group(1).endswith(".SettingsService"):
            continue
        found.append((name, match.group(1), match.group(2)))
    return sorted(found)


_DELEGATORS = _discover_delegators()


def test_delegator_discovery_finds_the_extracted_api_surface():
    targets = {cls for _, _, cls in _DELEGATORS}
    assert {
        "RegistrySettings",
        "RegistryDomainSettings",
        "ScheduleSettings",
        "PermissionSettings",
        "WorkspaceUiSettings",
        "WarehouseSettings",
        "ObxSettings",
    } <= targets
    assert len(_DELEGATORS) > 80


@pytest.mark.parametrize(
    "facade_name,module_path,class_name",
    _DELEGATORS,
    ids=[d[0] for d in _DELEGATORS],
)
async def test_delegator_forwards_every_argument_and_returns_result(
    monkeypatch, facade_name, module_path, class_name
):
    facade = getattr(SettingsService, facade_name)
    target_cls = getattr(importlib.import_module(module_path), class_name)
    assert hasattr(target_cls, facade_name), f"{class_name} lacks {facade_name}"

    is_async = inspect.iscoroutinefunction(facade)
    sentinel_result = object()
    mock = (AsyncMock if is_async else MagicMock)(return_value=sentinel_result)
    monkeypatch.setattr(target_cls, facade_name, mock)

    args, kwargs, sentinels = [], {}, []
    for param in inspect.signature(facade).parameters.values():
        value = object()
        sentinels.append(value)
        if param.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[param.name] = value
        else:
            args.append(value)

    result = facade(*args, **kwargs)
    if is_async:
        result = await result

    assert result is sentinel_result
    mock.assert_called_once()
    forwarded = list(mock.call_args.args) + list(mock.call_args.kwargs.values())
    assert {id(v) for v in forwarded} == {id(v) for v in sentinels}


# ---------------------------------------------------------------------------
# Module alias contract
# ---------------------------------------------------------------------------

_SETTINGS_DIR = Path(_ss.__file__).parent
_ALIAS_IMPORT = 'importlib.import_module("back.objects.domain.settings.SettingsService")'


def _alias_lookups() -> list[tuple[str, str]]:
    lookups = []
    for path in sorted(_SETTINGS_DIR.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        if _ALIAS_IMPORT not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "_ss"
            ):
                lookups.append((path.name, node.attr))
    return sorted(set(lookups))


def test_extracted_settings_classes_only_read_names_the_module_provides():
    """Extracted classes resolve collaborators via ``_ss.<name>`` at call time,
    so a name dropped from SettingsService only fails when that path runs."""
    lookups = _alias_lookups()
    assert lookups, "no _ss lookups found; alias pattern changed?"
    missing = [f"{file}: _ss.{name}" for file, name in lookups if not hasattr(_ss, name)]
    assert not missing, "SettingsService module is missing:\n" + "\n".join(missing)
