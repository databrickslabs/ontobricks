"""Tests for the internal /domain/edit-lock router request plumbing.

The handlers delegate to ``EditLockService`` (covered in
``tests/units/registry/test_edit_lock_service.py``). These tests assert the
request wiring: body parsing, the admin-only ``force`` flag forwarding, and
that ``load-from-uc`` attaches a ``lock`` block on a successful load.
"""

import asyncio
import importlib
from types import SimpleNamespace

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_domain = importlib.import_module("api.routers.internal.domain")
_settings = importlib.import_module("api.routers.internal.settings")
_svc = importlib.import_module(
    "back.objects.registry.lockmgt.EditLockService"
)

pytestmark = pytest.mark.unit


def _run(coro):
    """Run a coroutine synchronously, robust to a running event loop.

    Mirrors ``tests/units/auth/test_permission_middleware._run`` — driving
    the async handlers from a sync test avoids the pytest-asyncio
    ``Runner.run() cannot be called from a running event loop`` clash that
    the bare ``async def`` endpoint tests in this suite hit under the full
    (event-loop-polluted) run.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def _request(body=None, *, user_role="editor", user_email="alice@acme.com"):
    req = MagicMock()
    req.json = AsyncMock(return_value=body if body is not None else {})
    req.state = SimpleNamespace(user_role=user_role, user_email=user_email)
    req.headers = {}
    return req


def test_get_edit_lock_delegates_to_service():
    with patch.object(
        _svc.EditLockService, "status",
        return_value={"success": True, "mode": "edit"},
    ) as st:
        res = _run(_domain.get_edit_lock(
            _request(), session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["mode"] == "edit"
    assert st.called


def test_acquire_forwards_force_flag():
    with patch.object(
        _svc.EditLockService, "acquire",
        return_value={"success": True, "mode": "edit"},
    ) as ac:
        _run(_domain.acquire_edit_lock(
            _request({"force": True}, user_role="admin"),
            session_mgr=MagicMock(), settings=MagicMock(),
        ))
    assert ac.call_args.kwargs["force"] is True


def test_acquire_defaults_force_false():
    with patch.object(
        _svc.EditLockService, "acquire",
        return_value={"success": True, "mode": "edit"},
    ) as ac:
        _run(_domain.acquire_edit_lock(
            _request({}), session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert ac.call_args.kwargs["force"] is False


def test_release_delegates_to_service():
    with patch.object(
        _svc.EditLockService, "release",
        return_value={"success": True, "released": True},
    ) as rl:
        res = _run(_domain.release_edit_lock(
            _request(), session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["released"] is True
    assert rl.called


def test_renew_delegates_to_service():
    with patch.object(
        _svc.EditLockService, "renew",
        return_value={"success": True, "renewed": True},
    ) as rn:
        res = _run(_domain.renew_edit_lock(
            _request(), session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["renewed"] is True
    assert rn.called


def test_close_releases_lock_then_resets_session():
    domain_obj = MagicMock()
    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(
            _svc.EditLockService, "release_for_session"
        ) as rel,
    ):
        res = _run(_domain.close_domain(
            _request(), session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["success"] is True
    assert rel.called
    domain_obj.reset.assert_called_once()
    domain_obj.clear_uc_metadata.assert_called_once()


def test_load_from_uc_attaches_lock_block_on_success():
    domain_obj = MagicMock()
    domain_obj.domain_folder = ""
    domain_obj.current_version = ""

    fake_p = MagicMock()
    fake_p.load_domain_from_uc.return_value = {"success": True, "version": "1"}

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(
            _svc.EditLockService, "on_domain_loaded",
            return_value={"mode": "edit", "is_self": True,
                          "holder_email": "alice@acme.com", "holder_name": "Alice"},
        ) as odl,
    ):
        req = _request({"domain": "acme", "version": "1"})
        res = _run(_domain.load_domain_from_uc(
            req, session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["success"] is True
    assert res["lock"]["mode"] == "edit"
    assert odl.called


def test_load_from_uc_releases_prev_lock_before_opening_different_domain():
    # Session currently has "acme" V2 open; the user opens a *different*
    # domain, so its lock must be freed BEFORE the new domain is loaded.
    domain_obj = MagicMock()
    domain_obj.domain_folder = "acme"
    domain_obj.current_version = "2"

    order = []
    fake_p = MagicMock()
    fake_p.load_domain_from_uc.side_effect = lambda *a, **k: (
        order.append("load") or {"success": True, "version": "1"}
    )

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(
            _svc.EditLockService, "release_prev",
            side_effect=lambda *a, **k: order.append("release_prev") or True,
        ) as rp,
        patch.object(
            _svc.EditLockService, "on_domain_loaded",
            return_value={"mode": "edit", "is_self": True},
        ) as odl,
    ):
        req = _request({"domain": "beta"})
        res = _run(_domain.load_domain_from_uc(
            req, session_mgr=MagicMock(), settings=MagicMock()
        ))

    assert res["success"] is True
    # Close-before-open: prev lock released, and released BEFORE the load.
    assert order == ["release_prev", "load"]
    assert rp.call_args.args[-2:] == ("acme", "2")
    # on_domain_loaded must not re-release the (already-freed) previous pair.
    assert odl.call_args.kwargs.get("prev_folder") == ""
    assert odl.call_args.kwargs.get("prev_version") == ""


def test_load_from_uc_same_domain_version_switch_defers_release():
    # Same domain, different version: no pre-load release — on_domain_loaded
    # gets the previous pair and releases the old version after the load
    # (so reopening the *same* version never churns the lock).
    domain_obj = MagicMock()
    domain_obj.domain_folder = "acme"
    domain_obj.current_version = "1"

    fake_p = MagicMock()
    fake_p.load_domain_from_uc.return_value = {"success": True, "version": "2"}

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(_svc.EditLockService, "release_prev") as rp,
        patch.object(
            _svc.EditLockService, "on_domain_loaded",
            return_value={"mode": "edit"},
        ) as odl,
    ):
        req = _request({"domain": "acme", "version": "2"})
        res = _run(_domain.load_domain_from_uc(
            req, session_mgr=MagicMock(), settings=MagicMock()
        ))

    assert res["success"] is True
    rp.assert_not_called()
    assert odl.call_args.kwargs.get("prev_folder") == "acme"
    assert odl.call_args.kwargs.get("prev_version") == "1"


def test_settings_locks_list_delegates_to_service():
    with patch.object(
        _svc.EditLockService, "list_all",
        return_value={"success": True, "locks": [{"folder": "acme", "version": "1"}]},
    ) as la:
        res = _run(_settings.list_edit_locks(
            session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["success"] is True
    assert res["locks"][0]["folder"] == "acme"
    assert la.called


def test_settings_locks_release_forwards_folder_version():
    with patch.object(
        _svc.EditLockService, "admin_release",
        return_value={"success": True, "released": True},
    ) as ar:
        res = _run(_settings.release_edit_lock_admin(
            _request({"folder": "acme", "version": "2"}),
            session_mgr=MagicMock(), settings=MagicMock(),
        ))
    assert res["released"] is True
    assert ar.call_args.args[-2:] == ("acme", "2")


def test_settings_locks_release_rejects_missing_fields():
    from back.core.errors import ValidationError

    with pytest.raises(ValidationError):
        _run(_settings.release_edit_lock_admin(
            _request({"folder": "", "version": ""}),
            session_mgr=MagicMock(), settings=MagicMock(),
        ))


def test_load_from_uc_no_lock_block_on_failure():
    # No domain currently open (empty prev), so a failed load is not a switch
    # and nothing needs reacquiring.
    domain_obj = MagicMock()
    domain_obj.domain_folder = ""
    domain_obj.current_version = ""

    fake_p = MagicMock()
    fake_p.load_domain_from_uc.return_value = {"success": False, "error": "boom"}

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(_svc.EditLockService, "release_prev", return_value=True),
        patch.object(_svc.EditLockService, "reacquire") as rq,
        patch.object(_svc.EditLockService, "on_domain_loaded") as odl,
    ):
        req = _request({"domain": "acme", "version": "1"})
        res = _run(_domain.load_domain_from_uc(
            req, session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["success"] is False
    assert "lock" not in res
    odl.assert_not_called()
    # No switch happened here (no prev domain), so nothing to reacquire.
    rq.assert_not_called()


def test_load_from_uc_reacquires_prev_lock_when_switch_load_fails():
    # Cross-domain switch whose load returns a failure envelope: the prev lock
    # we freed pre-load must be restored so the switch does not orphan it.
    domain_obj = MagicMock()
    domain_obj.domain_folder = "acme"
    domain_obj.current_version = "2"

    fake_p = MagicMock()
    fake_p.load_domain_from_uc.return_value = {"success": False, "error": "boom"}

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(_svc.EditLockService, "release_prev", return_value=True),
        patch.object(_svc.EditLockService, "reacquire") as rq,
        patch.object(_svc.EditLockService, "on_domain_loaded") as odl,
    ):
        req = _request({"domain": "beta"})
        res = _run(_domain.load_domain_from_uc(
            req, session_mgr=MagicMock(), settings=MagicMock()
        ))
    assert res["success"] is False
    odl.assert_not_called()
    assert rq.call_args.args[-2:] == ("acme", "2")


def test_load_from_uc_reacquires_prev_lock_when_switch_load_raises():
    # Same as above but the load raises: the prev lock is restored, then the
    # error propagates to the global handler.
    domain_obj = MagicMock()
    domain_obj.domain_folder = "acme"
    domain_obj.current_version = "2"

    fake_p = MagicMock()
    fake_p.load_domain_from_uc.side_effect = RuntimeError("kaboom")

    with (
        patch.object(_domain, "get_domain", return_value=domain_obj),
        patch.object(_domain, "Domain", return_value=fake_p),
        patch.object(_svc.EditLockService, "release_prev", return_value=True),
        patch.object(_svc.EditLockService, "reacquire") as rq,
        patch.object(_svc.EditLockService, "on_domain_loaded") as odl,
    ):
        req = _request({"domain": "beta"})
        with pytest.raises(RuntimeError):
            _run(_domain.load_domain_from_uc(
                req, session_mgr=MagicMock(), settings=MagicMock()
            ))
    odl.assert_not_called()
    assert rq.call_args.args[-2:] == ("acme", "2")