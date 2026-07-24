"""Tests for the single-editor edit-lock orchestrator (EditLockService).

The Lakebase store and the session are mocked; the focus is the service
logic: DRAFT gating, edit/view mode shaping, admin-only ``force`` take-over
gating, and the ``blocking_holder`` middleware helper.
"""

import importlib

from unittest.mock import MagicMock, patch

from back.objects.registry.lockmgt import EditLockService

_mod = importlib.import_module(
    "back.objects.registry.lockmgt.EditLockService"
)


def _request(email="alice@acme.com", role="editor", session_id="sess1"):
    req = MagicMock()
    req.state.user_email = email
    req.state.user_role = role
    req.state.session_id = session_id
    req.headers = {"x-forwarded-preferred-username": "Alice"}
    return req


def _domain(folder="acme", version="1", status="DRAFT"):
    d = MagicMock()
    d.domain_folder = folder
    d.current_version = version
    d.info = {"status": status}
    return d


def _patches(store, domain):
    return (
        patch.object(_mod, "get_domain", MagicMock(return_value=domain)),
        patch.object(_mod.EditLockService, "_store", MagicMock(return_value=store)),
    )


def _store(acquire=None, get=None, renew=True):
    st = MagicMock()
    st.acquire_edit_lock.return_value = acquire or {
        "acquired": True,
        "is_self": True,
        "holder_email": "alice@acme.com",
        "holder_name": "Alice",
        "acquired_at": "2026-01-01T00:00:00",
    }
    st.get_edit_lock.return_value = get
    st.release_edit_lock.return_value = True
    st.renew_edit_lock.return_value = renew
    return st


# ----------------------------------------------------------------------
# status
# ----------------------------------------------------------------------


def test_status_editor_gets_edit_mode():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.status(_request(), MagicMock(), MagicMock())
    assert res["mode"] == "edit"
    assert res["is_self"] is True
    store.acquire_edit_lock.assert_called_once()
    # Non-forcing acquire on a plain status check.
    assert store.acquire_edit_lock.call_args.kwargs["force"] is False


def test_status_viewer_gets_view_mode_no_takeover_for_non_admin():
    store = _store(
        acquire={
            "acquired": False,
            "is_self": False,
            "holder_email": "bob@acme.com",
            "holder_name": "Bob",
            "acquired_at": "2026-01-01T00:00:00",
        }
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.status(
            _request(email="carol@acme.com", role="editor"), MagicMock(), MagicMock()
        )
    assert res["mode"] == "view"
    assert res["holder_name"] == "Bob"
    assert res["can_take_over"] is False


def test_status_viewer_admin_can_take_over():
    store = _store(
        acquire={
            "acquired": False,
            "is_self": False,
            "holder_email": "bob@acme.com",
            "holder_name": "Bob",
        }
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.status(
            _request(email="admin@acme.com", role="admin"), MagicMock(), MagicMock()
        )
    assert res["mode"] == "view"
    assert res["can_take_over"] is True


def test_status_unavailable_backend_degrades_to_none_not_phantom_view():
    """A failed acquire with no holder (e.g. the domain_edit_locks table is
    missing) must degrade to permissive ``none`` — never a phantom ``view``
    that falsely claims "another user" is editing and breaks take-over."""
    store = _store(
        acquire={"acquired": False, "is_self": False, "holder_email": ""}
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.status(
            _request(email="admin@acme.com", role="admin"), MagicMock(), MagicMock()
        )
    assert res["mode"] == "none"
    assert res["holder_email"] == ""
    assert res["can_take_over"] is False


def test_status_non_draft_is_none_and_skips_store():
    store = _store()
    p1, p2 = _patches(store, _domain(status="PUBLISHED"))
    with p1, p2:
        res = EditLockService.status(_request(), MagicMock(), MagicMock())
    assert res["mode"] == "none"
    store.acquire_edit_lock.assert_not_called()


# ----------------------------------------------------------------------
# acquire — force gating
# ----------------------------------------------------------------------


def test_acquire_force_ignored_for_non_admin():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        EditLockService.acquire(
            _request(role="editor"), MagicMock(), MagicMock(), force=True
        )
    assert store.acquire_edit_lock.call_args.kwargs["force"] is False


def test_acquire_force_honoured_for_admin():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        EditLockService.acquire(
            _request(role="admin"), MagicMock(), MagicMock(), force=True
        )
    assert store.acquire_edit_lock.call_args.kwargs["force"] is True


# ----------------------------------------------------------------------
# release
# ----------------------------------------------------------------------


def test_release_calls_store_with_email():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.release(_request(), MagicMock(), MagicMock())
    assert res["released"] is True
    assert store.release_edit_lock.call_args.kwargs["holder_email"] == "alice@acme.com"


# ----------------------------------------------------------------------
# renew (lease keep-alive)
# ----------------------------------------------------------------------


def test_renew_reports_renewed_for_holder():
    store = _store(renew=True)
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.renew(_request(), MagicMock(), MagicMock())
    assert res == {"success": True, "renewed": True}
    assert (
        store.renew_edit_lock.call_args.kwargs["holder_email"]
        == "alice@acme.com"
    )


def test_renew_reports_lost_when_no_longer_holder():
    store = _store(renew=False)
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        res = EditLockService.renew(_request(), MagicMock(), MagicMock())
    assert res == {"success": True, "renewed": False}


def test_renew_noop_on_non_draft_skips_store():
    store = _store()
    p1, p2 = _patches(store, _domain(status="PUBLISHED"))
    with p1, p2:
        res = EditLockService.renew(_request(), MagicMock(), MagicMock())
    assert res == {"success": True, "renewed": False}
    store.renew_edit_lock.assert_not_called()


def test_status_exposes_lease_ttl():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2, patch.dict(
        _mod.os.environ, {"ONTOBRICKS_EDIT_LOCK_TTL_S": "300"}
    ):
        res = EditLockService.status(_request(), MagicMock(), MagicMock())
    assert res["lease_ttl_s"] == 300
    assert store.acquire_edit_lock.call_args.kwargs["ttl_seconds"] == 300


# ----------------------------------------------------------------------
# _ttl_seconds — global-config > env > default resolution
# ----------------------------------------------------------------------


def _patch_global_ttl(configured):
    """Patch the global-config lookup used by ``_ttl_seconds``.

    ``configured`` is the value ``get_edit_lock_ttl_s`` returns (``None``
    meaning "not set in Settings › Global").
    """
    gcs = MagicMock()
    gcs.get_edit_lock_ttl_s.return_value = configured
    reg_cfg = MagicMock()
    reg_cfg.as_dict.return_value = {"catalog": "c", "schema": "s"}
    return (
        patch("back.objects.session.global_config_service", gcs),
        patch.object(_mod.RegistryCfg, "from_session", MagicMock(return_value=reg_cfg)),
    )


def test_ttl_seconds_global_config_wins_over_env():
    g1, g2 = _patch_global_ttl(1800)
    with g1, g2, patch.dict(
        _mod.os.environ, {"ONTOBRICKS_EDIT_LOCK_TTL_S": "300"}
    ):
        assert EditLockService._ttl_seconds(MagicMock(), MagicMock()) == 1800


def test_ttl_seconds_falls_back_to_env_when_global_unset():
    g1, g2 = _patch_global_ttl(None)
    with g1, g2, patch.dict(
        _mod.os.environ, {"ONTOBRICKS_EDIT_LOCK_TTL_S": "300"}
    ):
        assert EditLockService._ttl_seconds(MagicMock(), MagicMock()) == 300


def test_ttl_seconds_zero_from_global_disables_lease():
    g1, g2 = _patch_global_ttl(0)
    with g1, g2, patch.dict(
        _mod.os.environ, {"ONTOBRICKS_EDIT_LOCK_TTL_S": "600"}
    ):
        assert EditLockService._ttl_seconds(MagicMock(), MagicMock()) == 0


def test_ttl_seconds_default_when_no_session_context():
    with patch.dict(_mod.os.environ, {}, clear=False) as _env:
        _mod.os.environ.pop("ONTOBRICKS_EDIT_LOCK_TTL_S", None)
        assert EditLockService._ttl_seconds() == _mod._DEFAULT_EDIT_LOCK_TTL_S


# ----------------------------------------------------------------------
# release_prev / release_prev_on_switch — close-before-open plumbing
# ----------------------------------------------------------------------


def test_release_prev_releases_holder_scoped_pair():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        ok = EditLockService.release_prev(
            _request(), MagicMock(), MagicMock(), "acme", "1"
        )
    assert ok is True
    store.release_edit_lock.assert_called_once_with(
        "acme", "1", holder_email="alice@acme.com"
    )


def test_release_prev_noops_without_folder_or_version():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        assert (
            EditLockService.release_prev(
                _request(), MagicMock(), MagicMock(), "", "1"
            )
            is False
        )
    store.release_edit_lock.assert_not_called()


def test_release_prev_on_switch_releases_before_switching_domain():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        switching = EditLockService.release_prev_on_switch(
            _request(),
            MagicMock(),
            MagicMock(),
            prev_folder="acme",
            prev_version="2",
            new_domain="beta",
        )
    assert switching is True
    store.release_edit_lock.assert_called_once_with(
        "acme", "2", holder_email="alice@acme.com"
    )


def test_release_prev_on_switch_same_domain_defers_release():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        switching = EditLockService.release_prev_on_switch(
            _request(),
            MagicMock(),
            MagicMock(),
            prev_folder="Acme",  # case-insensitive match with new_domain
            prev_version="1",
            new_domain="acme",
        )
    assert switching is False
    store.release_edit_lock.assert_not_called()


def test_reacquire_restores_previous_lock():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        ok = EditLockService.reacquire(
            _request(), MagicMock(), MagicMock(), "acme", "2"
        )
    assert ok is True
    assert store.acquire_edit_lock.call_args.kwargs["force"] is False
    assert (
        store.acquire_edit_lock.call_args.kwargs["holder_email"]
        == "alice@acme.com"
    )


def test_reacquire_noops_without_folder_or_version():
    store = _store()
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        assert (
            EditLockService.reacquire(
                _request(), MagicMock(), MagicMock(), "acme", ""
            )
            is False
        )
    store.acquire_edit_lock.assert_not_called()


def test_on_domain_loaded_defers_prev_release_for_version_switch():
    """Same-domain version switch: on_domain_loaded releases the old version's
    lock (via release_prev) and then acquires the freshly loaded one."""
    store = _store()
    p1, p2 = _patches(store, _domain(folder="acme", version="2"))
    with p1, p2:
        res = EditLockService.on_domain_loaded(
            _request(),
            MagicMock(),
            MagicMock(),
            prev_folder="acme",
            prev_version="1",
        )
    store.release_edit_lock.assert_called_once_with(
        "acme", "1", holder_email="alice@acme.com"
    )
    store.acquire_edit_lock.assert_called_once()
    assert res["mode"] == "edit"


# ----------------------------------------------------------------------
# admin overview — list_all / admin_release
# ----------------------------------------------------------------------


def test_list_all_returns_store_locks():
    store = MagicMock()
    store.list_all_edit_locks.return_value = [
        {
            "folder": "acme",
            "version": "1",
            "status": "DRAFT",
            "holder_email": "bob@acme.com",
            "holder_name": "Bob",
            "acquired_at": "2026-01-01T00:00:00",
        }
    ]
    with patch.object(_mod.EditLockService, "_store", MagicMock(return_value=store)):
        res = EditLockService.list_all(MagicMock(), MagicMock())
    assert res["success"] is True
    assert res["locks"][0]["folder"] == "acme"
    assert res["locks"][0]["holder_name"] == "Bob"


def test_list_all_degrades_to_empty_when_backend_unavailable():
    with patch.object(_mod.EditLockService, "_store", MagicMock(return_value=None)):
        res = EditLockService.list_all(MagicMock(), MagicMock())
    assert res == {"success": True, "locks": []}


def test_admin_release_force_releases_and_reports_result():
    store = MagicMock()
    store.force_release_edit_lock.return_value = True
    with patch.object(_mod.EditLockService, "_store", MagicMock(return_value=store)):
        res = EditLockService.admin_release(MagicMock(), MagicMock(), "acme", "1")
    assert res == {"success": True, "released": True}
    store.force_release_edit_lock.assert_called_once_with("acme", "1")


def test_admin_release_requires_folder_and_version():
    store = MagicMock()
    with patch.object(_mod.EditLockService, "_store", MagicMock(return_value=store)):
        res = EditLockService.admin_release(MagicMock(), MagicMock(), "", "1")
    assert res["success"] is False
    store.force_release_edit_lock.assert_not_called()


# ----------------------------------------------------------------------
# blocking_holder (middleware helper)
# ----------------------------------------------------------------------


def test_blocking_holder_returns_name_when_held_by_other():
    store = _store(
        get={"holder_email": "bob@acme.com", "holder_name": "Bob"}
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        holder = EditLockService.blocking_holder(
            _request(email="carol@acme.com"), MagicMock(), MagicMock()
        )
    assert holder == "Bob"


def test_blocking_holder_empty_when_self():
    store = _store(
        get={"holder_email": "alice@acme.com", "holder_name": "Alice"}
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        holder = EditLockService.blocking_holder(
            _request(email="alice@acme.com"), MagicMock(), MagicMock()
        )
    assert holder == ""


def test_blocking_holder_empty_on_non_draft():
    store = _store(
        get={"holder_email": "bob@acme.com", "holder_name": "Bob"}
    )
    p1, p2 = _patches(store, _domain(status="IN-REVIEW"))
    with p1, p2:
        holder = EditLockService.blocking_holder(
            _request(email="carol@acme.com"), MagicMock(), MagicMock()
        )
    assert holder == ""
    store.get_edit_lock.assert_not_called()


def test_blocking_holder_empty_when_lease_stale():
    """A stale lease (heartbeat lapsed past the TTL) must not block a new
    editor — it is reclaimable, so the gate treats it as free."""
    store = _store(
        get={
            "holder_email": "bob@acme.com",
            "holder_name": "Bob",
            "is_stale": True,
        }
    )
    p1, p2 = _patches(store, _domain())
    with p1, p2:
        holder = EditLockService.blocking_holder(
            _request(email="carol@acme.com"), MagicMock(), MagicMock()
        )
    assert holder == ""
