"""Task routes are owner-scoped; mutating routes carry explicit role guards."""

import asyncio
from types import SimpleNamespace

import pytest

from api.routers.internal import dtwin, home, tasks
from back.core.databricks.request_identity import (
    RequestIdentity,
    reset_request_identity,
    set_request_identity,
)
from back.core.errors import AuthorizationError, NotFoundError
from back.core.task_manager import get_task_manager
from back.core.task_manager.Task import Task

pytestmark = pytest.mark.unit


def _request(email, role):
    return SimpleNamespace(state=SimpleNamespace(user_email=email, user_role=role))


@pytest.fixture
def owned_tasks():
    tm = get_task_manager()
    created = []
    for email in ("alice@x", "bob@x", ""):
        token = set_request_identity(RequestIdentity(email=email))
        try:
            created.append(tm.create_task(f"task-{email or 'system'}", "test"))
        finally:
            reset_request_identity(token)
    yield created
    for task in created:
        tm._tasks.pop(task.id, None)


def test_create_task_records_the_request_owner(owned_tasks):
    assert [t.owner for t in owned_tasks] == ["alice@x", "bob@x", ""]


def test_list_shows_own_and_system_tasks_only(owned_tasks):
    ids = {t.id for t in owned_tasks}
    out = asyncio.run(tasks.list_tasks(_request("alice@x", "app_user")))
    names = {t["name"] for t in out["tasks"] if t["id"] in ids}
    assert names == {"task-alice@x", "task-system"}


def test_admin_lists_every_task(owned_tasks):
    ids = {t.id for t in owned_tasks}
    out = asyncio.run(tasks.list_tasks(_request("root@x", "admin")))
    assert ids <= {t["id"] for t in out["tasks"]}


def test_other_users_task_is_not_found(owned_tasks):
    bob_task = owned_tasks[1]
    with pytest.raises(NotFoundError):
        asyncio.run(tasks.get_task(bob_task.id, _request("alice@x", "app_user")))
    with pytest.raises(NotFoundError):
        asyncio.run(tasks.cancel_task(bob_task.id, _request("alice@x", "app_user")))


def test_system_task_cancel_is_admin_only(owned_tasks):
    system_task = owned_tasks[2]
    with pytest.raises(AuthorizationError):
        asyncio.run(tasks.cancel_task(system_task.id, _request("alice@x", "app_user")))


def test_owner_can_cancel_own_task(owned_tasks):
    alice_task = owned_tasks[0]
    out = asyncio.run(tasks.cancel_task(alice_task.id, _request("alice@x", "app_user")))
    assert out["success"] is True


@pytest.mark.parametrize(
    "owner,email,is_admin,visible,cancellable",
    [
        ("a", "a", False, True, True),
        ("a", "b", False, False, False),
        ("", "b", False, True, False),
        ("a", "b", True, True, True),
    ],
)
def test_task_visibility_rules(owner, email, is_admin, visible, cancellable):
    task = Task(id="t", name="n", task_type="x", owner=owner)
    assert task.visible_to(email, is_admin) is visible
    assert task.cancellable_by(email, is_admin) is cancellable


def _guards(router, path):
    route = next(r for r in router.routes if getattr(r, "path", "") == path)
    return [d.call.__name__ for d in route.dependant.dependencies]


@pytest.mark.parametrize(
    "router,path,guard",
    [
        (tasks.router, "/tasks/clear-completed", "require_app_admin"),
        (dtwin.router, "/dtwin/reasoning/materialize", "require_domain_builder"),
        (home.router, "/clear-ontology", "require_domain_editor"),
    ],
)
def test_mutating_routes_have_role_guards(router, path, guard):
    assert guard in _guards(router, path)


def test_unused_volume_write_route_is_removed():
    assert not any(getattr(r, "path", "") == "/write-volume-file" for r in home.router.routes)


def test_clear_ontology_is_lifecycle_status_gated():
    from shared.fastapi import main

    assert "/clear-ontology" in main._STATUS_GATE_EDIT_PATHS
