# Domain Versions Cards and Safe Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace Domain → Versions’ table with lifecycle-aware cards and add one server-authoritative policy that permits app admins to delete only older, unloaded Draft versions.

**Architecture:** Extend the existing `version_lifecycle` module with pure transition/deletion capability functions. Route both Domain and Registry delete requests through one guarded `SettingsService` orchestration before delegating physical cleanup to `RegistryService.delete_version`. Return server-computed capabilities to both frontends so JavaScript renders actions without reimplementing policy.

**Tech Stack:** FastAPI, Python 3.12, Jinja2, vanilla JavaScript, Bootstrap 5.3, pytest, Playwright.

## Global Constraints

- App version is `0.9.0`; changelog entries go under `changelogs/v0.9.0/`.
- Use `uv run --frozen` for every Python or pytest command.
- Comments, UI strings, documentation, tests, and changelog content are English-only.
- Lifecycle states and transitions remain exactly `DRAFT → IN-REVIEW → PUBLISHED`, `IN-REVIEW → DRAFT`, and admin-only `PUBLISHED → DRAFT`.
- Deletion is app-admin-only and allowed only for an older `DRAFT` version that is neither loaded nor latest.
- Policy is authoritative on the server; frontend visibility is never authorization.
- Domain and Registry delete paths must call the same guarded service.
- Desktop owns an internal card-list scroller; at `≤768px`, restore natural document flow.
- Do not add inline CSS or inline JavaScript handlers.
- Do not change modal sizing or the Publication/Validation workflow.

---

## File Map

**Create**

- `src/front/static/domain/css/domain-versions.css` — Domain Versions card layout, states, and responsive behavior.
- `tests/units/settings/test_settings_version_deletion.py` — guarded deletion orchestration matrix.
- `tests/units/api/test_delete_version_endpoints.py` — Domain and Settings route plumbing.
- `tests/units/domain/test_version_capabilities.py` — Domain and Registry capability payloads.
- `tests/units/front/test_domain_versions_cards.py` — static card/JS/CSS contracts.
- `tests/e2e/domain/test_domain_versions_cards.py` — rendered desktop/mobile and interaction contracts using mocked network responses.

**Modify**

- `src/back/objects/registry/version_lifecycle.py` — pure transition and deletion capability policy.
- `tests/units/registry/test_version_lifecycle.py` — policy matrix.
- `src/back/objects/domain/SettingsService.py` — guarded deletion and Registry capability decoration.
- `src/api/routers/internal/domain.py` — role-aware version listing and loaded-domain DELETE route.
- `src/api/routers/internal/settings.py` — role-aware Registry listing and guarded DELETE route.
- `src/back/objects/domain/Domain.py` — enriched card capability payload.
- `src/front/templates/domain.html` — wire the new stylesheet.
- `src/front/templates/partials/domain/_domain_versions.html` — card-list shell and semantic states.
- `src/front/static/domain/js/domain-versions.js` — render cards and execute server-described actions.
- `src/front/static/registry/js/registry.js` — consume guarded delete capability.
- `tests/units/front/test_create_version_ungated.py` — retain New Version behavior after markup changes.
- `docs/user-guide.md` — cards, inline lifecycle, and deletion policy.
- `docs/features.md` — update Version Control behavior.
- `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log` — implementation record and final test result.

---

### Task 1: Pure lifecycle and deletion capabilities

**Files:**
- Modify: `src/back/objects/registry/version_lifecycle.py`
- Modify: `tests/units/registry/test_version_lifecycle.py`

**Interfaces:**
- Produces: `transition_capabilities(current, *, user_role, user_domain_role, last_build, has_ontology) -> list[dict[str, object]]`
- Produces: `check_version_deletion(*, user_role, status, is_loaded, is_latest, version_count) -> None`
- Produces: `version_deletion_capability(...) -> dict[str, object]`
- Consumed by: Tasks 2 and 3.

- [ ] **Step 1: Write failing deletion-policy tests**

Append to `tests/units/registry/test_version_lifecycle.py`:

```python
from back.core.errors import AuthorizationError, ConflictError
from back.objects.registry.version_lifecycle import (
    check_version_deletion,
    transition_capabilities,
    version_deletion_capability,
)


def _check_delete(
    *,
    user_role=ROLE_ADMIN,
    status=STATUS_DRAFT,
    is_loaded=False,
    is_latest=False,
    version_count=3,
):
    return check_version_deletion(
        user_role=user_role,
        status=status,
        is_loaded=is_loaded,
        is_latest=is_latest,
        version_count=version_count,
    )


def test_old_unloaded_draft_is_deletable_by_app_admin():
    _check_delete()


@pytest.mark.parametrize("role", [ROLE_BUILDER, ROLE_EDITOR, ROLE_VIEWER, ROLE_NONE])
def test_version_deletion_requires_app_admin(role):
    with pytest.raises(AuthorizationError, match="administrator"):
        _check_delete(user_role=role)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"is_loaded": True}, "loaded"),
        ({"is_latest": True}, "latest"),
        ({"version_count": 1}, "at least one"),
        ({"status": STATUS_IN_REVIEW}, "Draft"),
        ({"status": STATUS_PUBLISHED}, "Draft"),
    ],
)
def test_protected_version_cannot_be_deleted(overrides, message):
    with pytest.raises(ConflictError, match=message):
        _check_delete(**overrides)


def test_delete_capability_returns_precise_block_reason():
    result = version_deletion_capability(
        user_role=ROLE_ADMIN,
        status=STATUS_PUBLISHED,
        is_loaded=False,
        is_latest=False,
        version_count=3,
    )
    assert result == {
        "delete_control_visible": True,
        "can_delete": False,
        "delete_block_reason": "Reopen this version as Draft before deleting it.",
    }


def test_non_admin_delete_control_is_hidden():
    result = version_deletion_capability(
        user_role=ROLE_BUILDER,
        status=STATUS_DRAFT,
        is_loaded=False,
        is_latest=False,
        version_count=3,
    )
    assert result["delete_control_visible"] is False
    assert result["can_delete"] is False
```

- [ ] **Step 2: Write failing transition-capability tests**

Append:

```python
def test_transition_capabilities_include_enabled_builder_action():
    result = transition_capabilities(
        STATUS_DRAFT,
        user_role="",
        user_domain_role=ROLE_BUILDER,
        last_build="2026-09-25T10:00:00Z",
        has_ontology=False,
    )
    assert result == [
        {
            "target_status": STATUS_IN_REVIEW,
            "label": "Submit for Review",
            "enabled": True,
            "blocked_reason": "",
        }
    ]


def test_transition_capabilities_keep_readiness_failure_disabled():
    result = transition_capabilities(
        STATUS_DRAFT,
        user_role=ROLE_ADMIN,
        user_domain_role=ROLE_NONE,
        last_build="",
        has_ontology=False,
    )
    assert result[0]["enabled"] is False
    assert "ontology" in result[0]["blocked_reason"]


def test_transition_capabilities_omit_unauthorized_actions():
    result = transition_capabilities(
        STATUS_PUBLISHED,
        user_role="",
        user_domain_role=ROLE_BUILDER,
        last_build="",
        has_ontology=True,
    )
    assert result == []
```

- [ ] **Step 3: Run tests and verify RED**

Run:

```bash
uv run --frozen pytest -q tests/units/registry/test_version_lifecycle.py
```

Expected: collection fails because the three new policy functions do not exist.

- [ ] **Step 4: Implement the pure policy**

In `version_lifecycle.py`, import `ConflictError`, add labels, then add:

```python
from typing import Any

from back.core.errors import AuthorizationError, ConflictError, ValidationError

TRANSITION_LABELS = {
    STATUS_IN_REVIEW: "Submit for Review",
    STATUS_PUBLISHED: "Publish",
    STATUS_DRAFT: "Return to Draft",
}


def transition_capabilities(
    current: str,
    *,
    user_role: str,
    user_domain_role: str,
    last_build: str,
    has_ontology: bool,
) -> list[dict[str, Any]]:
    current = (current or STATUS_DRAFT).upper()
    capabilities: list[dict[str, Any]] = []
    for (source, target), _tier in ALLOWED_TRANSITIONS.items():
        if source != current:
            continue
        label = (
            "Reopen as Draft"
            if source == STATUS_PUBLISHED and target == STATUS_DRAFT
            else TRANSITION_LABELS[target]
        )
        try:
            check_status_transition(
                source,
                target,
                user_role=user_role,
                user_domain_role=user_domain_role,
                last_build=last_build,
                has_ontology=has_ontology,
            )
        except AuthorizationError:
            continue
        except ValidationError as exc:
            capabilities.append(
                {
                    "target_status": target,
                    "label": label,
                    "enabled": False,
                    "blocked_reason": str(exc),
                }
            )
        else:
            capabilities.append(
                {
                    "target_status": target,
                    "label": label,
                    "enabled": True,
                    "blocked_reason": "",
                }
            )
    return capabilities


def check_version_deletion(
    *,
    user_role: str,
    status: str,
    is_loaded: bool,
    is_latest: bool,
    version_count: int,
) -> None:
    if user_role != ROLE_ADMIN:
        raise AuthorizationError("Only an application administrator can delete versions")
    if version_count <= 1:
        raise ConflictError("A domain must keep at least one version")
    if is_loaded:
        raise ConflictError("Load another version before deleting this one")
    if is_latest:
        raise ConflictError("The latest version cannot be deleted")
    if (status or STATUS_DRAFT).upper() != STATUS_DRAFT:
        raise ConflictError("Reopen this version as Draft before deleting it")


def version_deletion_capability(
    *,
    user_role: str,
    status: str,
    is_loaded: bool,
    is_latest: bool,
    version_count: int,
) -> dict[str, Any]:
    visible = user_role == ROLE_ADMIN
    try:
        check_version_deletion(
            user_role=user_role,
            status=status,
            is_loaded=is_loaded,
            is_latest=is_latest,
            version_count=version_count,
        )
    except (AuthorizationError, ConflictError) as exc:
        reason = str(exc)
        if "keep at least one" in reason:
            reason = "A domain must keep at least one version."
        elif "Load another" in reason:
            reason = "Load another version before deleting this one."
        elif "latest" in reason:
            reason = "The latest version cannot be deleted."
        elif "Draft" in reason:
            reason = "Reopen this version as Draft before deleting it."
        return {
            "delete_control_visible": visible,
            "can_delete": False,
            "delete_block_reason": reason,
        }
    return {
        "delete_control_visible": True,
        "can_delete": True,
        "delete_block_reason": "",
    }
```

- [ ] **Step 5: Run policy tests and verify GREEN**

Run:

```bash
uv run --frozen pytest -q tests/units/registry/test_version_lifecycle.py
```

Expected: all lifecycle and deletion tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/back/objects/registry/version_lifecycle.py tests/units/registry/test_version_lifecycle.py
git commit -m "feat(domain): define guarded version deletion policy"
```

---

### Task 2: Guarded deletion service and HTTP endpoints

**Files:**
- Create: `tests/units/settings/test_settings_version_deletion.py`
- Create: `tests/units/api/test_delete_version_endpoints.py`
- Modify: `src/back/objects/domain/SettingsService.py`
- Modify: `src/api/routers/internal/domain.py`
- Modify: `src/api/routers/internal/settings.py`

**Interfaces:**
- Consumes: `check_version_deletion(...)` from Task 1.
- Produces: `SettingsService.delete_registry_version_result(domain_name, version, *, user_role, session_mgr, settings) -> dict[str, Any]`.
- Produces: `DELETE /domain/versions/{version}`.
- Preserves: `DELETE /settings/registry/domains/{domain_name}/versions/{version}`.

- [ ] **Step 1: Write failing guarded-service tests**

Create `tests/units/settings/test_settings_version_deletion.py`:

```python
import importlib
from unittest.mock import MagicMock, patch

import pytest

from back.core.errors import AuthorizationError, ConflictError, NotFoundError
from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.PermissionService import ROLE_ADMIN, ROLE_BUILDER

module = importlib.import_module("back.objects.domain.SettingsService")


def _run(
    *,
    user_role=ROLE_ADMIN,
    target="1",
    versions=("1", "2", "3"),
    status="DRAFT",
    loaded_folder="acme",
    loaded_version="3",
):
    domain = MagicMock()
    domain.domain_folder = loaded_folder
    domain.current_version = loaded_version
    svc = MagicMock()
    svc.cfg.is_configured = True
    svc.list_versions_sorted.return_value = list(reversed(versions))
    svc.read_version.return_value = (True, {"info": {"status": status}}, "")
    svc.delete_version.return_value = (True, "")
    with (
        patch.object(module, "get_domain", return_value=domain),
        patch.object(module.RegistryService, "from_context", return_value=svc),
        patch.object(module, "clear_version_status_cache") as clear_status,
    ):
        result = SettingsService.delete_registry_version_result(
            "acme",
            target,
            user_role=user_role,
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    return result, svc, clear_status


def test_old_unloaded_draft_is_deleted():
    result, svc, clear_status = _run()
    assert result["success"] is True
    svc.delete_version.assert_called_once_with("acme", "1")
    clear_status.assert_called_once()


def test_non_admin_cannot_delete():
    with pytest.raises(AuthorizationError):
        _run(user_role=ROLE_BUILDER)


def test_loaded_version_cannot_delete():
    with pytest.raises(ConflictError, match="Load another"):
        _run(target="2", loaded_version="2")


def test_latest_version_cannot_delete():
    with pytest.raises(ConflictError, match="latest"):
        _run(target="3")


def test_non_draft_version_cannot_delete():
    with pytest.raises(ConflictError, match="Draft"):
        _run(status="PUBLISHED")


def test_missing_version_is_not_found():
    with pytest.raises(NotFoundError):
        _run(target="99")
```

- [ ] **Step 2: Write failing endpoint tests**

Create `tests/units/api/test_delete_version_endpoints.py`:

```python
import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

domain_router = importlib.import_module("api.routers.internal.domain")
settings_router = importlib.import_module("api.routers.internal.settings")


def _request(role="admin"):
    req = MagicMock()
    req.state = SimpleNamespace(user_role=role)
    return req


async def test_domain_delete_derives_loaded_folder():
    domain = MagicMock()
    domain.uc_domain_folder = "acme"
    with (
        patch.object(domain_router, "get_domain", return_value=domain),
        patch.object(
            domain_router.SettingsService,
            "delete_registry_version_result",
            return_value={"success": True},
        ) as delete,
    ):
        result = await domain_router.delete_domain_version(
            "1",
            _request(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    assert result["success"] is True
    assert delete.call_args.args[:2] == ("acme", "1")
    assert delete.call_args.kwargs["user_role"] == "admin"


async def test_settings_delete_forwards_target_and_role():
    with patch.object(
        settings_router.config_service,
        "delete_registry_version_result",
        return_value={"success": True},
    ) as delete:
        result = await settings_router.delete_registry_version(
            "acme",
            "1",
            _request(),
            session_mgr=MagicMock(),
            settings=MagicMock(),
        )
    assert result["success"] is True
    assert delete.call_args.args[:2] == ("acme", "1")
    assert delete.call_args.kwargs["user_role"] == "admin"
```

- [ ] **Step 3: Run focused tests and verify RED**

```bash
uv run --frozen pytest -q \
  tests/units/settings/test_settings_version_deletion.py \
  tests/units/api/test_delete_version_endpoints.py
```

Expected: service signature, Domain endpoint, and Settings request parameter are missing.

- [ ] **Step 4: Implement guarded orchestration**

In `SettingsService.py`, import `check_version_deletion` and replace
`delete_registry_version_result` with:

```python
@staticmethod
def delete_registry_version_result(
    domain_name: str,
    version: str,
    *,
    user_role: str,
    session_mgr: SessionManager,
    settings: Settings,
) -> Dict[str, Any]:
    try:
        domain = get_domain(session_mgr)
        svc = RegistryService.from_context(domain, settings)
        if not svc.cfg.is_configured:
            raise ValidationError("Registry not configured")

        versions = svc.list_versions_sorted(domain_name)
        if version not in versions:
            raise NotFoundError(
                f'Version {version} not found in "{domain_name}"'
            )
        ok, data, message = svc.read_version(domain_name, version)
        if not ok:
            raise InfrastructureError(
                "Failed to read registry version", detail=message
            )
        status = (data.get("info", {}).get("status") or "DRAFT").upper()
        check_version_deletion(
            user_role=user_role,
            status=status,
            is_loaded=(
                domain.domain_folder == domain_name
                and domain.current_version == version
            ),
            is_latest=version == versions[0],
            version_count=len(versions),
        )

        deleted, delete_message = svc.delete_version(domain_name, version)
        if not deleted:
            raise InfrastructureError(
                "Failed to delete registry version", detail=delete_message
            )
        clear_version_status_cache()
        return {
            "success": True,
            "message": f'Version {version} deleted from "{domain_name}"',
        }
    except OntoBricksError:
        raise
    except Exception as exc:
        logger.exception("Delete registry version failed: %s", exc)
        raise InfrastructureError(
            "Delete registry version failed", detail=str(exc)
        ) from exc
```

- [ ] **Step 5: Add and update routes**

In `domain.py`:

```python
@router.delete("/versions/{version}")
async def delete_domain_version(
    version: str,
    request: Request,
    session_mgr: SessionManager = Depends(get_session_manager),
    settings: Settings = Depends(get_settings),
):
    domain = get_domain(session_mgr)
    folder = domain.uc_domain_folder
    if not folder:
        raise ValidationError("Domain not saved to the registry")
    return SettingsService.delete_registry_version_result(
        folder,
        version,
        user_role=getattr(request.state, "user_role", "") or "",
        session_mgr=session_mgr,
        settings=settings,
    )
```

In `settings.py`, add `request: Request` and call:

```python
return config_service.delete_registry_version_result(
    domain_name,
    version,
    user_role=getattr(request.state, "user_role", "") or "",
    session_mgr=session_mgr,
    settings=settings,
)
```

- [ ] **Step 6: Run focused tests and verify GREEN**

```bash
uv run --frozen pytest -q \
  tests/units/registry/test_version_lifecycle.py \
  tests/units/settings/test_settings_version_deletion.py \
  tests/units/api/test_delete_version_endpoints.py
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  src/back/objects/domain/SettingsService.py \
  src/api/routers/internal/domain.py \
  src/api/routers/internal/settings.py \
  tests/units/settings/test_settings_version_deletion.py \
  tests/units/api/test_delete_version_endpoints.py
git commit -m "feat(domain): guard version deletion endpoints"
```

---

### Task 3: Server-computed version card capabilities

**Files:**
- Create: `tests/units/domain/test_version_capabilities.py`
- Modify: `src/back/objects/domain/Domain.py`
- Modify: `src/back/objects/domain/SettingsService.py`
- Modify: `src/api/routers/internal/domain.py`
- Modify: `src/api/routers/internal/settings.py`

**Interfaces:**
- Consumes: Task 1’s capability helpers.
- Produces: `/domain/versions-list` entries with metadata, transitions, and deletion capability.
- Produces: `/settings/registry/domains` version entries with deletion capability.

- [ ] **Step 1: Write failing Domain capability tests**

Create `tests/units/domain/test_version_capabilities.py` with a mocked session
domain and registry:

```python
import importlib
from unittest.mock import MagicMock, patch

from back.objects.domain.Domain import Domain
from back.objects.domain.SettingsService import SettingsService
from back.objects.registry.PermissionService import ROLE_ADMIN, ROLE_BUILDER

settings_module = importlib.import_module("back.objects.domain.SettingsService")


def _domain_service(*, status="DRAFT", current="3"):
    session = MagicMock()
    session.uc_domain_folder = "acme"
    session.current_version = current
    service = MagicMock()
    service.cfg.is_configured = True
    service.list_versions_sorted.return_value = ["3", "2", "1"]
    service.read_version.side_effect = lambda _folder, version: (
        True,
        {
            "info": {
                "status": status if version == "1" else "DRAFT",
                "description": f"Version {version}",
                "author": "alice@example.com",
                "last_update": "2026-09-25T10:00:00Z",
                "last_build": "2026-09-25T09:00:00Z",
            },
            "ontology": {"classes": [{"name": "Person"}]},
        },
        "",
    )
    return Domain(session), service


def test_version_list_exposes_card_metadata_and_capabilities():
    domain, service = _domain_service()
    result = domain.list_version_details(
        service,
        user_role=ROLE_ADMIN,
        user_domain_role=ROLE_BUILDER,
    )
    old = next(item for item in result["versions"] if item["version"] == "1")
    assert old["last_build"] == "2026-09-25T09:00:00Z"
    assert old["transitions"][0]["target_status"] == "IN-REVIEW"
    assert old["delete_control_visible"] is True
    assert old["can_delete"] is True


def test_latest_version_delete_is_blocked_in_payload():
    domain, service = _domain_service()
    result = domain.list_version_details(service, user_role=ROLE_ADMIN)
    latest = result["versions"][0]
    assert latest["is_active"] is True
    assert latest["can_delete"] is False
    assert "latest" in latest["delete_block_reason"]
```

- [ ] **Step 2: Add a failing Registry decoration test**

In the same file:

```python
def test_registry_listing_gets_same_delete_capability():
    domain = MagicMock()
    domain.domain_folder = "acme"
    domain.current_version = "3"
    service = MagicMock()
    service.cfg.is_configured = True
    service.list_domain_details_cached.return_value = (
        True,
        [
            {
                "name": "acme",
                "versions": [
                    {"version": "3", "status": "DRAFT"},
                    {"version": "1", "status": "DRAFT"},
                ],
            }
        ],
        "",
    )
    with (
        patch.object(settings_module, "get_domain", return_value=domain),
        patch.object(
            settings_module.RegistryService,
            "from_context",
            return_value=service,
        ),
    ):
        result = SettingsService.list_registry_domains_result(
            MagicMock(),
            MagicMock(),
            user_role=ROLE_ADMIN,
        )
    versions = result["domains"][0]["versions"]
    assert versions[0]["can_delete"] is False
    assert versions[1]["can_delete"] is True
```

- [ ] **Step 3: Run tests and verify RED**

```bash
uv run --frozen pytest -q tests/units/domain/test_version_capabilities.py
```

Expected: role parameters and capability fields are missing.

- [ ] **Step 4: Enrich `Domain.list_version_details`**

Change its signature:

```python
def list_version_details(
    self,
    svc: RegistryService,
    *,
    user_role: str = "",
    user_domain_role: str = "",
) -> Dict[str, Any]:
```

For every successfully read version, derive:

```python
info = data.get("info", {})
status = (info.get("status") or "DRAFT").upper()
is_current = ver == self._s.current_version
is_active = ver == latest
detail = {
    "version": ver,
    "description": info.get("description", ""),
    "status": status,
    "author": info.get("author", ""),
    "last_update": info.get("last_update", ""),
    "last_build": info.get("last_build", ""),
    "is_active": is_active,
    "is_current": is_current,
    "transitions": transition_capabilities(
        status,
        user_role=user_role,
        user_domain_role=user_domain_role,
        last_build=info.get("last_build", "") or "",
        has_ontology=svc.version_document_has_ontology(data, ver),
    ),
}
detail.update(
    version_deletion_capability(
        user_role=user_role,
        status=status,
        is_loaded=is_current,
        is_latest=is_active,
        version_count=len(sorted_versions),
    )
)
details.append(detail)
```

Apply the same capability shape to unreadable rows with an empty transition
list and a blocked delete reason.

- [ ] **Step 5: Forward request roles from `/domain/versions-list`**

Add `request: Request` and call:

```python
return p.list_version_details(
    p.build_registry_service(),
    user_role=getattr(request.state, "user_role", "") or "",
    user_domain_role=getattr(request.state, "user_domain_role", "") or "",
)
```

- [ ] **Step 6: Decorate Registry listing without mutating cached data**

In `SettingsService.list_registry_domains_result`, accept keyword-only
`user_role`, deep-copy `result`, and for each domain:

```python
versions = item.get("versions", []) or []
latest = versions[0].get("version") if versions else ""
for version_data in versions:
    version = str(version_data.get("version", ""))
    version_data.update(
        version_deletion_capability(
            user_role=user_role,
            status=version_data.get("status", "DRAFT"),
            is_loaded=(
                domain.domain_folder == item.get("name")
                and domain.current_version == version
            ),
            is_latest=version == latest,
            version_count=len(versions),
        )
    )
```

In `settings.py`, pass:

```python
user_role = getattr(request.state, "user_role", "") or ""
result = config_service.list_registry_domains_result(
    session_mgr,
    settings,
    user_role=user_role,
)
```

- [ ] **Step 7: Run capability and existing status tests**

```bash
uv run --frozen pytest -q \
  tests/units/domain/test_version_capabilities.py \
  tests/units/settings/test_settings_version_status.py \
  tests/units/api/test_set_version_status_endpoint.py
```

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```bash
git add \
  src/back/objects/domain/Domain.py \
  src/back/objects/domain/SettingsService.py \
  src/api/routers/internal/domain.py \
  src/api/routers/internal/settings.py \
  tests/units/domain/test_version_capabilities.py
git commit -m "feat(domain): expose version action capabilities"
```

---

### Task 4: Domain Versions card workspace

**Files:**
- Create: `src/front/static/domain/css/domain-versions.css`
- Create: `tests/units/front/test_domain_versions_cards.py`
- Modify: `src/front/templates/domain.html`
- Modify: `src/front/templates/partials/domain/_domain_versions.html`
- Modify: `src/front/static/domain/js/domain-versions.js`
- Modify: `tests/units/front/test_create_version_ungated.py`

**Interfaces:**
- Consumes: Task 3’s `/domain/versions-list` payload.
- Calls: existing `POST /domain/set-version-status`.
- Calls: Task 2’s `DELETE /domain/versions/{version}`.
- Preserves: `window.createNewDomainVersion()` and existing load/reload endpoints.

- [ ] **Step 1: Write failing markup, CSS, and JS contracts**

Create `tests/units/front/test_domain_versions_cards.py`:

```python
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[3]
HTML = ROOT / "src/front/templates/partials/domain/_domain_versions.html"
PAGE = ROOT / "src/front/templates/domain.html"
CSS = ROOT / "src/front/static/domain/css/domain-versions.css"
JS = ROOT / "src/front/static/domain/js/domain-versions.js"


def test_versions_use_semantic_card_list_not_table():
    html = HTML.read_text()
    assert 'id="versionsCardList"' in html
    assert 'role="list"' in html
    assert "<table" not in html
    assert "onclick=" not in html
    assert "style=" not in html


def test_versions_stylesheet_is_wired():
    assert "domain/css/domain-versions.css" in PAGE.read_text()
    assert CSS.exists()


def test_card_css_owns_full_height_scroll_and_mobile_reset():
    css = CSS.read_text()
    assert "#versionsCardList" in css
    assert "overflow-y: auto" in css
    mobile = css[css.index("@media (max-width: 768px)") :]
    assert "overflow: visible" in mobile
    assert "height: auto" in mobile


def test_js_renders_server_capabilities_and_new_endpoints():
    js = JS.read_text()
    assert "version.transitions" in js
    assert "version.can_delete" in js
    assert "version.delete_control_visible" in js
    assert "/domain/set-version-status" in js
    assert "/domain/versions/" in js
    assert "STATUS_MAP" in js
    assert "is_latest" not in js
    assert "status === 'DRAFT'" not in js
```

- [ ] **Step 2: Run frontend contracts and verify RED**

```bash
uv run --frozen pytest -q \
  tests/units/front/test_domain_versions_cards.py \
  tests/units/front/test_create_version_ungated.py
```

Expected: card list and stylesheet assertions fail.

- [ ] **Step 3: Replace the table shell with semantic states**

Rewrite `_domain_versions.html` with:

```html
<div class="content-section">
    <div class="section-header d-flex justify-content-between align-items-center mb-4">
        <div>
            <h4 class="mb-1"><i class="bi bi-clock-history me-2"></i>Versions</h4>
            <p class="text-muted mb-0 small">
                Review version history, lifecycle, and available actions.
            </p>
        </div>
        <div class="d-flex align-items-center gap-2">
            <button type="button" class="btn btn-sm btn-outline-secondary"
                    id="btnReloadVersion"
                    title="Reload the current version from the registry">
                <i class="bi bi-arrow-counterclockwise me-1"></i>Reload Saved
            </button>
            <button type="button" class="btn btn-sm btn-primary"
                    id="btnAddVersion"
                    title="Create a new version from the current state">
                <i class="bi bi-plus-circle me-1"></i>New Version
            </button>
        </div>
    </div>

    <section class="dm-versions-workspace card"
             aria-labelledby="versionsListHeading">
        <h5 id="versionsListHeading" class="visually-hidden">Saved versions</h5>
        <div id="versionsLoading" class="dm-versions-state text-center py-5">
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading versions</span>
            </div>
            <p class="text-muted mt-2 mb-0 small">Loading versions…</p>
        </div>
        <div id="versionsEmpty"
             class="dm-versions-state text-center py-5 ob-hidden">
            <i class="bi bi-clock-history text-muted fs-1"></i>
            <p class="text-muted mt-2 mb-1">No versions found</p>
            <p class="text-muted small mb-0">
                Save the domain to start tracking versions.
            </p>
        </div>
        <div id="versionsError"
             class="alert alert-warning m-3 ob-hidden"
             role="alert">
            <i class="bi bi-exclamation-triangle me-1"></i>
            <span id="versionsErrorMessage"></span>
            <button type="button"
                    class="btn btn-sm btn-outline-warning ms-2"
                    id="versionsRetryBtn">Retry</button>
        </div>
        <div id="versionsCardList"
             class="dm-version-card-list ob-hidden"
             role="list"></div>
    </section>
</div>
```

- [ ] **Step 4: Add the full-height responsive CSS**

Create `domain-versions.css`:

```css
#versions-section .content-section {
    overflow: hidden;
}

.dm-versions-workspace {
    flex: 1;
    min-height: 0;
    overflow: hidden;
}

.dm-version-card-list {
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 1rem;
}

.dm-version-card-list.ob-hidden {
    display: none;
}

.dm-version-card {
    border-left: 3px solid transparent;
}

.dm-version-card + .dm-version-card {
    margin-top: 0.75rem;
}

.dm-version-card.is-loaded {
    border-left-color: var(--db-primary);
}

.dm-version-card-header,
.dm-version-card-actions,
.dm-version-card-meta {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
}

.dm-version-card-actions {
    justify-content: space-between;
}

.dm-version-card-meta {
    color: var(--db-text-muted);
    font-size: 0.8rem;
}

.dm-version-card-description {
    color: var(--db-text-secondary);
}

.dm-version-action-group {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    flex-wrap: wrap;
}

@media (max-width: 768px) {
    #versions-section .content-section,
    .dm-versions-workspace,
    .dm-version-card-list {
        flex: none;
        height: auto;
        max-height: none;
        overflow: visible;
    }
}
```

Wire it in `domain.html` after `domain-review.css`.

- [ ] **Step 5: Rewrite the JS renderer and event delegation**

Keep `loadVersionsList`, `loadVersionFromList`, `addNewVersionFromList`, and
`reloadLastSavedVersion`, but replace table rendering with:

```javascript
const VERSION_STATUS_MAP = {
    'DRAFT': {
        cls: 'bg-warning-subtle text-dark border-warning',
        icon: 'pencil',
        label: 'Draft'
    },
    'IN-REVIEW': {
        cls: 'bg-info-subtle text-dark border-info',
        icon: 'eye',
        label: 'In Review'
    },
    'PUBLISHED': {
        cls: 'bg-success-subtle text-dark border-success',
        icon: 'broadcast',
        label: 'Published'
    }
};

function renderVersionCard(version, domainFolder) {
    const status = VERSION_STATUS_MAP[String(version.status || 'DRAFT').toUpperCase()]
        || VERSION_STATUS_MAP.DRAFT;
    const transitions = (version.transitions || []).map((transition) => {
        const disabled = transition.enabled ? '' : ' disabled';
        const reason = transition.blocked_reason
            ? ' title="' + escapeHtml(transition.blocked_reason) + '"'
            : '';
        return '<button type="button" class="btn btn-sm btn-outline-primary"'
            + ' data-action="transition"'
            + ' data-version="' + escapeHtml(version.version) + '"'
            + ' data-domain="' + escapeHtml(domainFolder) + '"'
            + ' data-target-status="' + escapeHtml(transition.target_status) + '"'
            + disabled + reason + '>'
            + escapeHtml(transition.label) + '</button>';
    }).join('');
    const load = version.is_current ? '' :
        '<button type="button" class="btn btn-sm btn-outline-primary"'
        + ' data-action="load" data-version="' + escapeHtml(version.version) + '">'
        + '<i class="bi bi-box-arrow-in-down me-1"></i>Load</button>';
    let deletion = '';
    if (version.delete_control_visible) {
        const disabled = version.can_delete ? '' : ' disabled';
        const title = escapeHtml(
            version.delete_block_reason || ('Delete version v' + version.version)
        );
        deletion = '<span tabindex="0" title="' + title + '">'
            + '<button type="button" class="btn btn-sm btn-outline-danger"'
            + ' data-action="delete" data-version="' + escapeHtml(version.version) + '"'
            + disabled + '><i class="bi bi-trash me-1"></i>Delete</button></span>';
    }
    return '<article class="card dm-version-card'
        + (version.is_current ? ' is-loaded' : '') + '" role="listitem"'
        + ' aria-labelledby="version-title-' + escapeHtml(version.version) + '">'
        + '<div class="card-body">'
        + '<div class="dm-version-card-header">'
        + '<h5 class="mb-0" id="version-title-' + escapeHtml(version.version) + '">'
        + 'v' + escapeHtml(version.version) + '</h5>'
        + '<span class="badge border ' + status.cls + '"><i class="bi bi-'
        + status.icon + ' me-1"></i>' + status.label + '</span>'
        + (version.is_current ? '<span class="badge bg-primary">Loaded</span>' : '')
        + (version.is_active ? '<span class="badge bg-secondary">Latest</span>' : '')
        + '</div>'
        + '<p class="dm-version-card-description mt-2 mb-2">'
        + escapeHtml(version.description || 'No description') + '</p>'
        + '<div class="dm-version-card-meta mb-3">'
        + '<span><i class="bi bi-person me-1"></i>'
        + escapeHtml(version.author || 'Unknown author') + '</span>'
        + '<span><i class="bi bi-clock me-1"></i>'
        + escapeHtml(version.last_update || 'No update date') + '</span>'
        + '<span><i class="bi bi-hammer me-1"></i>'
        + escapeHtml(version.last_build || 'Not built') + '</span>'
        + '</div>'
        + '<div class="dm-version-card-actions">'
        + '<div class="dm-version-action-group">' + transitions + '</div>'
        + '<div class="dm-version-action-group">' + load + deletion + '</div>'
        + '</div></div></article>';
}
```

Use `classList.toggle('ob-hidden', ...)` for all states. Add one delegated
listener on `versionsCardList`, plus listeners for `btnReloadVersion`,
`btnAddVersion`, and `versionsRetryBtn`.

Add:

```javascript
async function transitionVersion(domainFolder, version, targetStatus) {
    const confirmed = await showConfirmDialog({
        title: 'Update Lifecycle Status',
        message: 'Change version v' + escapeHtml(version) + ' to '
            + escapeHtml(targetStatus) + '?',
        confirmText: 'Update Status',
        confirmClass: 'btn-primary',
        icon: 'arrow-repeat'
    });
    if (!confirmed) return;
    const response = await fetch('/domain/set-version-status', {
        method: 'POST',
        credentials: 'same-origin',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            domain_name: domainFolder,
            version: version,
            status: targetStatus
        })
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
        throw new Error(data.message || 'Version status update failed');
    }
    await loadVersionsList(true);
}


async function deleteVersionFromList(version) {
    const confirmed = await showConfirmDialog({
        title: 'Delete Version',
        message: 'Permanently delete version v' + escapeHtml(version)
            + ' and its Knowledge Store content? This cannot be undone.',
        confirmText: 'Delete Version',
        confirmClass: 'btn-danger',
        icon: 'trash'
    });
    if (!confirmed) return;
    const response = await fetch(
        '/domain/versions/' + encodeURIComponent(version),
        {method: 'DELETE', credentials: 'same-origin'}
    );
    const data = await response.json();
    if (!response.ok || !data.success) {
        if (response.status === 409) await loadVersionsList(true);
        throw new Error(data.message || 'Version deletion failed');
    }
    showNotification(data.message, 'success');
    await loadVersionsList(true);
}
```

- [ ] **Step 6: Preserve New Version contracts**

Update `test_create_version_ungated.py` only if its parser depends on removed
inline handlers. Keep these assertions true:

```python
assert 'id="btnAddVersion"' in html
assert "disabled" not in btn.group(0)
assert "createNewDomainVersion()" in fn
assert "/domain/create-version" not in fn
```

- [ ] **Step 7: Run frontend unit tests**

```bash
uv run --frozen pytest -q \
  tests/units/front/test_domain_versions_cards.py \
  tests/units/front/test_create_version_ungated.py \
  tests/units/front/test_sidebar_content_stretch_contract.py
```

Expected: all selected tests pass.

- [ ] **Step 8: Commit**

```bash
git add \
  src/front/static/domain/css/domain-versions.css \
  src/front/templates/domain.html \
  src/front/templates/partials/domain/_domain_versions.html \
  src/front/static/domain/js/domain-versions.js \
  tests/units/front/test_domain_versions_cards.py \
  tests/units/front/test_create_version_ungated.py
git commit -m "feat(domain): redesign versions as action cards"
```

---

### Task 5: Harden Registry → Browse deletion

**Files:**
- Modify: `src/front/static/registry/js/registry.js`
- Modify: `tests/units/front/test_domain_versions_cards.py`

**Interfaces:**
- Consumes: Task 3’s `delete_control_visible`, `can_delete`, and `delete_block_reason` on Registry version entries.
- Calls: Task 2’s guarded Settings DELETE endpoint.

- [ ] **Step 1: Add a failing Registry contract**

Append to `test_domain_versions_cards.py`:

```python
REGISTRY_JS = ROOT / "src/front/static/registry/js/registry.js"


def test_registry_uses_server_delete_capability():
    js = REGISTRY_JS.read_text()
    assert "v.delete_control_visible" in js
    assert "v.can_delete" in js
    assert "v.delete_block_reason" in js
    delete_markup = js[js.index("const deleteBtn = isLoaded") : js.index(
        "html += '<div class=\"registry-version-row"
    )]
    assert "isLoaded" not in delete_markup
```

- [ ] **Step 2: Run and verify RED**

```bash
uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py
```

Expected: Registry still derives delete visibility from `isLoaded`.

- [ ] **Step 3: Render Registry delete from server capability**

Replace the per-version `deleteBtn` branch with:

```javascript
let deleteBtn = '';
if (v.delete_control_visible) {
    const disabled = v.can_delete ? '' : ' disabled';
    const reason = escapeHtml(
        v.delete_block_reason || ('Delete version v' + ver)
    );
    deleteBtn = '<span tabindex="0" title="' + reason + '">'
        + '<button type="button"'
        + ' class="btn btn-sm btn-outline-danger border-0 registry-delete-version-btn"'
        + ' data-domain="' + escapeHtml(d.name) + '"'
        + ' data-version="' + escapeHtml(ver) + '"'
        + disabled + '><i class="bi bi-trash"></i></button></span>';
}
```

Keep `deleteRegistryVersion` and its guarded endpoint. If deletion returns
`409`, show the message and force `loadRegistryDomains(true)` so capability
reasons refresh.

- [ ] **Step 4: Run frontend contracts**

```bash
uv run --frozen pytest -q tests/units/front/test_domain_versions_cards.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/front/static/registry/js/registry.js tests/units/front/test_domain_versions_cards.py
git commit -m "fix(registry): honor guarded version deletion capability"
```

---

### Task 6: Browser contracts, documentation, changelog, and full verification

**Files:**
- Create: `tests/e2e/domain/test_domain_versions_cards.py`
- Modify: `docs/user-guide.md`
- Modify: `docs/features.md`
- Modify/Create: `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log`

**Interfaces:**
- Verifies the complete feature from Tasks 1–5.

- [ ] **Step 1: Add mocked browser contracts**

Create `tests/e2e/domain/test_domain_versions_cards.py`:

```python
import json

import pytest

pytestmark = pytest.mark.e2e

DESKTOP = {"width": 1600, "height": 1000}
MOBILE = {"width": 390, "height": 844}

VERSIONS = {
    "success": True,
    "domain_folder": "acme",
    "versions": [
        {
            "version": "3",
            "description": "Current version",
            "status": "DRAFT",
            "author": "alice@example.com",
            "last_update": "2026-09-25T10:00:00Z",
            "last_build": "",
            "is_current": True,
            "is_active": True,
            "transitions": [],
            "delete_control_visible": True,
            "can_delete": False,
            "delete_block_reason": "The latest version cannot be deleted.",
        },
        {
            "version": "1",
            "description": "Old draft",
            "status": "DRAFT",
            "author": "alice@example.com",
            "last_update": "2026-09-20T10:00:00Z",
            "last_build": "2026-09-20T11:00:00Z",
            "is_current": False,
            "is_active": False,
            "transitions": [
                {
                    "target_status": "IN-REVIEW",
                    "label": "Submit for Review",
                    "enabled": True,
                    "blocked_reason": "",
                }
            ],
            "delete_control_visible": True,
            "can_delete": True,
            "delete_block_reason": "",
        },
    ],
}


def _open(page, live_server, viewport):
    page.set_viewport_size(viewport)
    page.route(
        "**/domain/versions-list",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(VERSIONS),
        ),
    )
    page.goto(f"{live_server}/domain?section=versions")
    page.wait_for_load_state("domcontentloaded")
    page.evaluate("SidebarNav.switchTo('versions')")
    page.locator(".dm-version-card").first.wait_for(state="visible")


def test_desktop_cards_order_actions_and_bottom_alignment(page, live_server):
    console_errors = []
    page.on(
        "console",
        lambda message: console_errors.append(message.text)
        if message.type == "error"
        else None,
    )
    _open(page, live_server, DESKTOP)
    cards = page.locator(".dm-version-card")
    assert cards.count() == 2
    assert cards.nth(0).locator("h5").inner_text() == "v3"
    assert cards.nth(1).locator("h5").inner_text() == "v1"
    assert cards.nth(1).get_by_text("Submit for Review").is_visible()
    assert cards.nth(1).get_by_text("Delete").is_enabled()
    bottoms = page.evaluate(
        """() => ({
            sidebar: document.querySelector('.sidebar-nav').getBoundingClientRect().bottom,
            workspace: document.querySelector('.dm-versions-workspace').getBoundingClientRect().bottom
        })"""
    )
    assert abs(bottoms["sidebar"] - bottoms["workspace"]) <= 1
    assert console_errors == []


def test_mobile_cards_use_natural_page_flow(page, live_server):
    _open(page, live_server, MOBILE)
    flow = page.evaluate(
        """() => ({
            overflow: getComputedStyle(document.querySelector('#versionsCardList')).overflowY,
            horizontal: document.documentElement.scrollWidth - window.innerWidth
        })"""
    )
    assert flow["overflow"] == "visible"
    assert flow["horizontal"] <= 1


def test_transition_posts_target_status_and_refreshes(page, live_server):
    requests = []
    captured = {}
    page.on("request", lambda request: requests.append(request.url))

    def handle_transition(route):
        captured.update(route.request.post_data_json)
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"success": True, "status": "IN-REVIEW"}),
        )

    page.route("**/domain/set-version-status", handle_transition)
    _open(page, live_server, DESKTOP)
    page.evaluate("window.showConfirmDialog = () => Promise.resolve(true)")
    page.get_by_text("Submit for Review").click()
    page.wait_for_timeout(100)
    assert captured == {
        "domain_name": "acme",
        "version": "1",
        "status": "IN-REVIEW",
    }
    assert sum(url.endswith("/domain/versions-list") for url in requests) >= 2


def test_delete_calls_loaded_domain_endpoint_and_refreshes(page, live_server):
    requests = []
    deleted = []
    page.on("request", lambda request: requests.append(request.url))

    def handle_delete(route):
        deleted.append(route.request.url)
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(
                {"success": True, "message": 'Version 1 deleted from "acme"'}
            ),
        )

    page.route("**/domain/versions/1", handle_delete)
    _open(page, live_server, DESKTOP)
    page.evaluate("window.showConfirmDialog = () => Promise.resolve(true)")
    page.locator(".dm-version-card").nth(1).get_by_text("Delete").click()
    page.wait_for_timeout(100)
    assert deleted and deleted[0].endswith("/domain/versions/1")
    assert sum(url.endswith("/domain/versions-list") for url in requests) >= 2


def test_cancelled_delete_returns_focus_to_action(page, live_server):
    _open(page, live_server, DESKTOP)
    page.evaluate("window.showConfirmDialog = () => Promise.resolve(false)")
    delete_button = page.locator(".dm-version-card").nth(1).get_by_text("Delete")
    delete_button.focus()
    delete_button.click()
    page.wait_for_timeout(50)
    assert delete_button.evaluate("(button) => document.activeElement === button")
```

- [ ] **Step 2: Run the targeted browser suite**

```bash
uv run --frozen pytest -q tests/e2e/domain/test_domain_versions_cards.py
```

Expected: all new browser tests pass with no page console errors.

- [ ] **Step 3: Update user documentation**

Replace `docs/user-guide.md`’s “Version Management” list with:

```markdown
### Version Management (Domain → Versions)

Domain → Versions presents saved versions as newest-first cards. Each card
shows lifecycle status, description, author, last update, last build, and
whether the version is Loaded or Latest.

- **Lifecycle:** permitted Submit for Review, Return to Draft, Publish, and
  Reopen actions appear directly on the card. Disabled actions explain their
  unmet precondition.
- **Load:** loads another version after confirming that unsaved changes will
  be discarded.
- **Delete:** app administrators may permanently delete only an older,
  unloaded Draft version. Loaded, latest, In Review, and Published versions
  are protected. Deletion also removes that version’s Knowledge Store content.
- **New Version:** branches the current state into the next Draft version.
- **Reload Saved:** discards local edits and reloads the loaded version.
```

Update `docs/features.md` so Version Control mentions cards, inline lifecycle,
and guarded admin deletion. Do not claim that lifecycle status is the same as
API/MCP Active selection.

- [ ] **Step 4: Run lint diagnostics on changed files**

Use `ReadLints` for all changed Python, JavaScript, CSS, HTML, and test files.
Fix only diagnostics introduced by this implementation.

- [ ] **Step 5: Run focused regression suites**

```bash
uv run --frozen pytest -q \
  tests/units/registry/test_version_lifecycle.py \
  tests/units/settings/test_settings_version_deletion.py \
  tests/units/settings/test_settings_version_status.py \
  tests/units/domain/test_version_capabilities.py \
  tests/units/api/test_delete_version_endpoints.py \
  tests/units/api/test_set_version_status_endpoint.py \
  tests/units/front/test_domain_versions_cards.py \
  tests/units/front/test_create_version_ungated.py \
  tests/units/front/test_sidebar_content_stretch_contract.py \
  tests/units/auth/test_permission_middleware.py
```

Expected: all selected tests pass.

- [ ] **Step 6: Run the mandatory full suite**

```bash
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures. Record the exact summary in the changelog.

- [ ] **Step 7: Write the changelog section**

Append/create `changelogs/v0.9.0/benoitcayladbx_2026-09-25.log`:

```text
## Redesign Domain Versions with guarded deletion

Context: Domain → Versions used a sparse technical table and sent users to
Registry for lifecycle actions. Registry deletion also lacked server-side
guards for loaded, latest, reviewed, or published versions.

Changes:

1. src/back/objects/registry/version_lifecycle.py
   Add server-authoritative lifecycle capabilities and conservative deletion policy.
2. src/back/objects/domain/SettingsService.py
   Route Domain and Registry version deletion through one guarded orchestration.
3. src/api/routers/internal/domain.py
   Add loaded-domain deletion and role-aware card capabilities.
4. src/api/routers/internal/settings.py
   Apply shared deletion capabilities to Registry Browse.
5. src/back/objects/domain/Domain.py
   Return card metadata and permitted actions for every version.
6. src/front/templates/partials/domain/_domain_versions.html
   Replace the table with an accessible card-list workspace.
7. src/front/static/domain/css/domain-versions.css
   Add full-height desktop cards and natural-flow mobile behavior.
8. src/front/static/domain/js/domain-versions.js
   Render cards and execute lifecycle, load, and guarded delete actions.
9. src/front/static/registry/js/registry.js
   Stop inferring delete safety and consume server capabilities.
10. tests/
    Add policy, API, service, frontend, and browser regression coverage.
11. docs/user-guide.md and docs/features.md
    Document cards, inline lifecycle, and deletion restrictions.

Modified files:
- src/back/objects/registry/version_lifecycle.py
- src/back/objects/domain/SettingsService.py
- src/back/objects/domain/Domain.py
- src/api/routers/internal/domain.py
- src/api/routers/internal/settings.py
- src/front/templates/domain.html
- src/front/templates/partials/domain/_domain_versions.html
- src/front/static/domain/css/domain-versions.css
- src/front/static/domain/js/domain-versions.js
- src/front/static/registry/js/registry.js
- tests/units/registry/test_version_lifecycle.py
- tests/units/settings/test_settings_version_deletion.py
- tests/units/domain/test_version_capabilities.py
- tests/units/api/test_delete_version_endpoints.py
- tests/units/front/test_domain_versions_cards.py
- tests/units/front/test_create_version_ungated.py
- tests/e2e/domain/test_domain_versions_cards.py
- docs/user-guide.md
- docs/features.md
- changelogs/v0.9.0/benoitcayladbx_2026-09-25.log
```

Immediately after the closing `Modified files` list, append a `Tests:` line
containing the literal final summary emitted by Step 6. Do not paraphrase or
estimate the counts.

- [ ] **Step 8: Commit documentation, tests, and changelog**

```bash
git add \
  tests/e2e/domain/test_domain_versions_cards.py \
  docs/user-guide.md \
  docs/features.md \
  changelogs/v0.9.0/benoitcayladbx_2026-09-25.log
git commit -m "test(domain): verify version cards and safe deletion"
```

- [ ] **Step 9: Final diff review**

```bash
git status --short
git diff --check HEAD~6..HEAD
git log --oneline -6
```

Expected: clean working tree, no whitespace errors, and one reviewable commit
per task (fewer than six prior commits is acceptable if execution intentionally
combined documentation with its owning task).
