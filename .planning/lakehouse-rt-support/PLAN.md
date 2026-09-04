# Lakehouse RT (SEA) Support — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a **Lakehouse RT** checkbox on *Settings → Lakehouse → SQL Warehouse* that
makes the Lakehouse (Delta) SQL client connect via the Statement Execution API (SEA)
instead of Thrift, so serverless Lakehouse/RT warehouses can create the R2RML VIEW and
build the triple store.

**Architecture:** The flag is stored next to the Lakehouse warehouse id in
`graph_engine_config.lakehouse.use_sea`. It flows: global config → `resolve_lakehouse_use_sea`
→ `DeltaBase.create_databricks_client` → `DatabricksClient` → `DatabricksAuth` →
`use_sea=True` in `databricks.sql.connect()` kwargs. Only the Lakehouse graph client is
affected; the global Databricks warehouse client is unchanged unless it is the same id.

**Tech Stack:** FastAPI, `databricks-sql-connector>=4.4.0` (resolved 4.5.0, SEA-capable),
Jinja2 + Bootstrap templates, vanilla JS, pytest.

## Global Constraints

- Default **off** (opt-in). Absent key ⇒ `False` ⇒ Thrift, so existing installs are unchanged.
- Do **not** bump `databricks-sql-connector`. 4.5.0 already supports SEA; the fix is passing
  `use_sea=True`. (`pyproject.toml:33`)
- SEA and CloudFetch coexist in 4.x — keep `use_cloud_fetch` handling intact.
- Errors go through the `OntoBricksError` hierarchy; never return `{'success': False}` from
  services or expose `str(e)` to clients (`.cursor/05` Error Handling).
- Logging: `%`-style, `get_logger(__name__)`, English only.
- After all changes: update `/changelogs/` (version `0.8.0`) and run
  `uv run --frozen pytest -q -m "not scenario"`.

---

### Task 1: Config normalization + resolver in `engine_config`

**Files:**
- Modify: `src/back/core/graphdb/engine_config.py:236-243` (`_finalize_lakehouse_bucket`)
- Modify: `src/back/core/graphdb/engine_config.py:298-300` (add `resolve_lakehouse_use_sea`)
- Test: `tests/units/core/graphdb/test_engine_config.py` (add cases; create if absent)

**Interfaces:**
- Produces: `resolve_lakehouse_use_sea(cfg: Optional[Mapping[str, Any]]) -> bool`
- Produces: `lakehouse` bucket always carries a normalized `use_sea: bool` key.

- [ ] **Step 1: Write failing test**

```python
# tests/units/core/graphdb/test_engine_config.py
import pytest
from back.core.graphdb.engine_config import (
    normalize_graph_engine_config,
    resolve_lakehouse_use_sea,
)


@pytest.mark.unit
class TestLakehouseUseSea:
    def test_default_false_when_absent(self):
        cfg = normalize_graph_engine_config({"lakehouse": {"warehouse_id": "wh"}})
        assert cfg["lakehouse"]["use_sea"] is False
        assert resolve_lakehouse_use_sea(cfg) is False

    def test_true_roundtrips(self):
        cfg = normalize_graph_engine_config(
            {"lakehouse": {"warehouse_id": "wh", "use_sea": True}}
        )
        assert cfg["lakehouse"]["use_sea"] is True
        assert resolve_lakehouse_use_sea(cfg) is True

    def test_truthy_string_coerced(self):
        cfg = {"lakehouse": {"use_sea": "true"}}
        assert resolve_lakehouse_use_sea(cfg) is True
        assert resolve_lakehouse_use_sea({"lakehouse": {"use_sea": "off"}}) is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/core/graphdb/test_engine_config.py::TestLakehouseUseSea -q`
Expected: FAIL (`ImportError: cannot import name 'resolve_lakehouse_use_sea'`).

- [ ] **Step 3: Implement**

```python
# _finalize_lakehouse_bucket — add after the warehouse_id block, before `return out`
def _finalize_lakehouse_bucket(lakehouse: MutableMapping[str, Any]) -> Dict[str, Any]:
    out = dict(lakehouse)
    wid = str(out.get("warehouse_id") or "").strip()
    if wid:
        out["warehouse_id"] = wid
    elif "warehouse_id" in out:
        out["warehouse_id"] = ""
    out["use_sea"] = _coerce_bool(out.get("use_sea"), default=False)
    return out
```

```python
# module-level helper (near the other coercion helpers)
def _coerce_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default
```

```python
# public resolver, next to resolve_lakehouse_warehouse_id
def resolve_lakehouse_use_sea(cfg: Optional[Mapping[str, Any]]) -> bool:
    """Return ``lakehouse.use_sea`` (default ``False`` — Thrift)."""
    return _coerce_bool(lakehouse_section(cfg).get("use_sea"), default=False)
```

If a `_coerce_bool` equivalent already exists in the module, reuse it instead of adding one.

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/core/graphdb/test_engine_config.py::TestLakehouseUseSea -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graphdb/engine_config.py tests/units/core/graphdb/test_engine_config.py
git commit -m "feat(lakehouse): normalize + resolve graph_engine_config.lakehouse.use_sea"
```

---

### Task 2: Persist `use_sea` via `GlobalConfigService`

**Files:**
- Modify: `src/back/objects/session/GlobalConfigService.py:159-167` (add `get_delta_warehouse_use_sea`)
- Modify: `src/back/objects/session/GlobalConfigService.py:402-427` (`set_delta_warehouse_id` gains `use_sea`)
- Test: `tests/units/session/test_global_config_service.py` (add; match existing path/name)

**Interfaces:**
- Consumes: `resolve_lakehouse_use_sea` (Task 1).
- Produces: `get_delta_warehouse_use_sea(host, token, registry_cfg) -> bool`
- Produces: `set_delta_warehouse_id(host, token, registry_cfg, warehouse_id, use_sea: Optional[bool] = None)`
  — writes `use_sea` only when provided (preserves existing value otherwise).

- [ ] **Step 1: Write failing test** (use the existing fake/in-memory store fixture in that suite)

```python
def test_set_and_get_delta_warehouse_use_sea(global_config_service_with_store):
    gcs, ctx = global_config_service_with_store  # (host, token, registry_cfg)
    host, token, reg = ctx
    gcs.set_delta_warehouse_id(host, token, reg, "wh-1", use_sea=True)
    assert gcs.get_delta_warehouse_id(host, token, reg) == "wh-1"
    assert gcs.get_delta_warehouse_use_sea(host, token, reg) is True

def test_set_delta_warehouse_preserves_use_sea_when_omitted(global_config_service_with_store):
    gcs, (host, token, reg) = global_config_service_with_store
    gcs.set_delta_warehouse_id(host, token, reg, "wh-1", use_sea=True)
    gcs.set_delta_warehouse_id(host, token, reg, "wh-2")  # no use_sea
    assert gcs.get_delta_warehouse_use_sea(host, token, reg) is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/session/test_global_config_service.py -q -k use_sea`
Expected: FAIL (`AttributeError: get_delta_warehouse_use_sea`).

- [ ] **Step 3: Implement**

```python
# getter — after get_delta_warehouse_id (:159-167)
def get_delta_warehouse_use_sea(
    self, host: str, token: str, registry_cfg: Dict[str, str]
) -> bool:
    """Return ``graph_engine_config.lakehouse.use_sea`` (default False)."""
    from back.core.graphdb.engine_config import resolve_lakehouse_use_sea

    return resolve_lakehouse_use_sea(
        self.get_graph_engine_config(host, token, registry_cfg)
    )
```

```python
# setter — extend signature + body (:402-427)
def set_delta_warehouse_id(
    self,
    host: str,
    token: str,
    registry_cfg: Dict[str, str],
    warehouse_id: str,
    use_sea: Optional[bool] = None,
) -> Tuple[bool, str]:
    """Persist the Lakehouse SQL warehouse (and optionally use_sea) under
    ``graph_engine_config.lakehouse``."""
    from back.core.graphdb.engine_config import normalize_graph_engine_config

    wid = (warehouse_id or "").strip()
    data = self.load(host, token, registry_cfg)
    nested = normalize_graph_engine_config(
        data.get("graph_engine_config")
        if isinstance(data.get("graph_engine_config"), dict)
        else {}
    )
    lh = dict(nested.get("lakehouse") or {})
    lh["warehouse_id"] = wid
    if use_sea is not None:
        lh["use_sea"] = bool(use_sea)
    nested["lakehouse"] = lh
    return self._save(
        host, token, registry_cfg, {"graph_engine_config": nested}
    )
```

Ensure `Optional` is imported in the module (it is used elsewhere).

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/session/test_global_config_service.py -q -k use_sea`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/objects/session/GlobalConfigService.py tests/units/session/test_global_config_service.py
git commit -m "feat(lakehouse): persist + read lakehouse.use_sea in global config"
```

---

### Task 3: `resolve_lakehouse_use_sea(domain, settings)` helper

**Files:**
- Modify: `src/back/core/helpers/DatabricksHelpers.py:262-290` (add method modeled on `resolve_use_cloud_fetch`)
- Check exports: `src/back/core/helpers/__init__.py` (re-export if `resolve_use_cloud_fetch` is)
- Test: `tests/units/core/helpers/test_databricks_helpers.py` (match existing path)

**Interfaces:**
- Consumes: `GlobalConfigService.get_delta_warehouse_use_sea` (Task 2).
- Produces: `DatabricksHelpers.resolve_lakehouse_use_sea(domain, settings) -> bool` and, if the
  package re-exports the sibling resolvers, a module-level `resolve_lakehouse_use_sea`.

- [ ] **Step 1: Write failing test**

```python
def test_resolve_lakehouse_use_sea_reads_global(monkeypatch):
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers
    monkeypatch.setattr(
        DatabricksHelpers, "get_databricks_host_and_token",
        staticmethod(lambda d, s: ("https://ws", "tok")),
    )
    monkeypatch.setattr(
        DatabricksHelpers, "_resolve_registry_cfg",
        staticmethod(lambda d, s: {"catalog": "c", "schema": "s"}),
    )
    from back.objects.session import global_config_service
    monkeypatch.setattr(
        global_config_service, "get_delta_warehouse_use_sea",
        lambda h, t, r: True,
    )
    assert DatabricksHelpers.resolve_lakehouse_use_sea(None, None) is True

def test_resolve_lakehouse_use_sea_default_false_without_registry(monkeypatch):
    from back.core.helpers.DatabricksHelpers import DatabricksHelpers
    monkeypatch.setattr(
        DatabricksHelpers, "get_databricks_host_and_token",
        staticmethod(lambda d, s: ("", "")),
    )
    monkeypatch.setattr(
        DatabricksHelpers, "_resolve_registry_cfg",
        staticmethod(lambda d, s: {}),
    )
    assert DatabricksHelpers.resolve_lakehouse_use_sea(None, None) is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/core/helpers/test_databricks_helpers.py -q -k lakehouse_use_sea`
Expected: FAIL (`AttributeError`).

- [ ] **Step 3: Implement** (note default is **False** here, unlike CloudFetch's `True`)

```python
@staticmethod
def resolve_lakehouse_use_sea(domain, settings) -> bool:
    """Resolve the Lakehouse SEA (Statement Execution API) toggle.

    Defaults to ``False`` (Thrift) when unset or the registry is not
    configured, so classic warehouses keep their current transport.
    """
    from back.objects.session import global_config_service

    host, token = DatabricksHelpers.get_databricks_host_and_token(domain, settings)
    registry_cfg = DatabricksHelpers._resolve_registry_cfg(domain, settings)

    if not host or not registry_cfg.get("catalog") or not registry_cfg.get("schema"):
        return False
    try:
        return bool(
            global_config_service.get_delta_warehouse_use_sea(host, token, registry_cfg)
        )
    except Exception as exc:  # noqa: BLE001 - best-effort default resolution
        logger.debug("Could not resolve Lakehouse use_sea, defaulting to False: %s", exc)
        return False
```

If `resolve_use_cloud_fetch` is re-exported from `back/core/helpers/__init__.py`, add
`resolve_lakehouse_use_sea` alongside it (module-level `resolve_lakehouse_use_sea = DatabricksHelpers.resolve_lakehouse_use_sea`).

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/core/helpers/test_databricks_helpers.py -q -k lakehouse_use_sea`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/helpers/DatabricksHelpers.py src/back/core/helpers/__init__.py tests/units/core/helpers/test_databricks_helpers.py
git commit -m "feat(lakehouse): add resolve_lakehouse_use_sea(domain, settings)"
```

---

### Task 4: Thread `use_sea` through `DatabricksAuth` + `DatabricksClient`

**Files:**
- Modify: `src/back/core/databricks/DatabricksAuth.py:154-195` (`__init__` accepts `use_sea`)
- Modify: `src/back/core/databricks/DatabricksAuth.py:300-312` (`get_sql_connection_params`)
- Modify: `src/back/core/databricks/DatabricksAuth.py:378-386` (probe `probe_params`)
- Modify: `src/back/core/databricks/DatabricksClient.py:32-44` (pass `use_sea` to auth)
- Test: `tests/units/auth/test_auth.py:191-223` (`TestGetSqlConnectionParams`)

**Interfaces:**
- Produces: `DatabricksAuth(..., use_sea: Optional[bool] = None)`, attribute `self.use_sea: bool`.
- Produces: `get_sql_connection_params()` includes `"use_sea": True` only when enabled.
- Produces: `DatabricksClient(..., use_sea: Optional[bool] = None)`.

- [ ] **Step 1: Write failing test**

```python
# in TestGetSqlConnectionParams
@patch.object(DatabricksAuth, "can_use_cloud_fetch", return_value=False)
def test_params_include_use_sea_when_enabled(self, _cf, monkeypatch):
    _clear_databricks_env(monkeypatch)
    auth = DatabricksAuth(
        host="https://ws.databricks.com", token="pat",
        warehouse_id="wh-rt", use_sea=True,
    )
    params = auth.get_sql_connection_params()
    assert params["use_sea"] is True

@patch.object(DatabricksAuth, "can_use_cloud_fetch", return_value=False)
def test_params_omit_use_sea_by_default(self, _cf, monkeypatch):
    _clear_databricks_env(monkeypatch)
    auth = DatabricksAuth(
        host="https://ws.databricks.com", token="pat", warehouse_id="wh",
    )
    params = auth.get_sql_connection_params()
    assert "use_sea" not in params  # Thrift default
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/auth/test_auth.py::TestGetSqlConnectionParams -q`
Expected: FAIL (`use_sea` unexpected kwarg / assertion error).

- [ ] **Step 3: Implement**

```python
# __init__ signature (:154-159) — add param
def __init__(
    self,
    host: Optional[str] = None,
    token: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    use_cloud_fetch: Optional[bool] = None,
    use_sea: Optional[bool] = None,
) -> None:
    ...
    self.use_sea = bool(use_sea)  # add near the use_cloud_fetch resolution (~:190-195)
```

```python
# get_sql_connection_params (:305 area) — add after use_cloud_fetch line
params["use_cloud_fetch"] = self.can_use_cloud_fetch()
if self.use_sea:
    params["use_sea"] = True
```

```python
# probe_cloud_fetch_capability probe_params (:379-386) — mirror the flag
probe_params = {
    "server_hostname": ...,
    "http_path": ...,
    "_socket_timeout": _CLOUD_FETCH_PROBE_TIMEOUT_SECONDS,
    "use_cloud_fetch": True,
}
if self.use_sea:
    probe_params["use_sea"] = True
```

```python
# DatabricksClient.__init__ (:32-44)
def __init__(
    self,
    host: Optional[str] = None,
    token: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    use_cloud_fetch: Optional[bool] = None,
    use_sea: Optional[bool] = None,
) -> None:
    self.auth = DatabricksAuth(
        host=host, token=token, warehouse_id=warehouse_id,
        use_cloud_fetch=use_cloud_fetch, use_sea=use_sea,
    )
    ...
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/auth/test_auth.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/databricks/DatabricksAuth.py src/back/core/databricks/DatabricksClient.py tests/units/auth/test_auth.py
git commit -m "feat(databricks): thread use_sea into connection params + probe"
```

---

### Task 5: Lakehouse client passes `use_sea`

**Files:**
- Modify: `src/back/core/graphdb/delta/DeltaBase.py:13-41` (`create_databricks_client`)
- Test: `tests/units/core/graphdb/delta/test_delta_base.py` (match existing path; create if absent)

**Interfaces:**
- Consumes: `DatabricksHelpers.resolve_lakehouse_use_sea` (Task 3), `DatabricksClient(use_sea=...)` (Task 4).

- [ ] **Step 1: Write failing test**

```python
def test_create_databricks_client_passes_use_sea(monkeypatch):
    import back.core.graphdb.delta.DeltaBase as db
    captured = {}

    class FakeClient:
        def __init__(self, **kw):
            captured.update(kw)

    monkeypatch.setattr(db, "get_databricks_host_and_token", lambda d, s: ("https://ws", "tok"))
    monkeypatch.setattr(db, "resolve_delta_warehouse_id", lambda d, s: "wh-rt")
    monkeypatch.setattr(
        "back.core.helpers.DatabricksHelpers.DatabricksHelpers.resolve_lakehouse_use_sea",
        staticmethod(lambda d, s: True),
    )
    monkeypatch.setattr("back.core.databricks.DatabricksClient", FakeClient)

    client = db.create_databricks_client(domain=object(), settings=object())
    assert captured.get("use_sea") is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/core/graphdb/delta/test_delta_base.py -q -k use_sea`
Expected: FAIL (`use_sea` not in captured kwargs).

- [ ] **Step 3: Implement**

```python
# DeltaBase.create_databricks_client — settings branch
from back.core.helpers import resolve_delta_warehouse_id
from back.core.helpers.DatabricksHelpers import DatabricksHelpers

if settings is not None:
    host, token = get_databricks_host_and_token(domain, settings)
    warehouse_id = resolve_delta_warehouse_id(domain, settings)
    use_sea = DatabricksHelpers.resolve_lakehouse_use_sea(domain, settings)
else:
    db = getattr(domain, "databricks", None) or {}
    host = db.get("host", "")
    token = db.get("token", "")
    warehouse_id = db.get("warehouse_id", "") or db.get("sql_warehouse_id", "")
    use_sea = bool(db.get("use_sea", False))
...
return DatabricksClient(
    host=host, token=token, warehouse_id=warehouse_id, use_sea=use_sea
)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/core/graphdb/delta/test_delta_base.py -q -k use_sea`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/graphdb/delta/DeltaBase.py tests/units/core/graphdb/delta/test_delta_base.py
git commit -m "feat(lakehouse): Delta client honours lakehouse.use_sea"
```

---

### Task 6: Route + service accept/return `use_sea`

**Files:**
- Modify: `src/api/routers/internal/settings.py:129-146` (`select_delta_warehouse` reads `use_sea`)
- Modify: `src/back/objects/domain/SettingsService.py:354-399` (`select_delta_warehouse` param + write)
- Modify: `src/back/objects/domain/SettingsService.py:1660-1693` (`get_delta_warehouse_result` returns `use_sea`)
- Test: `tests/integration/settings/test_delta_warehouse.py` (match existing path; create if absent)

**Interfaces:**
- Consumes: `GlobalConfigService.set_delta_warehouse_id(..., use_sea=...)` (Task 2),
  `get_delta_warehouse_use_sea` (Task 2).
- Produces: `POST /settings/select-delta-warehouse` accepts `{warehouse_id, use_sea}`;
  `GET /settings/delta-warehouse` returns `use_sea`.

- [ ] **Step 1: Write failing test** (mock the global store; assert the round-trip through the service)

```python
def test_select_delta_warehouse_persists_use_sea(...):
    result = SettingsService.select_delta_warehouse(
        "wh-rt", email="a@b.c", user_token="t", use_sea=True,
        session_mgr=session_mgr, settings=settings,
    )
    assert result["use_sea"] is True
    assert SettingsService.get_delta_warehouse_result(session_mgr, settings)["use_sea"] is True
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/integration/settings/test_delta_warehouse.py -q -k use_sea`
Expected: FAIL (`select_delta_warehouse` has no `use_sea`).

- [ ] **Step 3: Implement**

```python
# router (:140-146)
return config_service.select_delta_warehouse(
    data.get("warehouse_id"),
    email,
    user_token,
    session_mgr,
    settings,
    use_sea=bool(data.get("use_sea", False)),
)
```

```python
# SettingsService.select_delta_warehouse — add keyword-only param + pass to setter
@staticmethod
def select_delta_warehouse(
    warehouse_id: Optional[str],
    email: str,
    user_token: str,
    session_mgr: SessionManager,
    settings: Settings,
    *,
    use_sea: bool = False,
) -> Dict[str, Any]:
    ...
    ok, msg = global_config_service.set_delta_warehouse_id(
        host, token, registry_cfg, wid, use_sea=use_sea,
    )
    ...
    return {
        "success": True,
        "message": (...),
        "delta_warehouse_id": wid,
        "use_sea": bool(use_sea),
        "effective_delta_warehouse_id": resolve_delta_warehouse_id(domain, settings),
    }
```

```python
# get_delta_warehouse_result — add use_sea to the returned dict
delta_use_sea = global_config_service.get_delta_warehouse_use_sea(
    host, token, registry_cfg
)
return {
    "success": True,
    "delta_warehouse_id": delta_wid,
    "use_sea": delta_use_sea,
    "effective_delta_warehouse_id": resolve_delta_warehouse_id(domain, settings),
    "registry_catalog": catalog,
    "registry_schema": schema,
    "storage_location": storage_location,
    "registry_configured": bool(storage_location),
}
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/integration/settings/test_delta_warehouse.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/api/routers/internal/settings.py src/back/objects/domain/SettingsService.py tests/integration/settings/test_delta_warehouse.py
git commit -m "feat(settings): select-delta-warehouse accepts + returns use_sea"
```

---

### Task 7: UI checkbox on the SQL Warehouse tab

**Files:**
- Modify: `src/front/templates/settings.html:162-194` (SQL Warehouse tab — add checkbox under the select)
- Modify: `src/front/static/config/js/settings.js:245-390` (load into checkbox; send `use_sea` on Apply)
- Reference: `.cursor/11-frontend-design.mdc` (tokens, `form-check` pattern used at
  `settings.html:780` Neo4j encrypted)

**Interfaces:**
- Consumes: `GET /settings/delta-warehouse` `use_sea` (Task 6), `POST /settings/select-delta-warehouse`
  body `use_sea` (Task 6).

- [ ] **Step 1: Add markup** (Bootstrap `form-check`, no inline CSS/JS)

```html
<!-- settings.html, inside #dtpane-warehouse, after #deltaWarehouseHelp (~:193) -->
<div class="form-check mt-3">
  <input class="form-check-input" type="checkbox" id="deltaUseSea">
  <label class="form-check-label small" for="deltaUseSea">
    Lakehouse RT warehouse
  </label>
  <div class="form-text small">
    Required for serverless <strong>Lakehouse/RT</strong> warehouses. Connects via the
    Statement Execution API instead of Thrift. Leave unchecked for classic SQL warehouses.
  </div>
</div>
```

- [ ] **Step 2: Load the saved value** — in the delta-warehouse preload (`settings.js` ~:245-275,
  wherever `/settings/delta-warehouse` is fetched), set the checkbox:

```javascript
const seaEl = document.getElementById('deltaUseSea');
if (seaEl) seaEl.checked = !!data.use_sea;
```

- [ ] **Step 3: Send it on Apply** — in `saveDeltaWarehouseSelection` (`settings.js:345-369`):

```javascript
const seaEl = document.getElementById('deltaUseSea');
const body = JSON.stringify({
    warehouse_id: warehouseId,
    use_sea: !!(seaEl && seaEl.checked),
});
// ... fetch('/settings/select-delta-warehouse', { ..., body })
```

- [ ] **Step 4: Browser-test** (frontend-design skill step 5)
  - Desktop + mobile widths, keyboard focus on the checkbox, no console/network errors.
  - Check → Apply → reload: checkbox stays checked; `GET /settings/delta-warehouse` returns `use_sea: true`.
  - Trigger a Lakehouse KG **Build** against the RT warehouse and confirm the VIEW is created.

- [ ] **Step 5: Commit**

```bash
git add src/front/templates/settings.html src/front/static/config/js/settings.js
git commit -m "feat(ui): Lakehouse RT checkbox on Settings -> Lakehouse -> SQL Warehouse"
```

---

### Task 8: Clearer error when RT hits Thrift

**Files:**
- Modify: `src/back/core/databricks/SQLWarehouse.py:229-248` (`_create_or_replace` error branch)
- Reference (message origin): `src/back/objects/digitaltwin/_build_pipeline.py:547-609`,
  `src/back/core/graphdb/delta/DeltaTripleStoreBuildPipeline.py:208-235`
- Test: `tests/units/core/test_sql_warehouse.py` (add a case)

**Interfaces:** none new — improves the `(ok, message)` string only.

- [ ] **Step 1: Write failing test**

```python
def test_create_view_maps_rt_thrift_error(monkeypatch):
    auth = DatabricksAuth(host="https://h", token="t", warehouse_id="wh")
    sw = SQLWarehouse(auth)
    monkeypatch.setattr(
        sw, "execute_statement",
        lambda stmt: (_ for _ in ()).throw(
            Exception("BAD_REQUEST: Lakehouse/RT is not supported for Thrift protocol")
        ),
    )
    ok, msg = sw.create_or_replace_view("c", "s", "v", "SELECT 1")
    assert ok is False
    assert "Lakehouse RT" in msg  # actionable hint, not "check source tables"
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run --frozen pytest tests/units/core/test_sql_warehouse.py -q -k rt_thrift`
Expected: FAIL.

- [ ] **Step 3: Implement** — in `_create_or_replace` `except` branch:

```python
except Exception as exc:
    logger.exception("ERROR creating %s: %s", kind.lower(), exc)
    detail = str(exc)
    if "not supported for Thrift protocol" in detail:
        return False, (
            f"Failed to create {kind.lower()}: this looks like a Lakehouse/RT "
            "(serverless real-time) warehouse. Enable 'Lakehouse RT warehouse' in "
            "Settings -> Lakehouse -> SQL Warehouse, then rebuild."
        )
    return False, f"Failed to create {kind.lower()}: {detail}"
```

Leave the generic "Check source tables, column mappings and warehouse permissions" wrapping in
the build pipeline for the non-RT case.

- [ ] **Step 4: Run to verify pass**

Run: `uv run --frozen pytest tests/units/core/test_sql_warehouse.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/back/core/databricks/SQLWarehouse.py tests/units/core/test_sql_warehouse.py
git commit -m "feat(lakehouse): actionable error when RT warehouse rejects Thrift DDL"
```

---

### Task 9: Changelog + full suite

**Files:**
- Create/append: `/changelogs/v0.8.0/<github-user>_2026-09-04.log`
- Docs: check `docs/` Settings/Lakehouse page + `README.md` for a one-line note on the toggle.

- [ ] **Step 1: Write the changelog** (English; title, context, numbered changes with paths,
  modified files, test result) via the `changelog` skill.
- [ ] **Step 2: Run the suite**

Run: `uv run --frozen pytest -q -m "not scenario"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add changelogs/ docs/ README.md
git commit -m "docs(lakehouse): changelog + Lakehouse RT toggle notes"
```

---

## Self-Review

- **Spec coverage:** config normalize/resolve (T1), persistence (T2), domain resolver (T3),
  auth/client transport (T4), Lakehouse client wiring (T5), API+service (T6), UI checkbox (T7),
  error copy (T8), changelog/docs/tests (T9). All of option B covered.
- **Default off:** `_finalize_lakehouse_bucket` + both resolvers default `False`; `get_sql_connection_params`
  omits `use_sea` unless enabled — existing installs unchanged. Consistent across tasks.
- **Type consistency:** `resolve_lakehouse_use_sea(cfg)` (engine_config, cfg arg) vs
  `DatabricksHelpers.resolve_lakehouse_use_sea(domain, settings)` — intentionally distinct
  (different layers); the Delta client calls the `(domain, settings)` one. `set_delta_warehouse_id`
  keeps its positional `warehouse_id`, adds keyword `use_sea`.
- **Open decision:** default is opt-in (**off**). If you prefer RT-just-works, flip the two resolver
  defaults to `True` — do it before T1/T3 land so tests encode the intended default.

## Notes / risks

- Verify exact existing test paths (`tests/units/...`) before creating new files; align with the
  suite's conventions and fixtures rather than the illustrative names above.
- If `back/core/helpers/__init__.py` does not re-export `resolve_use_cloud_fetch`, skip the
  module-level re-export in T3 and call the static method directly (as T5 does).
- Checking the box on a classic warehouse is expected to be harmless (SEA is valid there); the
  copy tells users to leave it off for classic to avoid confusion.
