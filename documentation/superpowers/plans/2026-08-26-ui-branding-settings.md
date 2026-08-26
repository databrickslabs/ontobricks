# Configurable UI Branding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let administrators configure the application title, primary brand
color, and global brand icon from Settings → Configuration → UI, with
first-paint rendering and contrast-safe derived colors.

**Architecture:** Store one versioned `ui_branding` object in
`GlobalConfigService`, expose it through one atomic multipart Settings API, and
resolve it into a normalized `UIBranding` value object. A lightweight HTML
request middleware places the cached branding on `request.state`; Jinja emits
title, icon, and CSS variables before first paint. The Settings UI keeps a
saved baseline, applies previews locally, and saves/discards atomically.

**Tech Stack:** Python 3.11, FastAPI, Pydantic/dataclasses, Jinja2, vanilla
JavaScript, Bootstrap 5, CSS custom properties, pytest, Playwright.

## Global Constraints

- Default title: `OntoBricks`.
- Default primary: `#4F46E5`.
- Default icon: `/static/global/img/favicon.svg`.
- Light/hover/focus/dark colors are derived; users configure one color only.
- Title is 1–60 trimmed Unicode characters.
- Color input is normalized uppercase `#RRGGBB`.
- Logo formats remain SVG/PNG/JPEG/WebP/GIF, maximum 1 MB.
- Save is atomic; validation failure writes nothing.
- Only administrators access the Settings API; all users see branding.
- Semantic status colors and `--ovz-*` remain fixed.
- Do not create commits during implementation unless the user asks again.

---

### Task 1: Branding value object and palette derivation

**Files:**
- Create: `src/back/core/helpers/UIBranding.py`
- Modify: `src/back/core/helpers/__init__.py`
- Create: `tests/units/ui/test_branding.py`

**Interfaces:**
- Produces:
  - `DEFAULT_APP_TITLE: str`
  - `DEFAULT_PRIMARY_COLOR: str`
  - `DEFAULT_LOGO_PATH: str`
  - `derive_brand_palette(primary_color: str) -> BrandPalette`
  - `normalize_ui_branding(raw: Mapping[str, Any]) -> UIBranding`
  - immutable `BrandPalette` and `UIBranding` dataclasses with `to_dict()`
- Consumes: no repository services; this module is deterministic and isolated.

- [x] **Step 1: Write failing unit tests**

Test defaults, title trimming/length, uppercase hex normalization, invalid
colors, deterministic dark/alpha values, foreground contrast ≥ 4.5, custom and
default logo resolution, and serialization:

```python
def test_default_branding_is_ontobricks_indigo():
    branding = normalize_ui_branding({})
    assert branding.app_title == "OntoBricks"
    assert branding.primary_color == "#4F46E5"
    assert branding.logo_url == "/static/global/img/favicon.svg"
    assert branding.palette.primary_rgb == "79, 70, 229"


@pytest.mark.parametrize("color", ["red", "#fff", "#GG46E5", ""])
def test_invalid_primary_color_is_rejected(color):
    with pytest.raises(ValueError, match="primary color"):
        normalize_ui_branding({"primary_color": color})


def test_on_primary_always_meets_wcag_contrast():
    for color in ("#111111", "#777777", "#F5E642", "#4F46E5"):
        palette = derive_brand_palette(color)
        assert contrast_ratio(color, palette.on_primary) >= 4.5
```

- [x] **Step 2: Verify RED**

Run:

```shell
uv run --frozen pytest tests/units/ui/test_branding.py -q
```

Expected: collection/import failure because `back.core.helpers.UIBranding` does not
exist.

- [x] **Step 3: Implement the value object**

Use immutable dataclasses. Normalize title/color in `normalize_ui_branding`;
allow `logo_data_url=""`; derive `logo_url` from data URL or default path.
Implement RGB parsing, black mixing with round-half-up channel rounding, alpha
strings, WCAG relative luminance, white/dark foreground selection, and selected
text darkening:

```python
@dataclass(frozen=True)
class BrandPalette:
    primary_rgb: str
    primary_dark: str
    primary_darker: str
    primary_light: str
    hover: str
    focus: str
    on_primary: str
    selected_text: str


@dataclass(frozen=True)
class UIBranding:
    version: int
    app_title: str
    primary_color: str
    logo_data_url: str
    logo_url: str
    is_custom_logo: bool
    palette: BrandPalette
```

- [x] **Step 4: Verify GREEN**

Run the Task 1 test command. Expected: all tests pass.

---

### Task 2: Persist unified branding and preserve legacy logo behavior

**Files:**
- Modify: `src/back/objects/session/GlobalConfigService.py`
- Modify: `src/back/objects/domain/SettingsService.py`
- Test: `tests/units/settings/test_ui_branding.py`

**Interfaces:**
- Consumes: `normalize_ui_branding`, existing `_load`, `_save`, Registry config,
  navbar-logo MIME/size constants.
- Produces:
  - `GlobalConfigService.get_ui_branding(...) -> dict`
  - `GlobalConfigService.set_ui_branding(..., branding: dict) -> None`
  - `SettingsService.get_ui_branding_result(...) -> dict`
  - `SettingsService.save_ui_branding_result(..., app_title, primary_color,
    logo_content, logo_mime, reset_logo) -> dict`

- [x] **Step 1: Write failing persistence/service tests**

Cover defaults, legacy `navbar_logo` fallback, one `_save` call for a complete
branding update, failed save leaving cache unchanged, compatibility setters
updating `ui_branding.logo_data_url`, title/color validation, upload/reset
conflict, and logo validation:

```python
def test_legacy_navbar_logo_is_resolved_into_unified_branding(service):
    service._cache[key] = {"navbar_logo": "data:image/png;base64,abc"}
    branding = service.get_ui_branding(...)
    assert branding["logo_data_url"] == "data:image/png;base64,abc"


def test_save_ui_branding_is_atomic(settings_service, global_config):
    result = settings_service.save_ui_branding_result(
        ..., app_title="Acme Graph", primary_color="#123456",
        logo_content=None, logo_mime=None, reset_logo=False,
    )
    assert result["success"] is True
    global_config.set_ui_branding.assert_called_once()
```

- [x] **Step 2: Verify RED**

Run:

```shell
uv run --frozen pytest tests/units/settings/test_ui_branding.py -q
```

Expected: failures for missing service methods/default key.

- [x] **Step 3: Add storage and service methods**

Add to `_empty()`:

```python
"ui_branding": {
    "version": 1,
    "app_title": DEFAULT_APP_TITLE,
    "primary_color": DEFAULT_PRIMARY_COLOR,
    "logo_data_url": "",
},
```

Resolve legacy `navbar_logo` only when unified logo is empty. Normalize before
save and update cache only after store persistence succeeds. Refactor existing
navbar-logo get/set wrappers to delegate to unified branding.

In `SettingsService`, validate all fields before one write. Reuse the existing
logo allow-list and 1 MB cap. Return normalized `branding.to_dict()`.

- [x] **Step 4: Verify GREEN and existing logo tests**

Run:

```shell
uv run --frozen pytest tests/units/settings/test_ui_branding.py \
  tests/units/settings/test_config.py -q
```

Expected: pass.

---

### Task 3: Add atomic admin API

**Files:**
- Modify: `src/api/routers/internal/settings.py`
- Test: `tests/units/api/test_ui_branding_settings.py`
- Test: `tests/units/settings/test_settings_domain_permissions.py`

**Interfaces:**
- Consumes: Task 2 service methods and `_settings_request_identity`.
- Produces:
  - `GET /settings/ui-branding`
  - `POST /settings/ui-branding` multipart endpoint

- [x] **Step 1: Write failing API and permission tests**

Test GET defaults, POST text/color, POST logo, reset, invalid hex, invalid
title, reset/upload conflict, persistence error, admin enforcement, and one
service call:

```python
def test_save_ui_branding_accepts_atomic_multipart(client):
    response = client.post(
        "/settings/ui-branding",
        data={
            "app_title": "Acme Graph",
            "primary_color": "#123456",
            "reset_logo": "false",
        },
    )
    assert response.status_code == 200
    assert response.json()["branding"]["app_title"] == "Acme Graph"
```

- [x] **Step 2: Verify RED**

Run:

```shell
uv run --frozen pytest tests/units/api/test_ui_branding_settings.py -q
```

Expected: 404 for both routes.

- [x] **Step 3: Implement GET and POST**

Use `Form(...)` and optional `UploadFile`. Read the upload once, pass bytes and
MIME to the service, and map service validation failures to the repository's
existing structured status conventions. Require admin through the existing
middleware/service guard; do not add a public Settings exception.

- [x] **Step 4: Verify GREEN**

Run the Task 3 test command plus permission tests. Expected: pass.

---

### Task 4: Render branding before first paint

**Files:**
- Create: `src/shared/fastapi/ui_branding.py`
- Modify: `src/shared/fastapi/main.py`
- Modify: `src/front/fastapi/dependencies.py`
- Modify: `src/front/templates/base.html`
- Modify: top-level templates that hardcode `OntoBricks` in title blocks
- Modify: `src/front/templates/home.html`
- Modify: `src/front/templates/partials/layout/help_modal.html`
- Modify: `src/front/templates/partials/dtwin/_query_chat.html`
- Modify: `src/front/templates/partials/ontology/_ontology_map.html`
- Modify: `src/back/fastapi/graphql_routes.py`
- Test: `tests/units/front/test_ui_branding_rendering.py`
- Test: `tests/units/api/test_ui_rendering.py`

**Interfaces:**
- Consumes: normalized Task 1 branding and cached Task 2 storage.
- Produces:
  - request-state `ui_branding`;
  - Jinja global `ui_branding_for(request)`;
  - Jinja helper `brand_page_title(page_title, request)`.

- [x] **Step 1: Write failing rendering tests**

Assert configured title in navbar/alt/page title, derived CSS variables in the
head after static CSS, custom icon in every listed consumer, default fallback
on resolver failure, and page-specific title composition:

```python
def test_configured_branding_is_in_first_html_response(client, branding_store):
    branding_store.set(title="Acme Graph", color="#123456", logo=DATA_URL)
    html = client.get("/").text
    assert ">Acme Graph<" in html
    assert "<title>Acme Graph - Home</title>" in html
    assert "--db-primary: #123456" in html
    assert DATA_URL in html
```

- [x] **Step 2: Verify RED**

Run:

```shell
uv run --frozen pytest tests/units/front/test_ui_branding_rendering.py -q
```

Expected: configured values absent from server HTML.

- [x] **Step 3: Implement middleware/context helpers**

For HTML GET requests, resolve cached branding and set
`request.state.ui_branding`; on any exception log once and set defaults.
Register middleware in an order where authentication/registry request context
already exists. The context processor must remain synchronous and only read
request state.

Emit escaped CSS variables:

```html
<style id="obBrandTheme">
  :root {
    --db-primary: {{ branding.primary_color }};
    --db-primary-rgb: {{ branding.palette.primary_rgb }};
    --db-primary-dark: {{ branding.palette.primary_dark }};
    --db-primary-darker: {{ branding.palette.primary_darker }};
    --db-primary-light: {{ branding.palette.primary_light }};
    --db-hover-indigo: {{ branding.palette.hover }};
    --db-focus-ring: 0 0 0 0.2rem {{ branding.palette.focus }};
    --db-shadow-primary: 0 8px 20px {{ branding.palette.focus }};
    --db-on-primary: {{ branding.palette.on_primary }};
    --db-primary-selected-text: {{ branding.palette.selected_text }};
  }
</style>
```

Replace literal title suffixes with the title helper and icon paths with
`branding.logo_url`.

- [x] **Step 4: Verify GREEN**

Run Task 4 tests and existing UI rendering tests. Expected: pass.

---

### Task 5: Centralize all primary-color consumers

**Files:**
- Modify: `src/front/static/global/css/main.css`
- Modify: `src/front/static/global/css/components.css`
- Modify: `src/front/static/global/css/sidebar-layout.css`
- Modify: `src/front/static/global/ontoviz/css/ontoviz.css`
- Modify: `src/front/static/ontology/js/ontology-map.js`
- Modify: `src/front/static/global/js/navbar.js`
- Test: `tests/units/front/test_clarity_design_contract.py`
- Test: `tests/units/front/test_ui_branding_rendering.py`

**Interfaces:**
- Consumes: CSS variables emitted by Task 4.
- Produces: no hardcoded active indigo outside the fallback `:root` palette.

- [x] **Step 1: Write failing contract tests**

Scan approved consumer files after stripping the `:root` fallback block and
assert no raw `#4F46E5` or `rgba(79, 70, 229, ...)` remains. Assert primary
buttons use `--db-on-primary`, active text uses
`--db-primary-selected-text`, and JS obtains selection color from CSS:

```python
def test_primary_consumers_do_not_bypass_runtime_tokens():
    for path in PRIMARY_CONSUMERS:
        text = strip_default_palette(_read(path))
        assert "#4F46E5" not in text.upper()
        assert "79, 70, 229" not in text
```

- [x] **Step 2: Verify RED**

Run the targeted contract test. Expected: failures naming current literal
consumers.

- [x] **Step 3: Replace literals with tokens**

Keep static defaults in `main.css :root`, add `--db-on-primary` and
`--db-primary-selected-text`, and replace consumer literals. In
`ontology-map.js`, read:

```javascript
getComputedStyle(document.documentElement)
    .getPropertyValue('--db-primary')
    .trim() || '#4F46E5'
```

Update `navbar.js` to apply normalized branding returned by navbar state only
as a compatibility/live-refresh path; server-rendered state remains primary.

- [x] **Step 4: Verify GREEN**

Run frontend design and rendering contracts. Expected: pass.

---

### Task 6: Build Settings → Configuration → UI

**Files:**
- Modify: `src/front/config/menu_config.json`
- Modify: `src/front/templates/settings.html`
- Modify: `src/front/static/config/js/settings.js`
- Modify: `src/front/static/config/css/config.css`
- Test: `tests/units/front/test_ui_branding_settings_contract.py`

**Interfaces:**
- Consumes: Task 3 GET/POST response and Task 4 CSS variables/brand DOM hooks.
- Produces:
  - `loadUIBranding()`
  - `previewUIBranding(draft)`
  - `saveUIBranding()`
  - `discardUIBrandingChanges()`
  - `resetUIBrandingDefaults()`

- [x] **Step 1: Write failing template/JS contract tests**

Assert menu location, `#ui-section`, Branding/Theme cards, moved logo/default
entity icon controls, title/color inputs, swatches, Save/Discard/Defaults,
multipart endpoint usage, `textContent` title updates, CSS variable updates,
favicon/brand image preview, baseline restore, and no duplicate DOM ids.

- [x] **Step 2: Verify RED**

Run:

```shell
uv run --frozen pytest tests/units/front/test_ui_branding_settings_contract.py -q
```

Expected: missing `ui` menu/section and functions.

- [x] **Step 3: Add menu and section markup**

Add under `settings-config`:

```json
{"id": "ui", "label": "UI", "icon": "bi-palette"}
```

Move existing Default Entity Icon and Application Logo blocks from Global.
Add accessible title/color fields, derived swatches, component preview, status
region, and actions. Follow `.section-header` and `.btn-sm` contracts.

- [x] **Step 4: Implement preview/save/discard**

Keep `savedUIBranding` and `draftUIBranding` objects. Synchronize native color
and hex inputs. Preview through `document.documentElement.style.setProperty`,
DOM `textContent`, favicon/brand image sources, and FileReader data URLs.
Create one `FormData` on Save. Discard removes/reapplies inline overrides from
the saved baseline and revokes object URLs.

Initialize on `ui` section activation and once during Settings page startup if
UI is the requested section.

- [x] **Step 5: Verify GREEN**

Run Task 6 contract tests and all frontend unit tests. Expected: pass.

---

### Task 7: Documentation, changelog, browser verification, and full suite

**Files:**
- Modify: `.cursor/11-frontend-design.mdc`
- Modify: `documentation/user-guide.md`
- Modify: `changelogs/v0.8.0/benoitcayladbx_2026-08-26.log`

**Interfaces:**
- Consumes: completed Tasks 1–6.
- Produces: documented runtime-theme contract and verification evidence.

- [x] **Step 1: Update documentation**

Document:

- `#4F46E5` as fallback/default, not immutable runtime color;
- one configurable primary family, fixed semantic colors;
- Settings → Configuration → UI workflow;
- preview, discard, save, and reset behavior;
- icon/title scope.

- [x] **Step 2: Run targeted tests**

```shell
uv run --frozen pytest \
  tests/units/ui/test_branding.py \
  tests/units/settings/test_ui_branding.py \
  tests/units/api/test_ui_branding_settings.py \
  tests/units/front/test_ui_branding_rendering.py \
  tests/units/front/test_ui_branding_settings_contract.py \
  tests/units/front/test_clarity_design_contract.py -q
```

Expected: all pass.

- [x] **Step 3: Run browser verification**

Use the web dev-loop tester on desktop 1600×1000 and mobile 390×844:

1. Open Settings → UI.
2. Preview title `Acme Graph`, a dark color, a light color, and a custom icon.
3. Verify navbar, favicon, swatches, button foreground, selected tabs, focus
   ring, Help, Home, Chat, and Assistant.
4. Discard and verify exact restoration.
5. Save and verify persistence in a clean browser context without default-theme
   flash.
6. Reset defaults and save.
7. Confirm console/network contain no errors.

- [x] **Step 4: Run mandatory full suite**

```shell
uv run --frozen pytest -q -m "not scenario"
```

Expected: zero failures.

- [x] **Step 5: Update changelog**

Append an English section containing context, numbered changes with paths, all
modified files, targeted tests, full-suite summary, and browser measurements.

- [x] **Step 6: Check diagnostics and diff**

Run IDE diagnostics for changed files and:

```shell
git diff --check
git status --short
```

Expected: no introduced lint or whitespace errors. Do not commit unless the
user explicitly requests another commit.
