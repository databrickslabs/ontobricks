# Configurable UI Branding — Design Specification

## Purpose

Administrators can already replace the navbar application icon from Settings,
but the application name and Clarity indigo palette remain hardcoded. This
feature groups all visual identity controls in a dedicated **UI** section under
the existing **Configuration** Settings group and makes the branding consistent
across every application surface.

The configurable values are:

- application title, default `OntoBricks`;
- primary color, default `#4F46E5`;
- application icon, default `/static/global/img/favicon.svg`.

The light/secondary brand color and all interaction-state colors are derived
from the primary color. They are not independently configurable.

## Decisions

- Branding is stored as one atomic global configuration object.
- The title applies to the visible navbar brand and every browser page title.
- The icon applies everywhere the application brand is shown, including the
  navbar, favicon, home page, Help, Knowledge Graph Chat, and Ontology
  Assistant.
- Settings provides immediate, unsaved preview plus explicit **Save** and
  **Discard changes** actions.
- UI branding is rendered before first paint; a configured application must not
  flash the default title, icon, or indigo palette.
- All users see the configured branding. Only administrators can read or write
  the Settings UI and its API.

## Settings Information Architecture

The existing Settings sidebar group **Configuration** contains:

1. Databricks
2. Registry
3. Global
4. UI

The new `ui` menu item uses `bi-palette`, maps to `#ui-section`, and follows the
existing `menu_config.json` → `_sidebar_nav.html` → `SidebarNav` contract.

Settings currently places "Default Entity Icon" and "Application Logo" inside
Global. Both move to UI because they change presentation rather than runtime
infrastructure. Non-visual settings remain in Global.

## UI Section

### Branding card

- **Application title** text input.
- Current icon preview.
- File input accepting SVG, PNG, JPEG, WebP, or GIF.
- **Reset icon** action, which previews the bundled default.
- Guidance: recommended square image, 64×64 px; maximum size 1 MB.

### Theme card

- Native color input for the primary color.
- Synchronized hexadecimal text input.
- Read-only swatches for:
  - primary;
  - derived hover;
  - derived selected/light background;
  - focus ring;
  - button foreground.
- A small component preview showing a primary button, active navigation item,
  hover well, and focused control.

### Actions and dirty state

- Field changes update the current page, navbar, favicon, and preview
  immediately without writing to the server.
- **Save** persists title, color, and icon in one transaction.
- **Discard changes** restores the last successfully loaded/saved branding and
  removes the preview.
- **Reset to defaults** previews `OntoBricks`, `#4F46E5`, and the bundled icon;
  it does not persist until Save.
- Save is disabled while the form is pristine or invalid and shows a spinner
  during the request.
- Navigation away with unsaved changes uses the same guarded-section pattern as
  other Settings forms; the administrator must discard or remain on the page.

## Data Model and Persistence

`GlobalConfigService` gains a versioned `ui_branding` object:

```json
{
  "version": 1,
  "app_title": "OntoBricks",
  "primary_color": "#4F46E5",
  "logo_data_url": ""
}
```

An empty `logo_data_url` means the bundled default icon. `ui_branding` is stored
through the existing Registry Store abstraction, so Volume and Lakebase
backends keep identical behavior.

### Backward compatibility

The existing top-level `navbar_logo` key remains readable:

1. if `ui_branding.logo_data_url` is present, use it;
2. otherwise use legacy `navbar_logo`;
3. otherwise use the bundled icon.

The first successful unified save writes the resolved logo into `ui_branding`.
Existing `/settings/navbar-logo` endpoints remain as compatibility wrappers
over the unified service until a separate removal is planned. They must update
the same `ui_branding.logo_data_url`, preventing two sources of truth.

Unknown fields in future branding versions are ignored on read and preserved
when feasible on write.

## API

### `GET /settings/ui-branding`

Admin-only. Returns the persisted values plus resolved defaults and the derived
palette:

```json
{
  "success": true,
  "branding": {
    "app_title": "OntoBricks",
    "primary_color": "#4F46E5",
    "logo_url": "/static/global/img/favicon.svg",
    "is_custom_logo": false,
    "palette": {
      "primary_rgb": "79, 70, 229",
      "primary_dark": "#4338CA",
      "primary_darker": "#3730A3",
      "primary_light": "rgba(79, 70, 229, 0.10)",
      "hover": "rgba(79, 70, 229, 0.06)",
      "focus": "rgba(79, 70, 229, 0.18)",
      "on_primary": "#FFFFFF"
    }
  }
}
```

### `POST /settings/ui-branding`

Admin-only multipart request:

- `app_title`: string;
- `primary_color`: `#RRGGBB`;
- optional `logo_file`;
- `reset_logo`: boolean.

The server validates every field and derives the complete palette before
performing one `GlobalConfigService` write. A failure writes nothing. The
response returns the normalized branding object used by GET.

Supplying both `logo_file` and `reset_logo=true` is invalid.

## Validation and Palette Derivation

### Title

- Trim surrounding whitespace.
- Required.
- 1–60 Unicode characters after trimming.
- Render only through Jinja escaping or DOM `textContent`; never via untrusted
  `innerHTML`.

### Icon

- Non-empty file.
- Maximum 1 MB.
- MIME allow-list: `image/svg+xml`, `image/png`, `image/jpeg`, `image/webp`,
  `image/gif`.
- Existing upload validation remains the single implementation used by both the
  unified and compatibility endpoints.

### Color

- Normalize to uppercase `#RRGGBB`.
- Reject any other syntax; alpha is not accepted.
- Convert once to an RGB tuple.
- Derive dark variants by deterministic mixing with black:
  - dark: 15% black;
  - darker: 30% black.
- Derive translucent variants from the base RGB:
  - hover: alpha 0.06;
  - selected/light: alpha 0.10;
  - focus ring and primary shadow: alpha 0.18.
- Choose `on_primary` from `#FFFFFF` and `#111827`, selecting the option with
  the higher WCAG contrast ratio against the primary color. The result must
  meet 4.5:1.
- Selected-state foreground starts from the primary color and is progressively
  darkened until it reaches 4.5:1 against the light color composited over the
  warm surface.

Palette derivation lives in one backend utility and is reused by API responses,
server rendering, and tests. JavaScript preview implements the same documented
algorithm and is checked against backend fixture vectors.

## First-Paint Rendering

A dedicated branding request-context middleware resolves the cached global
branding for HTML requests and stores the normalized result on
`request.state.ui_branding`. It uses the existing `GlobalConfigService` cache,
so this does not introduce an uncached Registry read per page.

A synchronous Jinja context processor exposes `ui_branding` from request state.
`base.html` then:

- emits the derived CSS custom properties after static stylesheets;
- renders the configured title in the navbar;
- composes the default `<title>` with the configured application title;
- sets the navbar icon and favicon to the resolved logo URL.

Top-level page title blocks stop embedding the literal `OntoBricks`. They
provide only the page-specific prefix, composed with the configured application
title by a shared Jinja helper.

If configuration loading fails, middleware logs the error and uses the complete
default branding object. Branding failure must never prevent a page response.

## Theme Tokens and Consumers

The server override controls:

- `--db-primary`;
- `--db-primary-rgb`;
- `--db-primary-dark`;
- `--db-primary-darker`;
- `--db-primary-light`;
- `--db-hover-indigo` (retained name for compatibility);
- `--db-focus-ring`;
- `--db-shadow-primary`;
- new `--db-on-primary`;
- new `--db-primary-selected-text`.

Before runtime theming is enabled, hardcoded indigo consumers are replaced with
these tokens:

- `global/css/main.css`;
- `global/css/components.css`;
- `global/css/sidebar-layout.css`;
- `global/ontoviz/css/ontoviz.css`;
- the selected/highlight SVG styling in `ontology/js/ontology-map.js`.

The static `:root` values remain the complete default/fallback palette, so
standalone pages and failed branding resolution still render correctly.
Semantic status colors (success, warning, danger, info) and the separate
`--ovz-*` greyscale system are not configurable.

## Brand Icon Consumers

Every brand icon consumer uses the resolved `ui_branding.logo_url`, either
server-rendered or through a shared DOM hook:

- `base.html`: favicon and `#brandLogoImg`;
- `home.html`: hero icon;
- `partials/layout/help_modal.html`;
- `partials/dtwin/_query_chat.html`;
- `partials/ontology/_ontology_map.html`;
- Settings preview;
- GraphQL playground favicon where the HTML is generated by Python.

Decorative icons use empty alt text. The navbar logo alt text uses the
configured application title.

## Error Handling and Cache Behavior

- Validation errors return HTTP 400 with a field-specific message.
- Unauthorized writes return the existing admin authorization response.
- Registry-not-configured or persistence errors return the existing structured
  service error and do not mutate the cached branding.
- After a successful save, `GlobalConfigService` replaces its cache entry
  immediately. The response supplies the canonical saved object, which becomes
  the Settings page's new discard baseline.
- Other open browser tabs receive the new branding on navigation or refresh;
  cross-tab live synchronization is outside this scope.
- Upload preview object URLs are revoked when replaced, discarded, or saved.

## Testing

### Backend unit tests

- default branding object;
- legacy `navbar_logo` migration;
- atomic get/set behavior for Volume and Lakebase store contracts;
- title trimming, required/length limits, and escaping boundary;
- color normalization and invalid formats;
- palette fixture vectors and WCAG foreground contrast;
- logo MIME/size validation and reset/upload conflict;
- cache update only after persistence succeeds.

### API and permission tests

- GET normalized response;
- multipart POST title/color only;
- multipart POST with logo;
- reset to defaults;
- admin-only read/write;
- invalid request leaves stored configuration unchanged;
- legacy logo endpoints update unified branding.

### Frontend contract tests

- Configuration group contains the UI section;
- Global no longer contains visual controls;
- all title blocks compose with the configured title;
- no raw `#4F46E5` or `rgba(79, 70, 229, ...)` remains outside fallback token
  declarations and approved fixtures;
- every listed icon consumer resolves through branding;
- preview and discard use CSS variables and `textContent`.

### Browser verification

Desktop and mobile:

- UI section layout and responsive controls;
- immediate preview of title, primary/light states, icon, and favicon;
- discard restores the saved state;
- save survives navigation and a clean browser context;
- default reset;
- page titles across Home, Domain, Ontology, Mapping, Knowledge Graph, Settings,
  Registry, About, and access-denied pages;
- primary button contrast and selected navigation readability for representative
  dark, medium, and light input colors;
- no flash of default branding on a configured app.

The mandatory repository suite remains:

```shell
uv run --frozen pytest -q -m "not scenario"
```

## Documentation

- Update `.cursor/11-frontend-design.mdc` so `#4F46E5` is the default palette,
  not an immutable runtime value.
- Document that semantic colors are fixed and only the primary brand family is
  configurable.
- Update the user guide Settings section with UI branding, preview, discard,
  save, and reset behavior.
- Record implementation and verification in the versioned changelog.

## Out of Scope

- Dark mode.
- Independent secondary/light color.
- Configurable fonts, spacing, radii, or semantic status colors.
- Per-user, per-domain, or per-workspace themes.
- Cross-tab live synchronization.
- Animated or externally-hosted logo URLs.
