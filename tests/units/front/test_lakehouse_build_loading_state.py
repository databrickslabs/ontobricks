"""Contracts for the Lakehouse Build initial loading state."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]
BUILD_HTML = REPO_ROOT / "src/front/templates/partials/dtwin/_query_databricks_build.html"
BUILD_JS = REPO_ROOT / "src/front/static/query/js/query-databricks-build.js"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _function_body(source: str, signature: str, next_signature: str) -> str:
    start = source.index(signature)
    end = source.index(next_signature, start)
    return source[start:end]


def test_server_markup_starts_with_spinner_only() -> None:
    html = _read(BUILD_HTML)
    loading = html[html.index('id="dbxBuildLoadingState"') - 80 :]
    loading = loading[: loading.index("</div>") + len("</div>")]
    assert 'role="status"' in loading
    assert 'aria-live="polite"' in loading
    assert "ob-loading-spinner" in loading
    assert "Loading triple store information..." in loading
    assert "d-none" not in loading
    content_line = next(
        line for line in html.splitlines() if 'id="dbxBuildContent"' in line
    )
    assert "d-none" in content_line


def test_server_markup_has_hidden_retryable_error() -> None:
    html = _read(BUILD_HTML)
    error_line = next(
        line for line in html.splitlines() if 'id="dbxBuildLoadError"' in line
    )
    assert "d-none" in error_line
    assert 'role="alert"' in error_line
    assert 'id="dbxBuildLoadRetry"' in html


def test_state_transition_controls_all_three_containers() -> None:
    js = _read(BUILD_JS)
    body = _function_body(
        js,
        "function _setDbxBuildInitialState(",
        "\nasync function loadDatabricksBuildInfo",
    )
    for element_id in (
        "dbxBuildLoadingState",
        "dbxBuildLoadError",
        "dbxBuildContent",
    ):
        assert element_id in body
    assert "state !== 'loading'" in body
    assert "state !== 'error'" in body
    assert "state !== 'ready'" in body


def test_initial_loader_reveals_content_only_after_success() -> None:
    js = _read(BUILD_JS)
    body = _function_body(
        js,
        "async function loadDatabricksBuildInfo(",
        "\nfunction _apiErrorMessage",
    )
    assert "let dbxBuildInfoLoaded = false;" in js
    assert "if (!resp.ok || !data.success)" in body
    assert "throw new Error(" in body
    assert "dbxBuildInfoLoaded = true;" in body
    render = body.index("_applyDbxStorageKind(isViewMode);")
    ready = body.index("_setDbxBuildInitialState('ready')")
    assert render < ready


def test_initial_error_stays_hidden_until_retry_succeeds() -> None:
    js = _read(BUILD_JS)
    body = _function_body(
        js,
        "async function loadDatabricksBuildInfo(",
        "\nfunction _apiErrorMessage",
    )
    assert "const isInitialLoad = !dbxBuildInfoLoaded;" in body
    assert "if (isInitialLoad)" in body
    assert "_setDbxBuildInitialState('loading')" in body
    assert "_setDbxBuildInitialState('error')" in body
    init = js[js.index("document.addEventListener('DOMContentLoaded'") :]
    assert "dbxBuildLoadRetry" in init
    assert "addEventListener('click', loadDatabricksBuildInfo)" in init


def test_a_stale_initial_failure_cannot_hide_loaded_content() -> None:
    js = _read(BUILD_JS)
    body = _function_body(
        js,
        "async function loadDatabricksBuildInfo(",
        "\nfunction _apiErrorMessage",
    )
    assert "if (isInitialLoad && !dbxBuildInfoLoaded)" in body
