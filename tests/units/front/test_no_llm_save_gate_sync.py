"""Domain → General saves must synchronously refresh the No-LLM gate."""

import json
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
DOMAIN_JS = REPO_ROOT / "src/front/static/domain/js/domain.js"
NAVBAR_JS = REPO_ROOT / "src/front/static/global/js/navbar.js"


def _run_save(saved_endpoint: str, success: bool = True) -> list[list[object]]:
    harness = f"""
const fs = require('fs');
const vm = require('vm');

const events = [];
const elements = {{
    domainName: {{ value: 'EnergyDomain' }},
    domainDescription: {{ value: 'Energy domain' }},
    domainAuthor: {{ value: 'owner@example.com' }},
    domainLlmEndpoint: {{ value: {json.dumps(saved_endpoint)} }},
}};
const context = {{
    console,
    URLSearchParams,
    setTimeout,
    clearTimeout,
    sessionStorage: {{ getItem: () => null }},
    document: {{
        body: {{ classList: {{ add: () => {{}} }} }},
        addEventListener: () => {{}},
        getElementById: (id) => elements[id] || null,
        querySelector: () => null,
        querySelectorAll: () => [],
    }},
    showNotification: () => {{}},
    fetch: async () => ({{
        json: async () => ({{
            success: {str(success).lower()},
            info: {{ llm_endpoint: {json.dumps(saved_endpoint)} }},
            message: 'save result',
        }}),
    }}),
    refreshNavbarIndicators: () => {{
        events.push(['refresh']);
        return new Promise(() => {{}});
    }},
}};
context.window = context;
context.window.location = {{ search: '', pathname: '/domain/' }};
context.window.OB = {{
    updateLlmAvailability: (configured) => events.push(['availability', configured]),
}};

vm.createContext(context);
vm.runInContext(fs.readFileSync({json.dumps(str(DOMAIN_JS))}, 'utf8'), context);
context.saveDomainInfo().then(() => process.stdout.write(JSON.stringify(events)));
"""
    result = subprocess.run(
        ["node", "-e", harness],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize(
    ("saved_endpoint", "configured"),
    [("", False), ("   ", False), ("gateway-model", True)],
)
def test_successful_save_updates_gate_before_navbar_refresh(
    saved_endpoint: str, configured: bool
):
    assert _run_save(saved_endpoint) == [
        ["availability", configured],
        ["refresh"],
    ]


def test_failed_save_does_not_change_gate_or_refresh_navbar():
    assert _run_save("gateway-model", success=False) == []


def _run_pre_registry_save(
    saved_endpoint: str, *, http_ok: bool = True, success: bool = True
) -> list[list[object]]:
    harness = f"""
const fs = require('fs');
const vm = require('vm');

const events = [];
const elements = {{
    domainName: {{ value: 'EnergyDomain' }},
    domainLlmEndpoint: {{ value: 'unsaved-form-value' }},
}};
const context = {{
    console: {{ log: () => {{}}, warn: () => {{}}, error: () => {{}} }},
    URL,
    URLSearchParams,
    setTimeout,
    clearTimeout,
    sessionStorage: {{
        getItem: () => null,
        setItem: () => {{}},
        removeItem: () => {{}},
    }},
    history: {{ replaceState: () => {{}} }},
    document: {{
        body: {{ dataset: {{}} }},
        addEventListener: () => {{}},
        getElementById: (id) => elements[id] || null,
        querySelector: () => null,
        querySelectorAll: () => [],
    }},
    showNotification: () => {{}},
    fetch: async () => ({{
        ok: {str(http_ok).lower()},
        json: async () => ({{
            success: {str(success).lower()},
            info: {{ llm_endpoint: {json.dumps(saved_endpoint)} }},
            message: 'save result',
        }}),
    }}),
    invalidateDomainCaches: () => events.push(['invalidate']),
}};
context.window = context;
context.window.location = {{
    search: '',
    pathname: '/domain/',
    origin: 'http://localhost',
    hash: '',
}};
context.window.OB = {{
    updateLlmAvailability: (configured) => events.push(['availability', configured]),
}};

vm.createContext(context);
vm.runInContext(fs.readFileSync({json.dumps(str(NAVBAR_JS))}, 'utf8'), context);
context.invalidateDomainCaches = () => events.push(['invalidate']);
context.saveDomainInfoBeforeSave().then(() => {{
    events.push(['registry']);
    process.stdout.write(JSON.stringify(events));
}});
"""
    result = subprocess.run(
        ["node", "-e", harness],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize(
    ("saved_endpoint", "configured"),
    [("", False), ("   ", False), ("gateway-model", True)],
)
def test_pre_registry_save_updates_gate_from_response_before_next_steps(
    saved_endpoint: str, configured: bool
):
    assert _run_pre_registry_save(saved_endpoint) == [
        ["availability", configured],
        ["invalidate"],
        ["registry"],
    ]


@pytest.mark.parametrize(
    ("http_ok", "success"),
    [(False, True), (True, False)],
)
def test_pre_registry_save_failure_does_not_change_gate(
    http_ok: bool, success: bool
):
    assert _run_pre_registry_save(
        "gateway-model", http_ok=http_ok, success=success
    ) == [["registry"]]
