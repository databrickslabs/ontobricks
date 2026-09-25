import subprocess
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
HTML = ROOT / "src/front/templates/partials/domain/_domain_versions.html"
PAGE = ROOT / "src/front/templates/domain.html"
CSS = ROOT / "src/front/static/domain/css/domain-versions.css"
JS = ROOT / "src/front/static/domain/js/domain-versions.js"
REGISTRY_JS = ROOT / "src/front/static/registry/js/registry.js"


def _run_renderer_assertions(assertions: str, script: Path = JS) -> None:
    runner = """
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

class TestElement {
    constructor(tagName) {
        this.tagName = tagName.toUpperCase();
        this.children = [];
        this.parentElement = null;
        this.attributes = {};
        this.dataset = {};
        this.className = '';
        this.textContent = '';
        this.disabled = false;
        this.tabIndex = -1;
        this.title = '';
    }

    append(...children) {
        children.forEach((child) => this.appendChild(child));
    }

    prepend(child) {
        child.parentElement = this;
        this.children.unshift(child);
    }

    appendChild(child) {
        child.parentElement = this;
        this.children.push(child);
        return child;
    }

    setAttribute(name, value) {
        const text = String(value);
        this.attributes[name] = text;
        if (name === 'tabindex') this.tabIndex = Number(text);
        if (name === 'title') this.title = text;
    }
}

const document = {
    createElement: (tagName) => new TestElement(tagName),
    addEventListener: () => {},
    getElementById: () => null
};
const context = vm.createContext({document, window: {}, console});
vm.runInContext(fs.readFileSync(process.argv[1], 'utf8'), context);

function allElements(root) {
    return [root].concat(root.children.flatMap(allElements));
}

""" + textwrap.dedent(assertions)
    result = subprocess.run(
        ["node", "-e", runner, str(script)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_versions_use_semantic_card_list_not_table():
    html = HTML.read_text(encoding="utf-8")
    assert 'id="versionsCardList"' in html
    assert 'role="list"' in html
    assert "<table" not in html
    assert "onclick=" not in html
    assert "style=" not in html


def test_versions_stylesheet_is_wired():
    assert "domain/css/domain-versions.css" in PAGE.read_text(encoding="utf-8")
    assert CSS.exists()


def test_card_css_owns_full_height_scroll_and_mobile_reset():
    css = CSS.read_text(encoding="utf-8")
    assert "#versionsCardList" in css
    assert "overflow-y: auto" in css
    mobile = css[css.index("@media (max-width: 768px)") :]
    assert "#versions-section .dm-versions-workspace" in mobile
    assert "#versionsCardList" in mobile
    assert "overflow: visible" in mobile
    assert "height: auto" in mobile


def test_disabled_action_wrapper_has_visible_keyboard_focus():
    css = CSS.read_text(encoding="utf-8")
    assert ".dm-version-action-blocked:focus-visible" in css
    assert "var(--db-focus-ring)" in css


def test_js_renders_server_capabilities_and_new_endpoints():
    js = JS.read_text(encoding="utf-8")
    assert "version.transitions" in js
    assert "version.can_delete" in js
    assert "version.delete_control_visible" in js
    assert "/domain/set-version-status" in js
    assert "/domain/versions/" in js
    assert "STATUS_MAP" in js
    assert "is_latest" not in js
    assert "status === 'DRAFT'" not in js


def test_renderer_keeps_malicious_values_as_text_and_dom_properties():
    _run_renderer_assertions(
        r"""
        const attack = '1"><img src=x onerror=alert(1)>';
        const domain = 'acme" data-owned="yes"><script>alert(2)</script>';
        const reason = 'blocked"><img src=x onerror=alert(3)>';
        const version = {
            version: attack,
            status: 'DRAFT',
            description: '<img src=x onerror=alert(4)>',
            author: '"><script>alert(5)</script>',
            last_update: '" onmouseover="alert(6)',
            last_build: '<svg onload=alert(7)>',
            is_current: false,
            is_active: false,
            transitions: [{
                enabled: false,
                blocked_reason: reason,
                target_status: 'IN-REVIEW" data-owned="yes',
                label: '<img src=x onerror=alert(8)>'
            }],
            delete_control_visible: true,
            can_delete: false,
            delete_block_reason: reason
        };
        const card = context.renderVersionCard(version, domain);
        const elements = allElements(card);
        assert.equal(elements.some((item) => ['IMG', 'SCRIPT', 'SVG'].includes(item.tagName)), false);
        assert.equal(elements.find((item) => item.tagName === 'H5').textContent, 'v' + attack);
        assert.equal(
            elements.find(
                (item) => item.className.includes('dm-version-card-description')
            ).textContent,
            version.description
        );
        const transition = elements.find(
            (item) => item.tagName === 'BUTTON' && item.dataset.action === 'transition'
        );
        assert.equal(transition.dataset.version, attack);
        assert.equal(transition.dataset.domain, domain);
        assert.equal(transition.dataset.targetStatus, version.transitions[0].target_status);
        assert.equal(transition.textContent, version.transitions[0].label);
        assert.equal(transition.parentElement.title, reason);
        """
    )


def test_disabled_action_reasons_have_one_keyboard_reachable_wrapper():
    _run_renderer_assertions(
        r"""
        function render(canDelete) {
            return context.renderVersionCard({
                version: '2',
                status: 'DRAFT',
                transitions: [
                    {enabled: true, target_status: 'IN-REVIEW', label: 'Submit'},
                    {
                        enabled: false,
                        blocked_reason: 'Build required',
                        target_status: 'PUBLISHED',
                        label: 'Publish'
                    }
                ],
                delete_control_visible: true,
                can_delete: canDelete,
                delete_block_reason: canDelete ? '' : 'Latest version'
            }, 'acme');
        }

        const enabledDeleteCard = render(true);
        const enabledElements = allElements(enabledDeleteCard);
        const transitions = enabledElements.filter(
            (item) => item.tagName === 'BUTTON' && item.dataset.action === 'transition'
        );
        assert.equal(transitions[0].disabled, false);
        assert.equal(transitions[0].parentElement.tagName, 'DIV');
        assert.equal(transitions[1].disabled, true);
        assert.equal(transitions[1].parentElement.tagName, 'SPAN');
        assert.equal(transitions[1].parentElement.tabIndex, 0);
        assert.equal(transitions[1].parentElement.title, 'Build required');

        const enabledDelete = enabledElements.find(
            (item) => item.tagName === 'BUTTON' && item.dataset.action === 'delete'
        );
        assert.equal(enabledDelete.disabled, false);
        assert.equal(enabledDelete.parentElement.tabIndex, -1);

        const disabledElements = allElements(render(false));
        const disabledDelete = disabledElements.find(
            (item) => item.tagName === 'BUTTON' && item.dataset.action === 'delete'
        );
        assert.equal(disabledDelete.disabled, true);
        assert.equal(disabledDelete.parentElement.tabIndex, 0);
        assert.equal(disabledDelete.parentElement.title, 'Latest version');
        """
    )


def test_registry_uses_server_delete_capability():
    js = REGISTRY_JS.read_text(encoding="utf-8")
    assert "v.delete_control_visible" in js
    assert "v.can_delete" in js
    assert "v.delete_block_reason" in js
    delete_section = js[
        js.index("function createRegistryVersionDeleteControl") :
        js.index("document.addEventListener")
    ]
    assert "isLoaded" not in delete_section


def test_registry_delete_control_keeps_untrusted_values_in_dom_properties():
    _run_renderer_assertions(
        r"""
        const domain = 'acme" data-owned="yes"><script>alert(1)</script>';
        const version = '1"><img src=x onerror=alert(2)>';
        const reason = 'blocked"><svg onload=alert(3)>';
        const wrapper = context.createRegistryVersionDeleteControl({
            version,
            delete_control_visible: true,
            can_delete: false,
            delete_block_reason: reason
        }, domain);
        const elements = allElements(wrapper);
        assert.equal(elements.some((item) => ['IMG', 'SCRIPT', 'SVG'].includes(item.tagName)), false);
        const button = elements.find((item) => item.tagName === 'BUTTON');
        assert.equal(button.dataset.domain, domain);
        assert.equal(button.dataset.version, version);
        assert.equal(wrapper.title, reason);
        """,
        REGISTRY_JS,
    )


def test_registry_enabled_delete_confirmation_escapes_untrusted_values():
    _run_renderer_assertions(
        r"""
        (async () => {
            const domain = 'acme"><script data-owned="domain"></script>';
            const version = '1"><img data-owned="version" src=x>';
            const control = context.createRegistryVersionDeleteControl({
                version,
                delete_control_visible: true,
                can_delete: true,
                delete_block_reason: ''
            }, domain);
            const button = allElements(control).find((item) => item.tagName === 'BUTTON');
            let confirmation;
            context.escapeHtml = (value) => String(value)
                .replaceAll('&', '&amp;')
                .replaceAll('<', '&lt;')
                .replaceAll('>', '&gt;')
                .replaceAll('"', '&quot;')
                .replaceAll("'", '&#039;');
            context.showConfirmDialog = async (options) => {
                confirmation = options;
                return false;
            };

            await context.deleteRegistryVersion(
                button.dataset.domain,
                button.dataset.version
            );

            assert.equal(confirmation.title, 'Delete Version');
            assert.equal(confirmation.message.includes('<script'), false);
            assert.equal(confirmation.message.includes('<img'), false);
            assert.equal(confirmation.message.includes('&lt;script'), true);
            assert.equal(confirmation.message.includes('&lt;img'), true);
            assert.equal(confirmation.message.includes(domain), false);
            assert.equal(confirmation.message.includes(version), false);
        })().catch((error) => {
            console.error(error);
            process.exitCode = 1;
        });
        """,
        REGISTRY_JS,
    )


def test_registry_delete_conflict_shows_server_message_and_refreshes():
    _run_renderer_assertions(
        r"""
        (async () => {
            const calls = {notifications: [], refreshes: [], invalidations: 0};
            context.escapeHtml = (value) => String(value);
            context.showConfirmDialog = async () => true;
            context.showNotification = (...args) => calls.notifications.push(args);
            context.fetch = async (url, options) => {
                calls.request = {url, options};
                return {
                    status: 409,
                    json: async () => ({
                        success: false,
                        message: 'Deletion blocked by current server state'
                    })
                };
            };

            await context.deleteRegistryVersion(
                'domain/name',
                '1',
                (force) => calls.refreshes.push(force),
                () => { calls.invalidations += 1; }
            );

            assert.equal(
                calls.request.url,
                '/settings/registry/domains/domain%2Fname/versions/1'
            );
            assert.equal(calls.request.options.method, 'DELETE');
            assert.deepEqual(
                calls.notifications,
                [['Deletion blocked by current server state', 'error']]
            );
            assert.deepEqual(calls.refreshes, [true]);
            assert.equal(calls.invalidations, 0);
        })().catch((error) => {
            console.error(error);
            process.exitCode = 1;
        });
        """,
        REGISTRY_JS,
    )


def test_registry_delete_control_has_exactly_one_reachable_tab_stop():
    _run_renderer_assertions(
        r"""
        function render(canDelete) {
            return context.createRegistryVersionDeleteControl({
                version: '2',
                delete_control_visible: true,
                can_delete: canDelete,
                delete_block_reason: canDelete ? '' : 'Latest version'
            }, 'acme');
        }

        const enabledWrapper = render(true);
        const enabledButton = allElements(enabledWrapper).find(
            (item) => item.tagName === 'BUTTON'
        );
        assert.equal(enabledWrapper.tabIndex, -1);
        assert.equal(enabledButton.disabled, false);
        assert.equal(enabledButton.tabIndex, 0);

        const disabledWrapper = render(false);
        const disabledButton = allElements(disabledWrapper).find(
            (item) => item.tagName === 'BUTTON'
        );
        assert.equal(disabledWrapper.tabIndex, 0);
        assert.equal(disabledWrapper.title, 'Latest version');
        assert.equal(disabledButton.disabled, true);
        """,
        REGISTRY_JS,
    )
