/**
 * Navbar and shell UI handlers (replaces inline onclick in base.html).
 */
(function () {
    'use strict';

    /** Menu actions from menu_config.json — only these may be invoked from data attributes. */
    var NAVBAR_ACTIONS = {
        domainNew: true,
        domainLoad: true,
        domainSave: true
    };

    function initNavbarActionDelegation() {
        var nav = document.getElementById('navbarNav');
        if (!nav) return;
        nav.addEventListener('click', function (e) {
            var link = e.target.closest('a[data-navbar-action]');
            if (!link) return;
            var name = link.getAttribute('data-navbar-action');
            if (!name || !NAVBAR_ACTIONS[name]) return;
            e.preventDefault();
            var fn = window[name];
            if (typeof fn === 'function') fn();
        });
    }

    function initTaskTrackerControls() {
        var taskToggle = document.getElementById('taskTrackerToggle');
        if (taskToggle) {
            taskToggle.addEventListener('click', function (e) {
                if (typeof window.toggleTaskDropdown === 'function') {
                    window.toggleTaskDropdown(e);
                }
                e.preventDefault();
            });
        }
        var refreshBtn = document.getElementById('taskTrackerRefreshBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', function () {
                if (typeof window.refreshTasks === 'function') window.refreshTasks();
            });
        }
    }

    function initNotificationControls() {
        var notifToggle = document.getElementById('notifCenterToggle');
        if (notifToggle && window.NotificationCenter) {
            notifToggle.addEventListener('click', function (e) {
                window.NotificationCenter.toggle(e);
                e.preventDefault();
            });
        }
        var clearBtn = document.getElementById('notifCenterClearBtn');
        if (clearBtn && window.NotificationCenter) {
            clearBtn.addEventListener('click', function () {
                window.NotificationCenter.clearAll();
            });
        }
    }

    // ── Keyboard shortcuts ──────────────────────────────────────────

    var _SHORTCUT_OVERLAY_ID = 'obShortcutOverlay';

    function _isMac() { return navigator.platform.indexOf('Mac') > -1; }
    function _mod(e) { return _isMac() ? e.metaKey : e.ctrlKey; }

    function _buildOverlay() {
        var el = document.getElementById(_SHORTCUT_OVERLAY_ID);
        if (el) return el;
        el = document.createElement('div');
        el.id = _SHORTCUT_OVERLAY_ID;
        el.className = 'ob-shortcut-overlay d-none';
        var mod = _isMac() ? 'Cmd' : 'Ctrl';
        el.innerHTML =
            '<div class="ob-shortcut-card">' +
            '<h6><i class="bi bi-keyboard me-2"></i>Keyboard Shortcuts</h6>' +
            '<table class="table table-sm mb-0">' +
            '<tr><td><kbd>' + mod + '+S</kbd></td><td>Save domain</td></tr>' +
            '<tr><td><kbd>' + mod + '+K</kbd></td><td>Focus sidebar search (if available)</td></tr>' +
            '<tr><td><kbd>?</kbd></td><td>Show / hide this overlay</td></tr>' +
            '</table>' +
            '<div class="text-muted small mt-2">Press <kbd>Esc</kbd> or <kbd>?</kbd> to close</div>' +
            '</div>';
        document.body.appendChild(el);
        el.addEventListener('click', function (ev) {
            if (ev.target === el) el.classList.add('d-none');
        });
        return el;
    }

    function _toggleShortcutHelp() {
        var ol = _buildOverlay();
        ol.classList.toggle('d-none');
    }

    function initKeyboardShortcuts() {
        document.addEventListener('keydown', function (e) {
            var tag = (e.target.tagName || '').toLowerCase();
            var inInput = (tag === 'input' || tag === 'textarea' || tag === 'select' || e.target.isContentEditable);

            if (_mod(e) && e.key.toLowerCase() === 's') {
                e.preventDefault();
                var saveFn = window.domainSave;
                if (typeof saveFn === 'function') saveFn();
            }

            if (_mod(e) && e.key.toLowerCase() === 'k') {
                e.preventDefault();
                var searchInput = document.querySelector('.sidebar-nav input[type="search"], .sidebar-nav input[type="text"]');
                if (searchInput) searchInput.focus();
            }

            if (!inInput && e.key === '?') {
                e.preventDefault();
                _toggleShortcutHelp();
            }

            if (e.key === 'Escape') {
                var ol = document.getElementById(_SHORTCUT_OVERLAY_ID);
                if (ol && !ol.classList.contains('d-none')) {
                    ol.classList.add('d-none');
                }
            }
        });
    }

    function init() {
        initNavbarActionDelegation();
        initTaskTrackerControls();
        initNotificationControls();
        initKeyboardShortcuts();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
