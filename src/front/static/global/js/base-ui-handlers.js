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

    function init() {
        initNavbarActionDelegation();
        initTaskTrackerControls();
        initNotificationControls();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
