/**
 * Registry → My Tasks
 *
 * Cross-domain review worklist. Lists the domain versions where the
 * current user has a pending action (submit for review, sign off, or
 * publish) and lets them act inline. "Review & sign off" loads the
 * domain and deep-links to the Domain → Validation workspace.
 *
 * Data source: GET /review/my-tasks (see ReviewService.my_tasks).
 */
(function () {
    'use strict';

    let loaded = false;

    document.addEventListener('DOMContentLoaded', init);

    function init() {
        document.addEventListener('sidebarSectionChanged', (e) => {
            if (e.detail?.section === 'my-tasks' && !loaded) {
                loadTasks();
            }
        });

        if (new URLSearchParams(window.location.search).get('section') === 'my-tasks') {
            loadTasks();
        }

        document.getElementById('btnRefreshMyTasks')?.addEventListener('click', () => {
            loaded = false;
            loadTasks();
        });
    }

    async function loadTasks() {
        const container = document.getElementById('myTasksContainer');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Loading your tasks...</div>';

        try {
            const resp = await fetch('/review/my-tasks', {
                credentials: 'same-origin',
            });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                container.innerHTML =
                    '<div class="alert alert-danger small mb-0">' +
                    escapeHtml(data.message || 'Failed to load tasks') +
                    '</div>';
                return;
            }
            render(data.tasks || []);
            loaded = true;
        } catch (err) {
            console.error('loadTasks error:', err);
            showNotification('Could not load your tasks: ' + err.message, 'error');
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">Network error: ' +
                escapeHtml(String(err)) + '</div>';
        }
    }

    function render(tasks) {
        const container = document.getElementById('myTasksContainer');
        if (!container) return;

        if (!tasks.length) {
            container.innerHTML =
                '<div class="my-tasks-empty text-center text-muted py-5">' +
                '<i class="bi bi-check2-circle d-block mb-2"></i>' +
                "You're all caught up &mdash; no pending review tasks." +
                '</div>';
            return;
        }

        const rows = tasks.map((t) => {
            const approvals = '<span class="my-tasks-approvals">' +
                t.approvals + ' / ' + t.required + '</span>';
            const actions = (t.actions || [])
                .map((a) => actionButton(t, a))
                .join(' ');
            return '<tr>' +
                '<td class="fw-medium">' + escapeHtml(t.domain) + '</td>' +
                '<td>v' + escapeHtml(t.version) + '</td>' +
                '<td>' + statusBadge(t.status) + '</td>' +
                '<td>' + approvals + '</td>' +
                '<td>' + relativeTime(t.last_activity) + '</td>' +
                '<td class="text-end">' + commentsButton(t) + ' ' + actions + '</td>' +
                '</tr>';
        }).join('');

        container.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-sm align-middle my-tasks-table mb-0">' +
            '<thead><tr>' +
            '<th>Domain</th><th>Version</th><th>Status</th>' +
            '<th>Approvals</th><th>Last activity</th>' +
            '<th class="text-end">Your action</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table></div>';

        container.querySelectorAll('button[data-action]').forEach((btn) => {
            btn.addEventListener('click', onAction);
        });
        container.querySelectorAll('button[data-comments]').forEach((btn) => {
            btn.addEventListener('click', () => {
                ReviewModals.showComments(btn.dataset.domain, btn.dataset.version);
            });
        });
    }

    function commentsButton(task) {
        return '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" ' +
            'data-comments="1" ' +
            'data-domain="' + escapeAttr(task.domain) + '" ' +
            'data-version="' + escapeAttr(task.version) + '" ' +
            'title="View all comments">' +
            '<i class="bi bi-chat-dots"></i></button>';
    }

    function actionButton(task, action) {
        const cls = action.id === 'publish'
            ? 'btn-success'
            : (action.id === 'review' ? 'btn-primary' : 'btn-outline-secondary');
        const icon = action.id === 'publish'
            ? 'broadcast'
            : (action.id === 'review' ? 'patch-check' : 'send');
        return '<button type="button" class="btn btn-sm ' + cls + ' ms-1" ' +
            'data-action="' + escapeAttr(action.id) + '" ' +
            'data-domain="' + escapeAttr(task.domain) + '" ' +
            'data-version="' + escapeAttr(task.version) + '">' +
            '<i class="bi bi-' + icon + ' me-1"></i>' +
            escapeHtml(action.label) + '</button>';
    }

    async function onAction(e) {
        const btn = e.currentTarget;
        const action = btn.dataset.action;
        const domain = btn.dataset.domain;
        const version = btn.dataset.version;

        if (action === 'review') {
            await loadDomainAndReview(domain, version);
            return;
        }
        if (action === 'submit') {
            await transition(domain, version, 'submit', {
                title: 'Submit for review',
                message: 'Submit <strong>' + escapeHtml(domain) + '</strong> v' +
                    escapeHtml(version) + ' for review? Editing locks until it is returned to Draft.',
                confirmText: 'Submit',
                icon: 'eye',
            });
            return;
        }
        if (action === 'publish') {
            await transition(domain, version, 'publish', {
                title: 'Publish version',
                message: 'Publish <strong>' + escapeHtml(domain) + '</strong> v' +
                    escapeHtml(version) + '? It becomes the live version on the API/MCP surface.',
                confirmText: 'Publish',
                icon: 'broadcast',
            });
        }
    }

    async function transition(domain, version, endpoint, dialog) {
        const r = await ReviewModals.promptComment({
            title: dialog.title,
            message: dialog.message,
            confirmText: dialog.confirmText,
            confirmClass: 'btn-primary',
            icon: dialog.icon,
        });
        if (!r.confirmed) return;

        try {
            const resp = await fetch(
                '/review/' + encodeURIComponent(domain) + '/' +
                encodeURIComponent(version) + '/' + endpoint,
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ comment: r.comment }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showNotification(data.message || 'Action failed', 'error');
                return;
            }
            showNotification(
                domain + ' v' + version + ' is now ' + (data.status || '') + '.',
                'success'
            );
            loaded = false;
            loadTasks();
        } catch (err) {
            showNotification('Error: ' + err.message, 'error');
        }
    }

    async function loadDomainAndReview(domain, version) {
        try {
            showNotification('Opening ' + domain + ' v' + version + '…', 'info', 4000);
            const resp = await fetch('/domain/load-from-uc', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ domain: domain, version: version }),
            });
            const data = await resp.json();
            if (!data.success) {
                showNotification('Error: ' + (data.message || 'Failed to load domain'), 'error');
                return;
            }
            window.location.href = '/domain/?section=review';
        } catch (err) {
            showNotification('Error loading domain: ' + err.message, 'error');
        }
    }

    function statusBadge(status) {
        const map = {
            'DRAFT': 'bg-warning-subtle text-dark border-warning',
            'IN-REVIEW': 'bg-info-subtle text-dark border-info',
            'PUBLISHED': 'bg-success-subtle text-dark border-success',
        };
        const cls = map[status] || map['DRAFT'];
        const label = status === 'IN-REVIEW' ? 'In Review'
            : ((status || 'DRAFT').charAt(0) +
               (status || 'DRAFT').slice(1).toLowerCase());
        return '<span class="badge border ' + cls + '">' +
            escapeHtml(label) + '</span>';
    }

    function relativeTime(iso) {
        if (!iso) return '<span class="text-muted small">never</span>';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '<span class="text-muted small">&mdash;</span>';
        const diff = (Date.now() - d.getTime()) / 1000;
        let txt;
        if (diff < 60) txt = 'just now';
        else if (diff < 3600) txt = Math.floor(diff / 60) + 'm ago';
        else if (diff < 86400) txt = Math.floor(diff / 3600) + 'h ago';
        else txt = d.toLocaleDateString();
        return '<span class="text-muted small">' + escapeHtml(txt) + '</span>';
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    function escapeAttr(text) {
        return escapeHtml(text).replace(/"/g, '&quot;');
    }
})();
