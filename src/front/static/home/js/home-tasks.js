/**
 * Home → My Tasks
 *
 * Compact review worklist surfaced on the home page, just below the
 * Current Domain panel. Mirrors Registry → My Tasks but only reveals
 * itself when the current user actually has pending review tasks.
 *
 * Data source: GET /review/my-tasks (see ReviewService.my_tasks).
 */
(function () {
    'use strict';

    document.addEventListener('DOMContentLoaded', loadTasks);

    async function loadTasks() {
        const section = document.getElementById('homeTasksSection');
        const container = document.getElementById('homeTasksContainer');
        if (!section || !container) return;

        try {
            const resp = await fetch('/review/my-tasks', {
                credentials: 'same-origin',
            });
            const data = await resp.json();
            const tasks = (resp.ok && data.success && data.tasks) ? data.tasks : [];
            if (!tasks.length) {
                section.style.display = 'none';
                return;
            }
            render(container, tasks);
            section.style.display = '';
        } catch (err) {
            // Home page must stay usable even if the review API is down.
            console.error('home loadTasks error:', err);
            section.style.display = 'none';
        }
    }

    function render(container, tasks) {
        const rows = tasks.map((t) => {
            const actions = validateButton(t);
            return '<tr>' +
                '<td class="fw-medium">' + escapeHtml(t.domain) + '</td>' +
                '<td>v' + escapeHtml(t.version) + '</td>' +
                '<td>' + statusBadge(t.status) + '</td>' +
                '<td><span class="my-tasks-approvals">' +
                t.approvals + ' / ' + t.required + '</span></td>' +
                '<td class="text-end">' + commentsButton(t) + ' ' + actions +
                '</td></tr>';
        }).join('');

        container.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-sm align-middle my-tasks-table mb-0">' +
            '<thead><tr>' +
            '<th>Domain</th><th>Version</th><th>Status</th>' +
            '<th>Approvals</th><th class="text-end">Review</th>' +
            '</tr></thead><tbody>' + rows + '</tbody></table></div>';

        container.querySelectorAll('button[data-validate]').forEach((btn) => {
            btn.addEventListener('click', () => {
                loadDomainAndReview(btn.dataset.domain, btn.dataset.version);
            });
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

    // The worklist no longer drives the workflow inline. Every task links
    // to the Domain → Validation workspace (loading the version first), the
    // single place to submit / approve / publish / send back to draft.
    function validateButton(task) {
        return '<button type="button" class="btn btn-sm btn-outline-info ms-1" ' +
            'data-validate="1" ' +
            'data-domain="' + escapeAttr(task.domain) + '" ' +
            'data-version="' + escapeAttr(task.version) + '" ' +
            'title="Open this version in the Validation workspace">' +
            '<i class="bi bi-ui-checks me-1"></i>Validate</button>';
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
