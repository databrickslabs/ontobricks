/**
 * Home → My tasks tab
 *
 * Review worklist + tasks assigned to the current user. The tab is always
 * visible; an empty state is shown when there is nothing to do. Reloads each
 * time the tab is opened (``sidebarSectionChanged`` with section ``tasks``).
 *
 * Data source: GET /review/my-tasks (see ReviewService.my_tasks).
 */
(function () {
    'use strict';

    document.addEventListener('DOMContentLoaded', loadTasks);
    document.addEventListener('sidebarSectionChanged', (e) => {
        if (e.detail?.section === 'tasks') loadTasks();
    });

    async function loadTasks() {
        const container = document.getElementById('homeTasksContainer');
        if (!container) return;

        try {
            const resp = await fetch('/review/my-tasks', {
                credentials: 'same-origin',
            });
            const data = await resp.json();
            const ok = resp.ok && data.success;
            const tasks = (ok && data.tasks) ? data.tasks : [];
            const assigned = (ok && data.assigned_tasks) ? data.assigned_tasks : [];
            render(container, tasks, assigned);
            setTileCount(tasks.length + assigned.length);
        } catch (err) {
            // Home page must stay usable even if the review API is down.
            console.error('home loadTasks error:', err);
            setTileCount(null);
            container.innerHTML =
                '<div class="my-tasks-empty text-muted small py-3">' +
                '<i class="bi bi-exclamation-triangle me-1"></i>Could not load tasks.</div>';
        }
    }

    /** My tasks tile count (null = unavailable). */
    function setTileCount(n) {
        const el = document.getElementById('homeCountTasks');
        if (el) el.textContent = (n === null) ? '\u2014' : String(n);
    }

    function render(container, tasks, assigned) {
        assigned = assigned || [];
        if (!tasks.length && !assigned.length) {
            container.innerHTML =
                '<div class="my-tasks-empty text-muted small py-4 text-center">' +
                '<i class="bi bi-check2-circle fs-4 d-block mb-2"></i>' +
                'No tasks waiting for you.</div>';
            return;
        }
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

        const reviewBlock = tasks.length
            ? '<div class="table-responsive">' +
              '<table class="table table-hover align-middle ob-runs-table mb-0">' +
              '<thead><tr>' +
              '<th>Context</th><th>Version</th><th>Status</th>' +
              '<th>Approvals</th><th class="text-end">Review</th>' +
              '</tr></thead><tbody>' + rows + '</tbody></table></div>'
            : '';

        container.innerHTML = reviewBlock + assignedBlock(assigned);

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
        container.querySelectorAll('button[data-task-done]').forEach((btn) => {
            btn.addEventListener('click', () => completeTask(btn));
        });
    }

    function assignedBlock(assigned) {
        if (!assigned.length) return '';
        const rows = assigned.map((t) =>
            '<tr>' +
            '<td class="fw-medium">' + escapeHtml(t.title) + '</td>' +
            '<td>' + escapeHtml(t.folder) + ' v' + escapeHtml(t.version) + '</td>' +
            '<td>' + escapeHtml((t.status || 'open').replace('_', ' ')) + '</td>' +
            '<td class="text-end">' +
            '<button type="button" class="btn btn-sm btn-success" data-task-done="1" ' +
            'data-folder="' + escapeAttr(t.folder) + '" ' +
            'data-version="' + escapeAttr(t.version) + '" ' +
            'data-task-id="' + escapeAttr(t.id) + '">' +
            '<i class="bi bi-check2 me-1"></i>Done</button></td></tr>'
        ).join('');
        return '<div class="mt-3 mb-2 fw-medium small text-uppercase text-muted">' +
            '<i class="bi bi-person-check me-1"></i>Assigned to me</div>' +
            '<div class="table-responsive">' +
            '<table class="table table-hover align-middle ob-runs-table mb-0">' +
            '<thead><tr><th>Task</th><th>Context</th><th>Status</th>' +
            '<th class="text-end">Action</th></tr></thead>' +
            '<tbody>' + rows + '</tbody></table></div>';
    }

    async function completeTask(btn) {
        const { folder, version, taskId } = btn.dataset;
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(folder) + '/' +
                encodeURIComponent(version) + '/tasks/' +
                encodeURIComponent(taskId) + '/status',
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ status: 'done' }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showNotification(data.message || 'Failed to update task', 'error');
                return;
            }
            showNotification('Task completed.', 'success');
            loadTasks();
        } catch (err) {
            showNotification('Error: ' + err.message, 'error');
        }
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
    // to the Context → Validation workspace (loading the version first), the
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
                showNotification('Error: ' + (data.message || 'Failed to load context'), 'error');
                return;
            }
            window.location.href = '/domain/?section=review';
        } catch (err) {
            showNotification('Error loading context: ' + err.message, 'error');
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

    function escapeAttr(text) {
        return escapeHtml(text).replace(/"/g, '&quot;');
    }
})();
