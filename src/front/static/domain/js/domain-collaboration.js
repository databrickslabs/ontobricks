/**
 * Context → Collaboration
 *
 * Two sections that concentrate every collaborative signal for the loaded
 * domain in one place:
 *
 *   • My Tasks    — tasks assigned to the current user plus the review
 *                   worklist (data: GET /review/my-tasks).
 *   • Discussions — a single activity timeline merging every comment thread
 *                   across the domain's surfaces (ontology, mappings, graph,
 *                   domain), newest first (data: GET /comments/{folder}/{ver}).
 *
 * The thread panel itself is the shared global component (comments-panel.js):
 * clicking a timeline entry re-opens it via OntoComments.openThread().
 */
(function () {
    'use strict';

    let tasksLoaded = false;
    let discussionsLoaded = false;
    let domainCtx = null;          // { folder, version, hasRegistry }
    let allComments = [];          // cached raw comments for re-render
    let allTaskRows = [];          // normalised My Tasks rows (assigned + review)
    const selectedTags = new Set(); // active tag-filter keys (multi-select)

    document.addEventListener('DOMContentLoaded', init);

    function init() {
        document.addEventListener('sidebarSectionChanged', (e) => {
            const section = e.detail && e.detail.section;
            if (section === 'mytasks' && !tasksLoaded) loadTasks();
            if (section === 'discussions' && !discussionsLoaded) loadDiscussions();
        });

        const initial = new URLSearchParams(window.location.search).get('section');
        if (initial === 'mytasks') loadTasks();
        if (initial === 'discussions') loadDiscussions();

        on('btnRefreshDomainTasks', 'click', () => { tasksLoaded = false; loadTasks(); });
        initTaskFilters();
        on('btnRefreshDomainDiscussions', 'click', () => {
            discussionsLoaded = false;
            loadDiscussions();
        });
        on('domainDiscShowResolved', 'change', renderDiscussions);
    }

    // ---- shared helpers -----------------------------------------------------

    function on(id, evt, fn) {
        const elx = document.getElementById(id);
        if (elx) elx.addEventListener(evt, fn);
    }

    function esc(text) {
        if (typeof window.escapeHtml === 'function') return window.escapeHtml(text);
        if (text == null) return '';
        const d = document.createElement('div');
        d.textContent = String(text);
        return d.innerHTML;
    }

    function escAttr(text) {
        return esc(text).replace(/"/g, '&quot;');
    }

    function note(msg, kind) {
        if (typeof window.showNotification === 'function') {
            window.showNotification(msg, kind || 'info');
        }
    }

    async function resolveDomainCtx() {
        if (domainCtx) return domainCtx;
        try {
            const r = await fetch('/domain/version-status', { credentials: 'same-origin' });
            const vs = await r.json();
            domainCtx = {
                folder: vs.domain_folder || '',
                version: vs.version || '',
                hasRegistry: !!vs.has_registry,
            };
        } catch (e) {
            domainCtx = { folder: '', version: '', hasRegistry: false };
        }
        return domainCtx;
    }

    function relativeTime(iso) {
        if (!iso) return 'never';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        const diff = (Date.now() - d.getTime()) / 1000;
        if (diff < 60) return 'just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return formatDateTime(iso);
    }

    function dayKey(iso) {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return 'Unknown date';
        const today = new Date();
        const yest = new Date();
        yest.setDate(today.getDate() - 1);
        if (d.toDateString() === today.toDateString()) return 'Today';
        if (d.toDateString() === yest.toDateString()) return 'Yesterday';
        return formatDateOnly(iso);
    }

    function parsedTags(cm) {
        const parsed = (window.OntoComments && OntoComments.parseBody)
            ? OntoComments.parseBody(cm.body)
            : { tags: [] };
        return parsed.tags || [];
    }

    function tagKey(t) {
        return String(t.ref || t.label || '');
    }

    // ========================================================================
    // MY TASKS
    // ========================================================================

    async function loadTasks() {
        const c = document.getElementById('domainMyTasksContainer');
        if (!c) return;
        c.innerHTML = spinner('Loading your tasks...');
        try {
            const resp = await fetch('/review/my-tasks', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                c.innerHTML = errBox(data.message || 'Failed to load tasks');
                return;
            }
            renderTasks(data.tasks || [], data.assigned_tasks || []);
            tasksLoaded = true;
        } catch (err) {
            c.innerHTML = errBox('Network error: ' + String(err));
        }
    }

    function renderTasks(reviewTasks, assigned) {
        allTaskRows = assigned.map(assignedRow).concat(reviewTasks.map(reviewRow));
        allTaskRows.sort((a, b) => String(b.updated || '').localeCompare(String(a.updated || '')));
        applyTaskFilters();
    }

    function assignedRow(t) {
        return {
            kind: 'assigned',
            title: t.title || '',
            folder: t.folder || '',
            version: t.version || '',
            from: t.created_by || '',
            status: String(t.status || 'open').toLowerCase(),
            updated: t.updated_at || t.created_at || '',
            due: t.due_date || '',
            task: t,
        };
    }

    function reviewRow(t) {
        const labels = (t.actions || []).map((a) => a.label).filter(Boolean);
        return {
            kind: 'review',
            title: labels.join(' · ') || 'Review',
            folder: t.domain || '',
            version: t.version || '',
            from: '',
            status: String(t.status || 'DRAFT').toLowerCase(),
            updated: t.last_activity || '',
            due: '',
            approvals: esc(t.approvals) + ' / ' + esc(t.required),
        };
    }

    function filterValue(id) {
        const b = document.getElementById(id);
        return b ? (b.dataset.value || '') : '';
    }

    function applyTaskFilters() {
        const c = document.getElementById('domainMyTasksContainer');
        if (!c) return;
        const q = ((document.getElementById('domainTasksSearch') || {}).value || '')
            .trim().toLowerCase();
        const kind = filterValue('domainTasksTypeFilter');
        const status = filterValue('domainTasksStatusFilter');

        const rows = allTaskRows.filter((r) =>
            (!kind || r.kind === kind) &&
            (!status || r.status === status) &&
            (!q || [r.title, r.folder, r.from].some((v) => String(v).toLowerCase().includes(q))));

        const count = document.getElementById('domainTasksCount');
        if (count) {
            const n = allTaskRows.length;
            count.textContent = rows.length === n
                ? n + (n === 1 ? ' task' : ' tasks')
                : rows.length + ' of ' + n + ' tasks';
        }

        if (!allTaskRows.length) {
            c.innerHTML =
                '<div class="text-center text-muted py-5">' +
                '<i class="bi bi-check2-circle d-block mb-2" style="font-size:1.8rem;"></i>' +
                "You're all caught up &mdash; no pending tasks.</div>";
            return;
        }
        if (!rows.length) {
            c.innerHTML =
                '<div class="text-center text-muted py-5">' +
                '<i class="bi bi-funnel d-block mb-2" style="font-size:1.8rem;"></i>' +
                'No tasks match the current filters.</div>';
            return;
        }

        c.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-hover align-middle ob-runs-table ob-tasks-table">' +
            '<thead><tr><th>Status</th><th>Task</th><th>Context</th><th>Type</th>' +
            '<th>From</th><th>Updated</th><th>Due</th>' +
            '<th class="text-end"><span class="visually-hidden">Actions</span></th></tr></thead>' +
            '<tbody>' + rows.map(taskRowHtml).join('') + '</tbody></table></div>';

        c.querySelectorAll('button[data-task-status]').forEach((b) => {
            b.addEventListener('click', () => onTaskStatus(b));
        });
        c.querySelectorAll('button[data-open-review]').forEach((b) => {
            b.addEventListener('click', () => openReview(b.dataset.domain, b.dataset.version));
        });
    }

    function taskRowHtml(r) {
        const dash = '<span class="text-muted">&mdash;</span>';
        const title = r.kind === 'review'
            ? '<button type="button" class="btn btn-link p-0 ob-run-link ob-tasks-title" ' +
              'data-open-review="1" data-domain="' + escAttr(r.folder) + '" ' +
              'data-version="' + escAttr(r.version) + '" title="' + escAttr(r.title) + '">' +
              esc(r.title) + '</button>' +
              '<span class="ob-tasks-meta ms-2">' + r.approvals + ' approvals</span>'
            : '<span class="ob-tasks-title fw-medium" title="' + escAttr(r.title) + '">' +
              esc(r.title) + '</span>';
        const type = r.kind === 'review'
            ? '<i class="bi bi-patch-check me-1" aria-hidden="true"></i>Review'
            : '<i class="bi bi-person-check me-1" aria-hidden="true"></i>Assigned';
        return '<tr>' +
            '<td>' + taskStatus(r.status) + '</td>' +
            '<td class="ob-tasks-title-cell">' + title + '</td>' +
            '<td>' + esc(r.folder) + ' <span class="ob-run-version">v' + esc(r.version) + '</span></td>' +
            '<td class="text-muted">' + type + '</td>' +
            '<td>' + (r.from ? esc(r.from) : dash) + '</td>' +
            '<td class="text-muted">' + (r.updated ? esc(relativeTime(r.updated)) : dash) + '</td>' +
            '<td>' + (r.due ? esc(r.due) : dash) + '</td>' +
            '<td class="text-end">' + taskActions(r) + '</td>' +
            '</tr>';
    }

    function taskActions(r) {
        if (r.kind === 'review') {
            return '<button type="button" class="btn btn-sm btn-outline-secondary" data-open-review="1" ' +
                'data-domain="' + escAttr(r.folder) + '" data-version="' + escAttr(r.version) + '">' +
                'Open</button>';
        }
        const t = r.task;
        const attrs = 'data-folder="' + escAttr(t.folder) + '" ' +
            'data-version="' + escAttr(t.version) + '" data-task-id="' + escAttr(t.id) + '"';
        const start = r.status === 'open'
            ? '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" ' +
              'data-task-status="in_progress" ' + attrs + '>' +
              '<i class="bi bi-play me-1"></i>Start</button>'
            : '';
        const done = (r.status !== 'done' && r.status !== 'cancelled')
            ? '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" ' +
              'data-task-status="done" ' + attrs + '>' +
              '<i class="bi bi-check2 me-1"></i>Done</button>'
            : '';
        return start + done;
    }

    function initTaskFilters() {
        on('domainTasksSearch', 'input', applyTaskFilters);
        ['domainTasksTypeFilter', 'domainTasksStatusFilter'].forEach((id) => {
            const btn = document.getElementById(id);
            const menu = btn && btn.parentElement.querySelector('.dropdown-menu');
            if (!menu) return;
            menu.querySelectorAll('.dropdown-item').forEach((item) => {
                item.addEventListener('click', () => {
                    const value = item.dataset.value || '';
                    btn.dataset.value = value;
                    btn.classList.toggle('is-active', !!value);
                    btn.querySelector('.ob-filter-value').textContent =
                        value ? ': ' + item.textContent : '';
                    menu.querySelectorAll('.dropdown-item')
                        .forEach((i) => i.classList.toggle('active', i === item));
                    applyTaskFilters();
                });
            });
        });
    }

    async function onTaskStatus(btn) {
        const { folder, version, taskId } = btn.dataset;
        const status = btn.dataset.taskStatus;
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(folder) + '/' +
                encodeURIComponent(version) + '/tasks/' +
                encodeURIComponent(taskId) + '/status',
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ status: status }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                note(data.message || 'Failed to update task', 'error');
                return;
            }
            note('Task marked ' + status.replace('_', ' ') + '.', 'success');
            tasksLoaded = false;
            loadTasks();
        } catch (err) {
            note('Error: ' + err.message, 'error');
        }
    }

    async function openReview(domain, version) {
        try {
            note('Opening ' + domain + ' v' + version + '…', 'info', 4000);
            const resp = await fetch('/domain/load-from-uc', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ domain: domain, version: version }),
            });
            const data = await resp.json();
            if (!data.success) {
                note('Error: ' + (data.message || 'Failed to load context'), 'error');
                return;
            }
            window.location.href = '/domain/?section=review';
        } catch (err) {
            note('Error loading context: ' + err.message, 'error');
        }
    }

    const TASK_STATUS = {
        open: ['is-pending', 'bi-circle', 'Open'],
        in_progress: ['is-running', 'bi-arrow-repeat', 'In progress'],
        done: ['is-success', 'bi-check-circle-fill', 'Done'],
        cancelled: ['is-canceled', 'bi-slash-circle', 'Cancelled'],
        draft: ['is-pending', 'bi-pencil', 'Draft'],
        'in-review': ['is-running', 'bi-hourglass-split', 'In review'],
        published: ['is-success', 'bi-check-circle-fill', 'Published'],
    };

    function taskStatus(status) {
        const s = TASK_STATUS[status] || TASK_STATUS.open;
        return '<span class="ob-run-status ' + s[0] + '">' +
            '<i class="bi ' + s[1] + '" aria-hidden="true"></i>' + esc(s[2]) + '</span>';
    }

    // ========================================================================
    // DISCUSSIONS TIMELINE
    // ========================================================================

    async function loadDiscussions() {
        const c = document.getElementById('domainDiscussionsContainer');
        if (!c) return;
        c.innerHTML = spinner('Loading discussions...');
        const dc = await resolveDomainCtx();
        if (!dc.folder || !dc.hasRegistry) {
            c.innerHTML =
                '<div class="alert alert-info small mb-0">' +
                'Save this context to the registry to start collaborating.</div>';
            return;
        }
        try {
            const url = '/comments/' + encodeURIComponent(dc.folder) + '/' +
                encodeURIComponent(dc.version);
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                c.innerHTML = errBox(data.message || 'Failed to load discussions');
                return;
            }
            allComments = data.comments || [];
            discussionsLoaded = true;
            buildTagFilter();
            renderDiscussions();
        } catch (err) {
            c.innerHTML = errBox('Network error: ' + String(err));
        }
    }

    function renderDiscussions() {
        const c = document.getElementById('domainDiscussionsContainer');
        if (!c) return;
        const showResolved = !!(document.getElementById('domainDiscShowResolved') || {}).checked;

        let items = allComments.slice();
        if (!showResolved) items = items.filter((x) => !x.resolved);
        // Tag filter (multi-select, OR semantics: keep comments carrying any
        // of the selected tags).
        if (selectedTags.size) {
            items = items.filter((x) =>
                parsedTags(x).some((t) => selectedTags.has(tagKey(t))));
        }
        // Newest first.
        items.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

        if (!items.length) {
            c.innerHTML = selectedTags.size
                ? '<div class="text-center text-muted py-5">' +
                  '<i class="bi bi-funnel d-block mb-2" style="font-size:1.8rem;"></i>' +
                  'No discussions match the selected tags.</div>'
                : '<div class="text-center text-muted py-5">' +
                  '<i class="bi bi-chat-square-dots d-block mb-2" style="font-size:1.8rem;"></i>' +
                  'No discussions yet. Open any ontology, mapping or twin page and ' +
                  'click <i class="bi bi-chat-dots"></i> to start one.</div>';
            return;
        }

        let html = '<div class="oc-timeline">';
        let lastDay = null;
        items.forEach((cm) => {
            const day = dayKey(cm.created_at);
            if (day !== lastDay) {
                html += '<div class="oc-timeline-day">' + esc(day) + '</div>';
                lastDay = day;
            }
            html += timelineEntry(cm);
        });
        html += '</div>';
        c.innerHTML = html;

        c.querySelectorAll('[data-open-thread]').forEach((row) => {
            row.addEventListener('click', () => { openThreadFor(); });
        });
    }

    function buildTagFilter() {
        const wrap = document.getElementById('domainDiscTagFilterWrap');
        const menu = document.getElementById('domainDiscTagMenu');
        if (!wrap || !menu) return;

        // Distinct tags across every comment, keyed by ref, with a label.
        const byKey = new Map();
        allComments.forEach((cm) => {
            parsedTags(cm).forEach((t) => {
                const k = tagKey(t);
                if (k && !byKey.has(k)) byKey.set(k, t.label || t.ref || k);
            });
        });

        // Forget selections whose tag no longer appears in the data.
        Array.from(selectedTags).forEach((k) => {
            if (!byKey.has(k)) selectedTags.delete(k);
        });

        if (!byKey.size) {
            wrap.style.display = 'none';
            menu.innerHTML = '';
            updateTagCount();
            return;
        }
        wrap.style.display = '';

        const entries = Array.from(byKey.entries())
            .sort((a, b) => String(a[1]).localeCompare(String(b[1])));
        let html =
            '<li class="d-flex justify-content-between align-items-center ' +
            'px-1 pb-1 mb-1 border-bottom">' +
            '<span class="small fw-medium text-muted">Filter by tag</span>' +
            '<button type="button" class="btn btn-link btn-sm p-0 ' +
            'text-decoration-none" id="domainDiscTagClear">Clear</button></li>';
        entries.forEach((e) => {
            const k = e[0];
            const checked = selectedTags.has(k) ? ' checked' : '';
            html +=
                '<li><label class="dropdown-item d-flex align-items-center ' +
                'gap-2 small">' +
                '<input class="form-check-input mt-0" type="checkbox" value="' +
                escAttr(k) + '"' + checked + '>' +
                '<span>' + esc(e[1]) + '</span></label></li>';
        });
        menu.innerHTML = html;

        menu.querySelectorAll('input[type=checkbox]').forEach((cb) => {
            cb.addEventListener('change', () => {
                if (cb.checked) selectedTags.add(cb.value);
                else selectedTags.delete(cb.value);
                updateTagCount();
                renderDiscussions();
            });
        });
        const clr = document.getElementById('domainDiscTagClear');
        if (clr) {
            clr.addEventListener('click', () => {
                selectedTags.clear();
                menu.querySelectorAll('input[type=checkbox]')
                    .forEach((cb) => { cb.checked = false; });
                updateTagCount();
                renderDiscussions();
            });
        }
        updateTagCount();
    }

    function updateTagCount() {
        const badge = document.getElementById('domainDiscTagCount');
        if (!badge) return;
        if (selectedTags.size) {
            badge.textContent = String(selectedTags.size);
            badge.classList.remove('d-none');
        } else {
            badge.classList.add('d-none');
        }
    }

    function timelineEntry(cm) {
        const parsed = (window.OntoComments && OntoComments.parseBody)
            ? OntoComments.parseBody(cm.body)
            : { text: cm.body || '', tags: [] };
        const author = cm.author || 'unknown';
        const initials = author.replace(/@.*/, '').slice(0, 2).toUpperCase();
        const isReply = !!cm.parent_id;

        const tagsHtml = (parsed.tags || []).map((t) =>
            '<span class="badge oc-tag-chip border me-1"><i class="bi bi-tag me-1"></i>' +
            esc(t.label || t.ref) + '</span>').join('');

        return '<div class="oc-tl-item" data-open-thread="1" ' +
            'title="Open this thread">' +
            '<div class="oc-tl-marker"><span class="oc-avatar">' + esc(initials) + '</span></div>' +
            '<div class="oc-tl-body">' +
            '<div class="oc-tl-head">' +
            '<span class="oc-author">' + esc(author) + '</span>' +
            (isReply ? '<span class="badge bg-light text-muted border ms-2"><i class="bi bi-reply me-1"></i>reply</span>' : '') +
            (cm.resolved ? '<span class="badge bg-success-subtle text-success border ms-2">Resolved</span>' : '') +
            '<span class="oc-time ms-auto">' + esc(relativeTime(cm.created_at)) + '</span>' +
            '</div>' +
            '<div class="oc-tl-text oc-md">' + renderMarkdown(parsed.text) + '</div>' +
            (tagsHtml ? '<div class="oc-bubble-tags mt-1">' + tagsHtml + '</div>' : '') +
            '</div></div>';
    }

    function openThreadFor() {
        if (!window.OntoComments || !domainCtx) return;
        OntoComments.openThread({
            folder: domainCtx.folder,
            version: domainCtx.version,
        });
    }

    // Global entry point so any domain-page template can call it via onclick.
    // Shows the panel immediately with a spinner, resolves context behind it.
    window.openDomainDiscussion = function () {
        if (window.OntoComments) OntoComments.showLoadingPanel();
        resolveDomainCtx().then(function (dc) {
            if (!window.OntoComments) return;
            if (!dc.folder || !dc.hasRegistry) {
                const panelEl = document.querySelector('.oc-comments [data-oc-list]');
                if (panelEl) {
                    panelEl.innerHTML =
                        '<div class="alert alert-warning small mb-0">' +
                        'Save this context to the registry to start a discussion.</div>';
                }
                return;
            }
            OntoComments.openThread({ folder: dc.folder, version: dc.version });
        });
    };

    // ---- tiny html helpers --------------------------------------------------

    function spinner(label) {
        return '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> ' + esc(label) + '</div>';
    }

    function errBox(msg) {
        return '<div class="alert alert-danger small mb-0">' + esc(msg) + '</div>';
    }
})();
