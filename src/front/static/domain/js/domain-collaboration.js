/**
 * Domain → Collaboration
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

    // Render a comment body's markdown to HTML (same approach as the Discussion
    // panel): use the global `marked` (loaded in base.html), with an escaped
    // text + <br> fallback when it isn't available.
    function renderMarkdown(text) {
        const src = text || '';
        if (typeof window.marked !== 'undefined' && window.marked.parse) {
            try {
                window.marked.setOptions({ breaks: true, gfm: true });
                return window.marked.parse(src);
            } catch (e) { /* fall through to plain text */ }
        }
        return esc(src).replace(/\n/g, '<br>');
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
        return d.toLocaleDateString();
    }

    function dayKey(iso) {
        const d = new Date(iso);
        if (isNaN(d.getTime())) return 'Unknown date';
        const today = new Date();
        const yest = new Date();
        yest.setDate(today.getDate() - 1);
        if (d.toDateString() === today.toDateString()) return 'Today';
        if (d.toDateString() === yest.toDateString()) return 'Yesterday';
        return d.toLocaleDateString(undefined, {
            weekday: 'short', year: 'numeric', month: 'short', day: 'numeric',
        });
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
        const c = document.getElementById('domainMyTasksContainer');
        if (!c) return;

        if (!reviewTasks.length && !assigned.length) {
            c.innerHTML =
                '<div class="text-center text-muted py-5">' +
                '<i class="bi bi-check2-circle d-block mb-2" style="font-size:1.8rem;"></i>' +
                "You're all caught up &mdash; no pending tasks.</div>";
            return;
        }
        c.innerHTML = assignedHtml(assigned) + reviewHtml(reviewTasks);
        c.querySelectorAll('button[data-task-status]').forEach((b) => {
            b.addEventListener('click', () => onTaskStatus(b));
        });
        c.querySelectorAll('button[data-open-review]').forEach((b) => {
            b.addEventListener('click', () => openReview(b.dataset.domain, b.dataset.version));
        });
    }

    function assignedHtml(tasks) {
        if (!tasks.length) return '';
        const rows = tasks.map((t) =>
            '<tr>' +
            '<td class="fw-medium">' + esc(t.title) + '</td>' +
            '<td>' + esc(t.folder) + ' v' + esc(t.version) + '</td>' +
            '<td>' + esc(t.created_by) + '</td>' +
            '<td>' + taskStatusBadge(t.status) + '</td>' +
            '<td>' + (t.due_date ? esc(t.due_date) : '<span class="text-muted small">&mdash;</span>') + '</td>' +
            '<td class="text-end">' + taskActions(t) + '</td>' +
            '</tr>'
        ).join('');
        return '<div class="mb-2 fw-medium small text-uppercase text-muted">' +
            '<i class="bi bi-person-check me-1"></i>Assigned to me</div>' +
            '<div class="table-responsive mb-4"><table class="table table-sm align-middle mb-0">' +
            '<thead><tr><th>Task</th><th>Domain</th><th>From</th><th>Status</th>' +
            '<th>Due</th><th class="text-end">Action</th></tr></thead>' +
            '<tbody>' + rows + '</tbody></table></div>';
    }

    function reviewHtml(tasks) {
        if (!tasks.length) return '';
        const rows = tasks.map((t) =>
            '<tr>' +
            '<td class="fw-medium">' + esc(t.domain) + '</td>' +
            '<td>v' + esc(t.version) + '</td>' +
            '<td>' + statusBadge(t.status) + '</td>' +
            '<td><span class="text-muted small">' + esc(t.approvals) + ' / ' + esc(t.required) + '</span></td>' +
            '<td><span class="text-muted small">' + esc(relativeTime(t.last_activity)) + '</span></td>' +
            '<td class="text-end">' +
            '<button type="button" class="btn btn-sm btn-primary" data-open-review="1" ' +
            'data-domain="' + escAttr(t.domain) + '" data-version="' + escAttr(t.version) + '">' +
            '<i class="bi bi-patch-check me-1"></i>Review</button></td>' +
            '</tr>'
        ).join('');
        return '<div class="mb-2 fw-medium small text-uppercase text-muted">' +
            '<i class="bi bi-ui-checks me-1"></i>Review worklist</div>' +
            '<div class="table-responsive mb-2"><table class="table table-sm align-middle mb-0">' +
            '<thead><tr><th>Domain</th><th>Version</th><th>Status</th>' +
            '<th>Approvals</th><th>Last activity</th><th class="text-end">Your action</th></tr></thead>' +
            '<tbody>' + rows + '</tbody></table></div>';
    }

    function taskActions(t) {
        const start = t.status === 'open'
            ? '<button type="button" class="btn btn-sm btn-outline-secondary ms-1" ' +
              'data-task-status="in_progress" data-folder="' + escAttr(t.folder) + '" ' +
              'data-version="' + escAttr(t.version) + '" data-task-id="' + escAttr(t.id) + '">' +
              '<i class="bi bi-play me-1"></i>Start</button>'
            : '';
        const done = (t.status !== 'done' && t.status !== 'cancelled')
            ? '<button type="button" class="btn btn-sm btn-success ms-1" ' +
              'data-task-status="done" data-folder="' + escAttr(t.folder) + '" ' +
              'data-version="' + escAttr(t.version) + '" data-task-id="' + escAttr(t.id) + '">' +
              '<i class="bi bi-check2 me-1"></i>Done</button>'
            : '';
        return start + ' ' + done;
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
                note('Error: ' + (data.message || 'Failed to load domain'), 'error');
                return;
            }
            window.location.href = '/domain/?section=review';
        } catch (err) {
            note('Error loading domain: ' + err.message, 'error');
        }
    }

    function taskStatusBadge(status) {
        const map = {
            open: 'bg-secondary-subtle text-dark border',
            in_progress: 'bg-info-subtle text-dark border-info',
            done: 'bg-success-subtle text-dark border-success',
            cancelled: 'bg-light text-muted border',
        };
        const cls = map[status] || map.open;
        return '<span class="badge ' + cls + '">' + esc((status || 'open').replace('_', ' ')) + '</span>';
    }

    function statusBadge(status) {
        const map = {
            'DRAFT': 'bg-warning-subtle text-dark border-warning',
            'IN-REVIEW': 'bg-info-subtle text-dark border-info',
            'PUBLISHED': 'bg-success-subtle text-dark border-success',
        };
        const cls = map[status] || map['DRAFT'];
        const label = status === 'IN-REVIEW' ? 'In Review'
            : ((status || 'DRAFT').charAt(0) + (status || 'DRAFT').slice(1).toLowerCase());
        return '<span class="badge border ' + cls + '">' + esc(label) + '</span>';
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
                'Save this domain to the registry to start collaborating.</div>';
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
                        'Save this domain to the registry to start a discussion.</div>';
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
