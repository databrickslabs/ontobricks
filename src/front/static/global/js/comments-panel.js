/**
 * Collaborative comments & tasks — domain discussion panel (global)
 *
 * A reusable right-side offcanvas that opens the single domain-wide,
 * threaded discussion for ``(folder, version)``. Any surface can open it
 * through the global API:
 *
 *   OntoComments.openThread({ folder, version });
 *
 * Backed by the /comments API (see CommentService). A comment can be
 * turned into a task assigned to a teammate; the assignee picker is loaded
 * from the domain access roster (/review/<folder>/<version>/team).
 *
 * Depends on Bootstrap 5 (Offcanvas) and the global escapeHtml in
 * utils.js (falls back to a local implementation when absent).
 */
(function () {
    'use strict';

    let el = null;
    let offcanvas = null;
    let ctx = null;          // { folder, version }
    let membersCache = {};   // key folder/version -> [members]
    let currentUser = null;  // current user's email/principal (for "Assign to me")
    let currentUserPromise = null;
    let lastListSig = '';    // signature of the last rendered comment set

    function esc(text) {
        if (typeof window.escapeHtml === 'function') return window.escapeHtml(text);
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    function escAttr(text) {
        return esc(text).replace(/"/g, '&quot;');
    }

    // Render a comment body's markdown to HTML. Uses `marked` (loaded globally
    // in base.html, same as the ontology chat assistant); falls back to escaped
    // text with <br> when it isn't available.
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

    function notify(msg, kind) {
        if (typeof window.showNotification === 'function') {
            window.showNotification(msg, kind || 'info');
        }
    }

    function build() {
        if (el) return;
        el = document.createElement('div');
        el.className = 'offcanvas offcanvas-end oc-comments';
        el.tabIndex = -1;
        el.setAttribute('aria-labelledby', 'ocCommentsTitle');
        el.style.width = '460px';
        el.innerHTML =
            '<div class="offcanvas-header border-bottom align-items-start">' +
            '<div class="flex-grow-1 me-2">' +
            '<h6 class="offcanvas-title mb-0" id="ocCommentsTitle">' +
            '<i class="bi bi-chat-dots me-2"></i>Discussion</h6>' +
            '<div class="small text-muted" data-oc-anchor></div>' +
            '</div>' +
            '<button type="button" class="btn btn-sm btn-outline-success me-2" ' +
            'data-oc-new-task title="Create a task">' +
            '<i class="bi bi-check2-square me-1"></i>New task</button>' +
            '<button type="button" class="btn-close" data-bs-dismiss="offcanvas" aria-label="Close"></button>' +
            '</div>' +
            '<div class="offcanvas-body d-flex flex-column p-0">' +
            '<div class="oc-newtask-box border-bottom p-3 d-none" data-oc-newtask></div>' +
            '<div class="oc-comments-list flex-grow-1 p-3" data-oc-list></div>' +
            '<div class="oc-comments-compose border-top p-3" data-oc-compose>' +
            '<textarea class="form-control form-control-sm mb-2" rows="2" ' +
            'data-oc-input placeholder="Write a comment..."></textarea>' +
            '<div class="d-flex justify-content-end">' +
            '<button type="button" class="btn btn-sm btn-primary" data-oc-send>' +
            '<i class="bi bi-send me-1"></i>Comment</button>' +
            '</div></div>' +
            '</div>';
        document.body.appendChild(el);

        const compose = el.querySelector('[data-oc-compose]');
        el.querySelector('[data-oc-send]').addEventListener('click', () => {
            const ta = compose.querySelector('[data-oc-input]');
            postComment((ta.value || '').trim(), null, ta, compose);
        });
        el.querySelector('[data-oc-new-task]').addEventListener('click', openNewTask);
        // No-op: polling was removed; handler kept for future cleanup hooks.
        el.addEventListener('hidden.bs.offcanvas', () => {});
    }

    // Header subtitle: discussions are domain-wide, so just name the domain
    // (no per-anchor "kind" badge that would separate threads by selection).
    function renderAnchorBadge() {
        const label = ctx.folder
            ? esc(ctx.folder) + ' · v' + esc(ctx.version)
            : '';
        el.querySelector('[data-oc-anchor]').innerHTML = label;
    }

    // ---- Tags (legacy) ------------------------------------------------------
    // The entity/relationship tag picker has been removed — discussions are
    // domain-wide and untagged. We keep the marker + parser/renderer so older
    // comments that embedded tags still display their chips (read-only).
    const TAG_MARK = '\n\n[[onto-tags]]';

    function parseBody(body) {
        const raw = body || '';
        const idx = raw.indexOf(TAG_MARK);
        if (idx === -1) return { text: raw, tags: [] };
        let tags = [];
        try { tags = JSON.parse(raw.slice(idx + TAG_MARK.length)) || []; }
        catch (e) { tags = []; }
        return { text: raw.slice(0, idx), tags: tags };
    }

    function tagsHtml(tags) {
        if (!tags || !tags.length) return '';
        return '<div class="oc-bubble-tags mt-1">' + tags.map((t) =>
            '<span class="badge oc-tag-chip border me-1"><i class="bi bi-tag me-1"></i>' +
            esc(t.label || t.ref) + '</span>').join('') + '</div>';
    }

    // Open the offcanvas immediately with a spinner so the panel appears on
    // click without waiting for any async work. Called by entry points that
    // need to resolve domain context before they can call openThread.
    function showLoadingPanel() {
        build();
        const list = el.querySelector('[data-oc-list]');
        if (list) {
            list.innerHTML =
                '<div class="text-center text-muted small py-4">' +
                '<span class="spinner-border spinner-border-sm me-1"></span> Loading...</div>';
        }
        const anchor = el.querySelector('[data-oc-anchor]');
        if (anchor) anchor.innerHTML = '';
        if (window.bootstrap) {
            offcanvas = bootstrap.Offcanvas.getOrCreateInstance(el);
            offcanvas.show();
            setTimeout(() => {
                document.querySelectorAll('.offcanvas-backdrop.show')
                    .forEach((b) => b.classList.add('oc-comments-backdrop'));
            }, 0);
        }
    }

    async function openThread(opts) {
        opts = opts || {};
        if (!opts.folder || !opts.version) {
            notify('Cannot open discussion: missing domain/version', 'error');
            return;
        }
        build();
        lastListSig = '';
        ctx = {
            folder: opts.folder,
            version: opts.version,
        };
        renderAnchorBadge();

        if (window.bootstrap) {
            offcanvas = bootstrap.Offcanvas.getOrCreateInstance(el);
            // Only call show() if not already visible (showLoadingPanel may
            // have already opened it, avoiding a flicker/re-animation).
            if (!el.classList.contains('show')) {
                offcanvas.show();
                setTimeout(() => {
                    document.querySelectorAll('.offcanvas-backdrop.show')
                        .forEach((b) => b.classList.add('oc-comments-backdrop'));
                }, 0);
            }
        }
        await reload();
        loadMembers();
        loadCurrentUser();
    }

    async function reload() {
        const list = el.querySelector('[data-oc-list]');
        list.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading...</div>';
        const url = '/comments/' + encodeURIComponent(ctx.folder) + '/' +
            encodeURIComponent(ctx.version);
        try {
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                list.innerHTML = '<div class="alert alert-danger small mb-0">' +
                    esc(data.message || 'Failed to load comments') + '</div>';
                return;
            }
            const comments = data.comments || [];
            renderList(list, comments);
            lastListSig = listSignature(comments);
        } catch (err) {
            list.innerHTML = '<div class="alert alert-danger small mb-0">Network error: ' +
                esc(String(err)) + '</div>';
        }
    }

    // Resolve the signed-in user once (for the "Assign to me" shortcut).
    function loadCurrentUser() {
        if (currentUserPromise) return currentUserPromise;
        currentUserPromise = fetch('/domain/current-user', { credentials: 'same-origin' })
            .then((r) => r.json())
            .then((d) => {
                currentUser = (d && d.success && d.email) ? d.email : null;
                return currentUser;
            })
            .catch(() => { currentUser = null; return null; });
        return currentUserPromise;
    }

    async function loadMembers() {
        const key = ctx.folder + '/' + ctx.version;
        if (membersCache[key]) return;
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(ctx.folder) + '/' +
                encodeURIComponent(ctx.version) + '/assignees',
                { credentials: 'same-origin' }
            );
            const data = await resp.json();
            membersCache[key] = (resp.ok && data.success && data.members)
                ? data.members : [];
        } catch (err) {
            membersCache[key] = [];
        }
    }

    // True while the user is actively typing a reply somewhere in the panel.
    function userIsComposing() {
        if (!el) return false;
        const active = document.activeElement;
        if (active && el.contains(active) && active.tagName === 'TEXTAREA') return true;
        return Array.from(el.querySelectorAll('textarea'))
            .some((t) => (t.value || '').trim().length > 0);
    }

    // Cheap change-detector for the rendered comment set.
    function listSignature(comments) {
        const base = comments.map((c) => c.id + ':' + (c.created_at || '')).join('|');
        return comments.length + '#' + base;
    }

    function renderList(list, comments) {
        if (!comments.length) {
            list.innerHTML =
                '<div class="text-center text-muted py-4">' +
                '<i class="bi bi-chat-square-dots d-block mb-2" style="font-size:1.6rem;"></i>' +
                'No comments yet. Start the discussion.</div>';
            return;
        }
        // Build a parent -> replies map; root comments keep document order.
        const roots = [];
        const replies = {};
        comments.forEach((c) => {
            if (c.parent_id) {
                (replies[c.parent_id] = replies[c.parent_id] || []).push(c);
            } else {
                roots.push(c);
            }
        });
        list.innerHTML = roots.map((r) => threadHtml(r, replies[r.id] || [])).join('');
        bindThreadActions(list);
        list.scrollTop = list.scrollHeight;
    }

    function threadHtml(root, replies) {
        const replyHtml = replies.map((r) => bubble(r, true)).join('');
        const resolvedCls = root.resolved ? ' oc-resolved' : '';
        const rawText = parseBody(root.body).text;
        return '<div class="oc-thread' + resolvedCls + '" data-thread="' + escAttr(root.id) + '"' +
            ' data-raw-text="' + escAttr(rawText) + '">' +
            bubble(root, false) +
            '<div class="oc-replies">' + replyHtml + '</div>' +
            '<div class="oc-thread-tools">' +
            '<button type="button" class="btn btn-link btn-sm p-0 me-3" data-reply="' + escAttr(root.id) + '">' +
            '<i class="bi bi-reply me-1"></i>Reply</button>' +
            '<button type="button" class="btn btn-link btn-sm p-0 me-3" data-task="' + escAttr(root.id) + '">' +
            '<i class="bi bi-check2-square me-1"></i>Create task</button>' +
            '<button type="button" class="btn btn-link btn-sm p-0 text-muted" data-resolve="' + escAttr(root.id) + '" ' +
            'data-resolved="' + (root.resolved ? '1' : '0') + '">' +
            '<i class="bi bi-' + (root.resolved ? 'arrow-counterclockwise' : 'check-circle') + ' me-1"></i>' +
            (root.resolved ? 'Reopen' : 'Resolve') + '</button>' +
            '</div>' +
            '<div class="oc-reply-box" data-reply-box="' + escAttr(root.id) + '" style="display:none;"></div>' +
            '<div class="oc-task-box" data-task-box="' + escAttr(root.id) + '" style="display:none;"></div>' +
            '</div>';
    }

    function bubble(c, isReply) {
        const actor = c.author || 'unknown';
        const initials = actor.replace(/@.*/, '').slice(0, 2).toUpperCase();
        const parsed = parseBody(c.body);
        return '<div class="oc-bubble' + (isReply ? ' oc-bubble-reply' : '') + '">' +
            '<div class="oc-avatar">' + esc(initials) + '</div>' +
            '<div class="oc-bubble-body">' +
            '<div class="oc-bubble-head">' +
            '<span class="oc-author">' + esc(actor) + '</span>' +
            '<span class="oc-time">' + formatTime(c.created_at) + '</span>' +
            (c.resolved && !isReply ? '<span class="badge bg-success-subtle text-success border ms-2">Resolved</span>' : '') +
            '</div>' +
            '<div class="oc-text oc-md">' + renderMarkdown(parsed.text) + '</div>' +
            tagsHtml(parsed.tags) +
            '</div></div>';
    }

    function bindThreadActions(list) {
        list.querySelectorAll('button[data-reply]').forEach((btn) => {
            btn.addEventListener('click', () => toggleReply(btn.dataset.reply));
        });
        list.querySelectorAll('button[data-resolve]').forEach((btn) => {
            btn.addEventListener('click', () => {
                resolveThread(btn.dataset.resolve, btn.dataset.resolved !== '1');
            });
        });
        list.querySelectorAll('button[data-task]').forEach((btn) => {
            btn.addEventListener('click', () => toggleTask(btn.dataset.task));
        });
    }

    function toggleReply(rootId) {
        const box = el.querySelector('[data-reply-box="' + cssEsc(rootId) + '"]');
        if (!box) return;
        if (box.style.display !== 'none') { box.style.display = 'none'; return; }
        box.style.display = '';
        box.innerHTML =
            '<textarea class="form-control form-control-sm mb-2" rows="2" ' +
            'placeholder="Write a reply..."></textarea>' +
            '<div class="d-flex justify-content-end">' +
            '<button type="button" class="btn btn-sm btn-outline-primary">Reply</button></div>';
        const ta = box.querySelector('textarea');
        box.querySelector('button').addEventListener('click', () => {
            postComment((ta.value || '').trim(), rootId, ta, box);
        });
        ta.focus();
    }

    // Build the inner markup of a task-creation form. Shared by the
    // per-comment task box and the standalone "New task" box in the header.
    const TASK_TITLE_MAX = 200;

    function taskFormHtml(heading, withCancel, commentText) {
        const members = membersCache[ctx.folder + '/' + ctx.version] || [];
        const opts = members.map((m) => {
            const label = esc(m.display_name || m.principal) +
                (m.principal === currentUser ? ' (me)' : '') +
                ' (' + esc(m.role) + ')';
            return '<option value="' + escAttr(m.principal) + '">' + label + '</option>';
        }).join('');
        const cancel = withCancel
            ? '<button type="button" class="btn btn-sm btn-outline-secondary" data-tk-cancel>Cancel</button>'
            : '';
        const copyBtn = commentText
            ? '<button type="button" class="btn btn-link btn-sm p-0 ms-1 text-muted" data-tk-copy ' +
              'title="Copy comment text as task name" ' +
              'data-tk-copy-text="' + escAttr(commentText) + '">' +
              '<i class="bi bi-clipboard me-1"></i>Copy from comment</button>'
            : '';
        return '<div class="oc-task-form border rounded p-2">' +
            '<div class="small fw-medium mb-2"><i class="bi bi-check2-square me-1"></i>' +
            esc(heading) + '</div>' +
            '<div class="d-flex align-items-center gap-1 mb-2">' +
            '<input type="text" class="form-control form-control-sm flex-grow-1" data-tk-title placeholder="Task title">' +
            copyBtn + '</div>' +
            '<div class="d-flex align-items-center justify-content-between mb-1">' +
            '<label class="form-label small text-muted mb-0">Assignee</label>' +
            '<button type="button" class="btn btn-link btn-sm p-0" data-tk-me>' +
            '<i class="bi bi-person-check me-1"></i>Assign to me</button>' +
            '</div>' +
            '<select class="form-select form-select-sm mb-2" data-tk-assignee>' +
            '<option value="">Assign to...</option>' + opts + '</select>' +
            '<input type="date" class="form-control form-control-sm mb-2" data-tk-due title="Due date (optional)">' +
            '<div class="d-flex justify-content-end gap-2">' + cancel +
            '<button type="button" class="btn btn-sm btn-success" data-tk-create>Create task</button>' +
            '</div></div>';
    }

    function wireTaskForm(box, commentId) {
        box.querySelector('[data-tk-me]').addEventListener('click', () => {
            assignToMe(box);
        });
        box.querySelector('[data-tk-create]').addEventListener('click', () => {
            createTask(commentId, box);
        });
        const cancel = box.querySelector('[data-tk-cancel]');
        if (cancel) cancel.addEventListener('click', () => hideTaskBox(box));
        const sel = box.querySelector('[data-tk-assignee]');
        if (sel) sel.addEventListener('change', () => syncDueVisibility(box));
        const copyBtn = box.querySelector('[data-tk-copy]');
        if (copyBtn) {
            copyBtn.addEventListener('click', () => {
                const raw = copyBtn.dataset.tkCopyText || '';
                const truncated = raw.length > TASK_TITLE_MAX
                    ? raw.slice(0, TASK_TITLE_MAX) + '…'
                    : raw;
                const titleInput = box.querySelector('[data-tk-title]');
                if (titleInput) {
                    titleInput.value = truncated;
                    titleInput.focus();
                }
            });
        }
        syncDueVisibility(box);
    }

    function syncDueVisibility(box) {
        // no-op: kept as hook for future visibility rules
    }

    function hideTaskBox(box) {
        box.innerHTML = '';
        box.style.display = 'none';
        box.classList.add('d-none');
    }

    function toggleTask(rootId) {
        const box = el.querySelector('[data-task-box="' + cssEsc(rootId) + '"]');
        if (!box) return;
        if (box.style.display !== 'none') { box.style.display = 'none'; return; }
        box.classList.remove('d-none');
        box.style.display = '';
        // Read the raw comment text stored on the parent thread element.
        const thread = el.querySelector('[data-thread="' + cssEsc(rootId) + '"]');
        const commentText = (thread && thread.dataset.rawText) || '';
        box.innerHTML = taskFormHtml('New task from this comment', false, commentText);
        wireTaskForm(box, rootId);
    }

    // Standalone task creation (not tied to a comment), opened from the panel header.
    async function openNewTask() {
        const box = el.querySelector('[data-oc-newtask]');
        if (!box) return;
        if (!box.classList.contains('d-none')) { hideTaskBox(box); return; }
        await loadMembers();
        box.classList.remove('d-none');
        box.style.display = '';
        box.innerHTML = taskFormHtml('New task', true);
        wireTaskForm(box, null);
    }

    // Select the current user in the assignee picker, adding an option for
    // them when they are not already in the roster.
    async function assignToMe(box) {
        const me = await loadCurrentUser();
        if (!me) { notify('Could not determine the current user', 'warning'); return; }
        const sel = box.querySelector('[data-tk-assignee]');
        if (!sel) return;
        const exists = Array.from(sel.options).some((o) => o.value === me);
        if (!exists) {
            const opt = document.createElement('option');
            opt.value = me;
            opt.textContent = me + ' (me)';
            sel.appendChild(opt);
        }
        sel.value = me;
        syncDueVisibility(box);
    }

    async function postComment(body, parentId, ta, scope) {
        if (!body) { notify('Write something first', 'warning'); return; }
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(ctx.folder) + '/' +
                encodeURIComponent(ctx.version),
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        body: body,
                        parent_id: parentId || null,
                    }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                notify(data.message || 'Failed to post comment', 'error');
                return;
            }
            if (ta) ta.value = '';
            await reload();
        } catch (err) {
            notify('Error: ' + err.message, 'error');
        }
    }

    async function resolveThread(rootId, resolved) {
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(ctx.folder) + '/' +
                encodeURIComponent(ctx.version) + '/' +
                encodeURIComponent(rootId) + '/resolve',
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ resolved: resolved }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                notify(data.message || 'Failed to update comment', 'error');
                return;
            }
            await reload();
        } catch (err) {
            notify('Error: ' + err.message, 'error');
        }
    }

    async function createTask(commentId, box) {
        const title = (box.querySelector('[data-tk-title]').value || '').trim();
        const assignee = box.querySelector('[data-tk-assignee]').value || '';
        const due = box.querySelector('[data-tk-due]').value || '';
        if (!title) { notify('Task title is required', 'warning'); return; }
        if (!assignee) { notify('Pick an assignee', 'warning'); return; }
        try {
            const resp = await fetch(
                '/comments/' + encodeURIComponent(ctx.folder) + '/' +
                encodeURIComponent(ctx.version) + '/tasks',
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        assignee: assignee,
                        title: title,
                        due_date: due || null,
                        comment_id: commentId,
                    }),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                notify(data.message || 'Failed to create task', 'error');
                return;
            }
            notify('Task assigned to ' + assignee, 'success');
            hideTaskBox(box);
            await reload();
        } catch (err) {
            notify('Error: ' + err.message, 'error');
        }
    }

    function formatTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        return esc(d.toLocaleString());
    }

    function cssEsc(s) {
        if (window.CSS && CSS.escape) return CSS.escape(s);
        return String(s).replace(/"/g, '\\"');
    }

    // Resolve the loaded domain folder + version once, then cache. Used by
    // the editor surfaces (ontology / mapping / graph) which always operate
    // on the loaded session domain, so they don't carry folder/version.
    let _ctxPromise = null;
    function resolveDomainContext() {
        if (_ctxPromise) return _ctxPromise;
        _ctxPromise = fetch('/domain/version-status', { credentials: 'same-origin' })
            .then((r) => r.json())
            .then((vs) => ({
                folder: vs.domain_folder || '',
                version: vs.version || '',
                hasRegistry: !!vs.has_registry,
            }))
            .catch(() => ({ folder: '', version: '', hasRegistry: false }));
        return _ctxPromise;
    }

    /**
     * Build the comment tag vocabulary ({type, ref, label}[]) from an
     * ontology config ({ classes, properties }). Shared by every surface
     * (ontology designer, mapping, graph viewer) so the entity/relationship
     * tag picker is built identically everywhere.
     */
    function taggableFromOntology(config) {
        const cfg = config || {};
        const out = [];
        (cfg.classes || []).forEach((c) => out.push({
            type: 'ontology_class',
            ref: c.uri || c.name,
            label: (c.emoji || '🔷') + ' ' + (c.name || c.uri),
        }));
        (cfg.properties || []).forEach((p) => out.push({
            type: 'ontology_property',
            ref: p.uri || p.name,
            label: '🔗 ' + (p.name || p.uri),
        }));
        return out;
    }

    /**
     * Open the domain discussion from any editor surface (ontology / mapping
     * / graph), auto-resolving the loaded domain + version. Legacy anchor
     * arguments are accepted for backward compatibility but ignored —
     * discussions are domain-wide.
     */
    async function openForSelection() {
        // Open the panel immediately — the spinner is visible while we resolve
        // the domain context and fetch comments asynchronously.
        showLoadingPanel();
        const dc = await resolveDomainContext();
        if (!dc.folder || !dc.hasRegistry) {
            const list = el.querySelector('[data-oc-list]');
            if (list) {
                list.innerHTML =
                    '<div class="alert alert-warning small mb-0">' +
                    'Save this domain to the registry to start a discussion.</div>';
            }
            return;
        }
        lastListSig = '';
        ctx = { folder: dc.folder, version: dc.version };
        renderAnchorBadge();
        await reload();
        loadMembers();
        loadCurrentUser();
    }

    window.OntoComments = {
        openThread: openThread,
        openForSelection: openForSelection,
        showLoadingPanel: showLoadingPanel,
        taggableFromOntology: taggableFromOntology,
        // Split a stored comment body into { text, tags } (strips the
        // internal tag marker). Shared with the Domain → Discussions timeline.
        parseBody: parseBody,
    };
})();
