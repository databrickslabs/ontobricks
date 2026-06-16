/**
 * Shared review modals (global)
 *
 * Two reusable popups for the ontology review/validation workflow,
 * available on every page via base.html:
 *
 *   ReviewModals.promptComment(opts) -> Promise<{confirmed, comment}>
 *       A confirm dialog with an attached comment textarea. Used for
 *       every status switch (submit / approve / request-changes /
 *       publish / reopen) so each decision can carry a note that is
 *       persisted to the audit trail.
 *
 *   ReviewModals.showComments(folder, version, opts) -> Promise<void>
 *       A chat-style popup listing the full decision history (every
 *       review event, comment included) for a (domain, version).
 *
 * Modal DOM is created lazily and reused. Depends on Bootstrap 5 (loaded
 * globally) and the global escapeHtml in utils.js (falls back to a local
 * implementation when absent).
 */
(function () {
    'use strict';

    const ACTION_META = {
        submitted: { icon: 'eye', cls: 'text-info', label: 'Submitted for review' },
        approved: { icon: 'hand-thumbs-up', cls: 'text-success', label: 'Approved' },
        changes_requested: { icon: 'arrow-counterclockwise', cls: 'text-danger', label: 'Changes requested' },
        published: { icon: 'broadcast', cls: 'text-success', label: 'Published' },
        reopened: { icon: 'unlock', cls: 'text-secondary', label: 'Reopened' },
        commented: { icon: 'chat-left-text', cls: 'text-muted', label: 'Comment' },
    };

    let promptEl = null;
    let promptModal = null;
    let promptResolve = null;

    let commentsEl = null;
    let commentsModal = null;

    function esc(text) {
        if (typeof window.escapeHtml === 'function') return window.escapeHtml(text);
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    /* ── Comment prompt ─────────────────────────────── */
    function buildPrompt() {
        if (promptEl) return;
        promptEl = document.createElement('div');
        promptEl.className = 'modal fade';
        promptEl.tabIndex = -1;
        promptEl.setAttribute('aria-hidden', 'true');
        promptEl.innerHTML =
            '<div class="modal-dialog"><div class="modal-content">' +
            '<div class="modal-header">' +
            '<h5 class="modal-title"><i class="bi bi-chat-left-text me-2" data-rm-titleicon></i>' +
            '<span data-rm-title>Add a comment</span></h5>' +
            '<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>' +
            '</div>' +
            '<div class="modal-body">' +
            '<p class="text-muted small" data-rm-message></p>' +
            '<div class="mb-1">' +
            '<label class="form-label small fw-medium" data-rm-label>Comment <span class="text-muted">(optional)</span></label>' +
            '<textarea class="form-control" data-rm-comment rows="3" placeholder="Add a note for the audit trail..."></textarea>' +
            '<div class="invalid-feedback" data-rm-required>A comment is required.</div>' +
            '</div></div>' +
            '<div class="modal-footer">' +
            '<button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Cancel</button>' +
            '<button type="button" class="btn btn-primary" data-rm-confirm>Confirm</button>' +
            '</div></div></div>';
        document.body.appendChild(promptEl);

        promptEl.querySelector('[data-rm-confirm]').addEventListener('click', onConfirm);
        promptEl.addEventListener('hidden.bs.modal', () => {
            if (promptResolve) {
                promptResolve({ confirmed: false, comment: '' });
                promptResolve = null;
            }
        });
    }

    function onConfirm() {
        const ta = promptEl.querySelector('[data-rm-comment]');
        const comment = (ta.value || '').trim();
        if (promptEl._requireComment && !comment) {
            ta.classList.add('is-invalid');
            ta.focus();
            return;
        }
        const resolve = promptResolve;
        promptResolve = null;
        promptModal.hide();
        if (resolve) resolve({ confirmed: true, comment: comment });
    }

    function promptComment(opts) {
        opts = opts || {};
        buildPrompt();
        promptEl.querySelector('[data-rm-title]').textContent = opts.title || 'Add a comment';
        const ticon = promptEl.querySelector('[data-rm-titleicon]');
        ticon.className = 'bi bi-' + (opts.icon || 'chat-left-text') + ' me-2';
        promptEl.querySelector('[data-rm-message]').innerHTML = opts.message || '';
        promptEl.querySelector('[data-rm-label]').innerHTML = opts.commentLabel ||
            ('Comment <span class="text-muted">(' +
                (opts.requireComment ? 'required' : 'optional') + ')</span>');
        const ta = promptEl.querySelector('[data-rm-comment]');
        ta.value = '';
        ta.classList.remove('is-invalid');
        ta.placeholder = opts.placeholder || 'Add a note for the audit trail...';
        const confirmBtn = promptEl.querySelector('[data-rm-confirm]');
        confirmBtn.className = 'btn ' + (opts.confirmClass || 'btn-primary');
        confirmBtn.textContent = opts.confirmText || 'Confirm';
        promptEl._requireComment = !!opts.requireComment;

        if (window.bootstrap) {
            promptModal = bootstrap.Modal.getOrCreateInstance(promptEl);
        }
        return new Promise((resolve) => {
            promptResolve = resolve;
            promptModal ? promptModal.show() : resolve({ confirmed: false, comment: '' });
            setTimeout(() => ta.focus(), 250);
        });
    }

    /* ── Comments chat ──────────────────────────────── */
    function buildComments() {
        if (commentsEl) return;
        commentsEl = document.createElement('div');
        commentsEl.className = 'modal fade';
        commentsEl.tabIndex = -1;
        commentsEl.setAttribute('aria-hidden', 'true');
        commentsEl.innerHTML =
            '<div class="modal-dialog modal-dialog-scrollable"><div class="modal-content">' +
            '<div class="modal-header">' +
            '<h5 class="modal-title"><i class="bi bi-chat-dots me-2"></i>' +
            '<span data-rm-ctitle>Comments</span></h5>' +
            '<button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>' +
            '</div>' +
            '<div class="modal-body review-chat" data-rm-cbody></div>' +
            '<div class="modal-footer">' +
            '<button type="button" class="btn btn-outline-secondary" data-bs-dismiss="modal">Close</button>' +
            '</div></div></div>';
        document.body.appendChild(commentsEl);
    }

    async function showComments(folder, version, opts) {
        opts = opts || {};
        buildComments();
        commentsEl.querySelector('[data-rm-ctitle]').textContent =
            opts.title || ('Comments — ' + folder + ' v' + version);
        const body = commentsEl.querySelector('[data-rm-cbody]');
        body.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading comments...</div>';

        if (window.bootstrap) {
            commentsModal = bootstrap.Modal.getOrCreateInstance(commentsEl);
            commentsModal.show();
        }

        try {
            const resp = await fetch(
                '/review/' + encodeURIComponent(folder) + '/' + encodeURIComponent(version),
                { credentials: 'same-origin' }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                body.innerHTML = '<div class="alert alert-danger small mb-0">' +
                    esc(data.message || 'Failed to load comments') + '</div>';
                return;
            }
            renderChat(body, data.events || []);
        } catch (err) {
            body.innerHTML = '<div class="alert alert-danger small mb-0">Network error: ' +
                esc(String(err)) + '</div>';
        }
    }

    function renderChat(body, events) {
        if (!events.length) {
            body.innerHTML = '<div class="text-center text-muted py-4">' +
                '<i class="bi bi-chat-square-dots d-block mb-2" style="font-size:1.6rem;"></i>' +
                'No comments yet.</div>';
            return;
        }
        body.innerHTML = events.map(chatBubble).join('');
        body.scrollTop = body.scrollHeight;
    }

    function chatBubble(e) {
        const meta = ACTION_META[e.action] || { icon: 'dot', cls: 'text-muted', label: e.action };
        const actor = e.actor || 'unknown';
        const initials = actor.replace(/@.*/, '').slice(0, 2).toUpperCase();
        const transition = (e.from_status && e.to_status)
            ? '<span class="review-chat-transition">' +
              esc(e.from_status) + ' &rarr; ' + esc(e.to_status) + '</span>'
            : '';
        const comment = e.comment
            ? '<div class="review-chat-text">' + esc(e.comment) + '</div>'
            : '<div class="review-chat-text review-chat-empty">No comment provided</div>';
        return '<div class="review-chat-row">' +
            '<div class="review-chat-avatar">' + esc(initials) + '</div>' +
            '<div class="review-chat-bubble">' +
            '<div class="review-chat-head">' +
            '<span class="review-chat-action ' + meta.cls + '">' +
            '<i class="bi bi-' + meta.icon + ' me-1"></i>' + esc(meta.label) + '</span>' +
            transition +
            '<span class="review-chat-time">' + formatTime(e.created_at) + '</span>' +
            '</div>' +
            comment +
            '<div class="review-chat-actor">' + esc(actor) + '</div>' +
            '</div></div>';
    }

    function formatTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        return esc(d.toLocaleString());
    }

    window.ReviewModals = { promptComment: promptComment, showComments: showComments };
})();
