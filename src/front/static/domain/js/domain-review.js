/**
 * Domain → Validation (review workflow)
 *
 * Business-user-oriented validation workspace for the loaded domain
 * version: a soft consistency-check summary, reviewer sign-off panel
 * (quorum-gated publish), and the full audit trail.
 *
 * Data sources:
 *   GET  /domain/version-status        -> loaded folder + version + status
 *   GET  /review/{folder}/{version}    -> review state + actions + events
 *   GET  /validate/detailed            -> soft readiness summary
 *   POST /review/{folder}/{version}/{submit|signoff|publish|reopen}
 */
(function () {
    'use strict';

    const state = { folder: '', version: '', detail: null };

    window.loadDomainReview = loadReview;

    document.addEventListener('DOMContentLoaded', () => {
        document.getElementById('btnRefreshReview')?.addEventListener(
            'click', loadReview
        );
    });

    async function loadReview() {
        const body = document.getElementById('reviewBody');
        if (!body) return;
        body.innerHTML = spinner('Loading review status...');

        try {
            const vs = await (await fetch('/domain/version-status', {
                credentials: 'same-origin',
            })).json();
            state.folder = vs.domain_folder || '';
            state.version = vs.version || '';

            if (!state.folder || !vs.has_registry) {
                body.innerHTML =
                    '<div class="alert alert-info small mb-0">' +
                    '<i class="bi bi-info-circle me-1"></i>' +
                    'Save this domain to the registry to start the review workflow.' +
                    '</div>';
                return;
            }

            const detail = await (await fetch(
                '/review/' + encodeURIComponent(state.folder) + '/' +
                encodeURIComponent(state.version),
                { credentials: 'same-origin' }
            )).json();

            if (!detail.success) {
                body.innerHTML =
                    '<div class="alert alert-danger small mb-0">' +
                    escapeHtml(detail.message || 'Failed to load review status') +
                    '</div>';
                return;
            }
            state.detail = detail;
            render(detail);
            loadReadiness();
            loadTeam();
        } catch (err) {
            console.error('loadReview error:', err);
            showNotification('Could not load review status: ' + err.message, 'error');
            body.innerHTML =
                '<div class="alert alert-danger small mb-0">Network error: ' +
                escapeHtml(String(err)) + '</div>';
        }
    }

    function render(d) {
        const body = document.getElementById('reviewBody');
        if (!body) return;
        body.innerHTML =
            statusBanner(d) +
            lifecycleDiagram(d) +
            '<div class="row g-3">' +
            '<div class="col-lg-6">' + consistencyCard() + teamCard() + '</div>' +
            '<div class="col-lg-6">' + timelineCard(d) + '</div>' +
            '</div>';

        wireActions(d);
    }

    /* ── Status banner (header: status + actions + notes) ── */
    function statusBanner(d) {
        const buttons = actionButtons(d);
        const notes = actionNotes(d);
        const right = buttons.length
            ? '<div class="review-actions d-flex flex-wrap gap-2 justify-content-end">' +
              buttons.join('') + '</div>'
            : '';
        const notesHtml = notes.length
            ? '<div class="review-banner-notes d-flex flex-column gap-2 mt-2">' +
              notes.join('') + '</div>'
            : '';
        return '<div class="review-banner review-banner-' +
            d.status.toLowerCase().replace('-', '') + ' mb-3">' +
            '<div class="d-flex justify-content-between align-items-start gap-3 flex-wrap">' +
            '<div class="d-flex align-items-center gap-3">' +
            statusBadge(d.status) +
            '<div><strong>' + escapeHtml(d.domain) + '</strong> ' +
            '<span class="text-muted">v' + escapeHtml(d.version) + '</span></div>' +
            '</div>' + right + '</div>' + notesHtml + '</div>';
    }

    /* ── Lifecycle diagram ─────────────────────────── */
    function lifecycleDiagram(d) {
        const status = (d.status || 'DRAFT').toUpperCase();
        const order = ['DRAFT', 'IN-REVIEW', 'PUBLISHED'];
        const curIdx = order.indexOf(status);
        const ev = d.events || [];
        const submitter = lastActorFor(ev, ['submitted']);
        const publisher = lastActorFor(ev, ['published']);
        const back = latestEventFor(ev, ['changes_requested', 'reopened']);
        const approvers = d.approvers || [];

        const stateOf = (i) =>
            i < curIdx ? 'done' : (i === curIdx ? 'current' : 'todo');

        // Draft: who (if anyone) last sent it back here.
        let draftFooter;
        if (status === 'DRAFT' && back && back.actor) {
            draftFooter = peopleLine(
                back.action === 'changes_requested'
                    ? 'Changes requested by' : 'Sent back by',
                [back.actor]);
        } else {
            draftFooter = mutedLine('Editable working copy');
        }

        // In Review: submitter + sign-offs collected.
        let irFooter = '';
        if (submitter) irFooter += peopleLine('Submitted by', [submitter]);
        irFooter += approvers.length
            ? peopleLine(
                'Signed off (' + d.approvals + '/' + d.quorum + ')', approvers)
            : mutedLine('Awaiting sign-off (0/' + d.quorum + ')');
        if (status === 'IN-REVIEW') irFooter += progressBar(d);

        // Published: who pushed it live.
        const pubFooter = publisher
            ? peopleLine('Published by', [publisher])
            : mutedLine('Not yet published');

        const boxes =
            stageBox({
                state: stateOf(0), tone: 'draft', icon: 'pencil',
                label: 'Draft',
                desc: 'Author edits the model', footer: draftFooter,
            }) +
            flowArrow('Submit', curIdx >= 1) +
            stageBox({
                state: stateOf(1), tone: 'inreview', icon: 'eye',
                label: 'In Review',
                desc: d.quorum + ' sign-off' + (d.quorum > 1 ? 's' : '') +
                    ' required',
                footer: irFooter,
            }) +
            flowArrow('Publish', curIdx >= 2) +
            stageBox({
                state: stateOf(2), tone: 'published', icon: 'broadcast',
                label: 'Published',
                desc: 'Live on API / MCP', footer: pubFooter,
            });

        return '<div class="card review-card review-flow-card mb-3">' +
            '<div class="card-body">' +
            '<h6 class="card-title"><i class="bi bi-diagram-3 me-2"></i>' +
            'Lifecycle</h6>' +
            '<div class="review-flow">' + boxes + '</div>' +
            '<div class="review-flow-note text-muted small mt-2">' +
            '<i class="bi bi-arrow-return-left me-1"></i>' +
            'An administrator can send an In&nbsp;Review or Published version ' +
            'back to Draft.</div></div></div>';
    }

    function stageBox(cfg) {
        const badge = cfg.state === 'done'
            ? '<span class="review-stage-badge"><i class="bi bi-check-lg"></i></span>'
            : (cfg.state === 'current'
                ? '<span class="review-stage-badge"><i class="bi bi-record-fill"></i></span>'
                : '');
        return '<div class="review-stage is-' + cfg.state +
            ' tone-' + cfg.tone + '">' +
            badge +
            '<div class="review-stage-head">' +
            '<span class="review-stage-icon"><i class="bi bi-' + cfg.icon +
            '"></i></span>' +
            '<span class="review-stage-label">' + escapeHtml(cfg.label) +
            '</span>' +
            (cfg.state === 'current'
                ? '<span class="review-stage-now">you are here</span>' : '') +
            '</div>' +
            '<div class="review-stage-desc">' + escapeHtml(cfg.desc) + '</div>' +
            '<div class="review-stage-foot">' + cfg.footer + '</div>' +
            '</div>';
    }

    function flowArrow(label, active) {
        return '<div class="review-arrow' + (active ? ' is-active' : '') + '">' +
            '<i class="bi bi-arrow-right"></i>' +
            '<span class="review-arrow-label">' + escapeHtml(label) + '</span>' +
            '</div>';
    }

    function progressBar(d) {
        const pct = d.quorum > 0
            ? Math.min(100, Math.round((d.approvals / d.quorum) * 100))
            : 0;
        return '<div class="review-progress mt-1">' +
            '<div class="progress" style="height:6px;">' +
            '<div class="progress-bar ' +
            (d.quorum_met ? 'bg-success' : 'bg-warning') +
            '" style="width:' + pct + '%"></div></div></div>';
    }

    function peopleLine(label, emails) {
        return '<div class="review-stage-line">' +
            '<div class="review-stage-line-label">' + escapeHtml(label) +
            '</div><div class="review-people">' +
            emails.map(personChip).join('') + '</div></div>';
    }

    function mutedLine(text) {
        return '<div class="review-stage-line">' +
            '<span class="review-stage-muted">' + escapeHtml(text) +
            '</span></div>';
    }

    function personChip(email) {
        const e = email || '';
        if (!e) {
            return '<span class="review-person" title="unknown user">' +
                '<span class="review-avatar review-avatar-muted">?</span>' +
                '<span class="review-person-name text-muted">unknown</span>' +
                '</span>';
        }
        return '<span class="review-person" title="' + escapeHtml(e) + '">' +
            '<span class="review-avatar">' + escapeHtml(initialsOf(e)) +
            '</span><span class="review-person-name">' +
            escapeHtml(shortName(e)) + '</span></span>';
    }

    function initialsOf(email) {
        const local = String(email).split('@')[0] || '';
        const parts = local.split(/[._-]+/).filter(Boolean);
        const letters = parts.length >= 2
            ? parts[0][0] + parts[1][0]
            : local.slice(0, 2);
        return letters.toUpperCase();
    }

    function shortName(email) {
        return String(email).split('@')[0] || email;
    }

    function lastActorFor(ev, actions) {
        const e = latestEventFor(ev, actions);
        return e ? (e.actor || '') : '';
    }

    function latestEventFor(ev, actions) {
        for (let i = ev.length - 1; i >= 0; i--) {
            if (actions.indexOf(ev[i].action) !== -1) return ev[i];
        }
        return null;
    }

    /* ── Consistency checks (soft) ─────────────────── */
    function consistencyCard() {
        return '<div class="card review-card mb-3"><div class="card-body">' +
            '<h6 class="card-title"><i class="bi bi-clipboard-check me-2"></i>Consistency checks</h6>' +
            '<p class="text-muted small mb-3">A quick readiness summary. ' +
            'These checks are advisory &mdash; they do not block publishing.</p>' +
            '<div id="reviewReadiness">' + spinner('Checking...') + '</div>' +
            '<div class="d-flex gap-2 mt-3">' +
            '<a class="btn btn-sm btn-outline-secondary" href="/domain/?section=validation">' +
            '<i class="bi bi-speedometer2 me-1"></i>Open Cockpit</a>' +
            '<a class="btn btn-sm btn-outline-secondary" href="/ontology/?section=pitfalls">' +
            '<i class="bi bi-bug me-1"></i>Run Pitfalls</a>' +
            '</div></div></div>';
    }

    async function loadReadiness() {
        const el = document.getElementById('reviewReadiness');
        if (!el) return;
        try {
            const d = await (await fetch('/validate/detailed', {
                credentials: 'same-origin',
            })).json();
            const dtwin = d.dtwin || {};
            const warehouse = d.warehouse || {};
            el.innerHTML =
                checkRow('Ontology valid', !!d.ontology_valid) +
                checkRow('Mapping complete', !!d.mapping_valid) +
                checkRow('SQL Warehouse configured', !!warehouse.warehouse_id) +
                checkRow('Digital Twin built', dtwin.indicator === 'green',
                    dtwin.indicator === 'orange');
        } catch (err) {
            el.innerHTML = '<div class="text-muted small">Readiness unavailable.</div>';
        }
    }

    function checkRow(label, ok, warn) {
        const icon = ok
            ? '<i class="bi bi-check-circle-fill text-success"></i>'
            : (warn
                ? '<i class="bi bi-exclamation-triangle-fill text-warning"></i>'
                : '<i class="bi bi-x-circle-fill text-danger"></i>');
        return '<div class="review-check d-flex align-items-center gap-2 mb-1">' +
            icon + '<span class="small">' + escapeHtml(label) + '</span></div>';
    }

    /* ── Domain access (who can view / edit / build) ───── */
    function teamCard() {
        return '<div class="card review-card mb-3"><div class="card-body">' +
            '<h6 class="card-title"><i class="bi bi-people me-2"></i>Domain access</h6>' +
            '<p class="text-muted small mb-3">People and groups with a role on ' +
            'this domain. Roles are managed by administrators in ' +
            'Registry &rarr; Teams.</p>' +
            '<div id="reviewTeam">' + spinner('Loading members...') + '</div>' +
            '</div></div>';
    }

    const ROLE_META = {
        builder: {
            label: 'Builder', icon: 'tools',
            cls: 'bg-primary-subtle text-primary-emphasis border-primary',
            desc: 'Can edit, build and publish',
        },
        editor: {
            label: 'Editor', icon: 'pencil-square',
            cls: 'bg-info-subtle text-info-emphasis border-info',
            desc: 'Can edit the model',
        },
        viewer: {
            label: 'Viewer', icon: 'eye',
            cls: 'bg-secondary-subtle text-secondary-emphasis border-secondary',
            desc: 'Read-only access',
        },
    };
    const ROLE_ORDER = ['builder', 'editor', 'viewer'];

    async function loadTeam() {
        const el = document.getElementById('reviewTeam');
        if (!el) return;
        try {
            const d = await (await fetch(
                '/review/' + encodeURIComponent(state.folder) + '/' +
                encodeURIComponent(state.version) + '/team',
                { credentials: 'same-origin' }
            )).json();
            const members = (d && d.members) || [];
            if (!members.length) {
                el.innerHTML = '<div class="text-muted small">' +
                    'No members assigned yet — only administrators can ' +
                    'access this domain.</div>';
                return;
            }
            const byRole = {};
            members.forEach((m) => {
                (byRole[m.role] = byRole[m.role] || []).push(m);
            });
            el.innerHTML = ROLE_ORDER
                .filter((r) => byRole[r] && byRole[r].length)
                .map((r) => roleGroup(r, byRole[r]))
                .join('');
        } catch (err) {
            el.innerHTML =
                '<div class="text-muted small">Member list unavailable.</div>';
        }
    }

    function roleGroup(role, members) {
        const meta = ROLE_META[role] || {
            label: role, icon: 'person', cls: 'bg-light text-dark border',
            desc: '',
        };
        return '<div class="review-role-group mb-3">' +
            '<div class="review-role-head d-flex align-items-center gap-2 mb-1">' +
            '<span class="badge border ' + meta.cls + '">' +
            '<i class="bi bi-' + meta.icon + ' me-1"></i>' +
            escapeHtml(meta.label) + '</span>' +
            '<span class="text-muted small">' + escapeHtml(meta.desc) +
            ' &middot; ' + members.length + '</span></div>' +
            '<div class="review-people">' +
            members.map(memberChip).join('') + '</div></div>';
    }

    function memberChip(m) {
        const name = m.display_name || m.principal || '';
        const principal = m.principal || name;
        const avatar = m.principal_type === 'group'
            ? '<span class="review-avatar review-avatar-group">' +
              '<i class="bi bi-people-fill"></i></span>'
            : '<span class="review-avatar">' +
              escapeHtml(initialsOf(principal)) + '</span>';
        return '<span class="review-person" title="' + escapeHtml(principal) +
            '">' + avatar + '<span class="review-person-name">' +
            escapeHtml(shortName(name)) + '</span></span>';
    }

    /* ── Actions (surfaced in the header) ──────────── */
    function actionButtons(d) {
        const a = d.actions || {};
        const buttons = [];
        if (a.can_submit) {
            buttons.push(btn('submit', 'btn-info', 'eye', 'Submit for review'));
        }
        if (a.can_approve) {
            buttons.push(btn('approve', 'btn-success', 'hand-thumbs-up', 'Approve'));
        }
        if (a.can_request_changes) {
            buttons.push(btn('request_changes', 'btn-outline-danger',
                'arrow-counterclockwise', 'Request changes'));
        }
        if (a.can_publish && d.publish_override) {
            buttons.push(btn('publish', 'btn-warning', 'broadcast',
                'Publish (override quorum)'));
        } else if (a.can_publish) {
            buttons.push(btn('publish', 'btn-success', 'broadcast', 'Publish'));
        }
        if (a.can_reopen) {
            buttons.push(btn('reopen', 'btn-outline-secondary',
                'arrow-counterclockwise', 'Back to Draft'));
        }
        return buttons;
    }

    function actionNotes(d) {
        const a = d.actions || {};
        const notes = [];
        if (a.can_submit === false && d.status === 'DRAFT' &&
            a.submit_blocked_reason) {
            notes.push('<div class="alert alert-warning small mb-0 py-2">' +
                '<i class="bi bi-exclamation-triangle me-1"></i>' +
                escapeHtml(a.submit_blocked_reason) + '</div>');
        }
        if (d.already_approved && d.status === 'IN-REVIEW') {
            notes.push('<div class="alert alert-success small mb-0 py-2">' +
                '<i class="bi bi-check-circle me-1"></i>' +
                'You have signed off on this version.</div>');
        }
        if (a.can_publish && d.publish_override) {
            notes.push('<div class="alert alert-warning small mb-0 py-2">' +
                '<i class="bi bi-shield-exclamation me-1"></i>' +
                'Admin override: only ' + d.approvals + ' of ' + d.quorum +
                ' sign-off(s) collected. Publishing will bypass the quorum.</div>');
        }
        if (d.status === 'IN-REVIEW' && !a.can_publish && !d.quorum_met) {
            notes.push('<div class="text-muted small">' +
                'Publishing unlocks once ' + d.quorum +
                ' sign-off(s) are collected.</div>');
        }
        return notes;
    }

    function btn(action, cls, icon, label) {
        return '<button type="button" class="btn btn-sm ' + cls + '" ' +
            'data-review-action="' + action + '">' +
            '<i class="bi bi-' + icon + ' me-1"></i>' + escapeHtml(label) + '</button>';
    }

    function wireActions() {
        document.querySelectorAll('[data-review-action]').forEach((b) => {
            b.addEventListener('click', () => onAction(b.dataset.reviewAction));
        });
    }

    async function onAction(action) {
        if (action === 'approve') {
            const r = await ReviewModals.promptComment({
                title: 'Approve version', confirmText: 'Approve',
                confirmClass: 'btn-success', icon: 'hand-thumbs-up',
                message: 'Record your sign-off on v' + escapeHtml(state.version) +
                    '. This counts toward the publish quorum.',
            });
            if (r.confirmed) {
                // Approve does not change the lifecycle status.
                await postAction('signoff',
                    { decision: 'approve', comment: r.comment }, false);
            }
            return;
        }
        if (action === 'request_changes') {
            const r = await ReviewModals.promptComment({
                title: 'Request changes', confirmText: 'Request changes',
                confirmClass: 'btn-danger', icon: 'arrow-counterclockwise',
                requireComment: true,
                message: 'Send v' + escapeHtml(state.version) +
                    ' back to Draft for changes. Tell the author what to fix.',
            });
            if (r.confirmed) {
                await postAction('signoff',
                    { decision: 'request_changes', comment: r.comment }, true);
            }
            return;
        }
        const dialogs = {
            submit: {
                title: 'Submit for review', confirmText: 'Submit', icon: 'eye',
                message: 'Submit v' + escapeHtml(state.version) +
                    ' for review? Editing locks until it returns to Draft.',
            },
            publish: {
                title: 'Publish version', confirmText: 'Publish', icon: 'broadcast',
                message: 'Publish v' + escapeHtml(state.version) +
                    '? It becomes the live version on the API/MCP surface.',
            },
            reopen: {
                title: 'Back to Draft', confirmText: 'Back to Draft',
                icon: 'arrow-counterclockwise',
                message: 'Send v' + escapeHtml(state.version) +
                    ' back to Draft? This re-enables editing (admin only).',
            },
        };
        const dlg = dialogs[action];
        if (!dlg) return;
        const r = await ReviewModals.promptComment({
            title: dlg.title, message: dlg.message,
            confirmText: dlg.confirmText, confirmClass: 'btn-primary', icon: dlg.icon,
        });
        if (!r.confirmed) return;
        await postAction(action, { comment: r.comment }, true);
    }

    async function postAction(endpoint, payload, statusChanging) {
        try {
            const resp = await fetch(
                '/review/' + encodeURIComponent(state.folder) + '/' +
                encodeURIComponent(state.version) + '/' + endpoint,
                {
                    method: 'POST',
                    credentials: 'same-origin',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload || {}),
                }
            );
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                showNotification(data.message || 'Action failed', 'error');
                return;
            }
            showNotification('Done — version is now ' + (data.status || '') + '.', 'success');
            if (typeof fetchCachedInvalidate === 'function') {
                fetchCachedInvalidate('/navbar/state');
            }
            if (statusChanging) {
                // Status drives the edit-lock body class; reload to re-apply it.
                setTimeout(() => window.location.reload(), 700);
            } else {
                loadReview();
            }
        } catch (err) {
            showNotification('Error: ' + err.message, 'error');
        }
    }

    /* ── Audit timeline ────────────────────────────── */
    function timelineCard(d) {
        const events = (d.events || []).slice().reverse(); // newest first
        const items = events.length
            ? events.map(timelineItem).join('')
            : '<div class="text-muted small">No activity yet.</div>';
        return '<div class="card review-card"><div class="card-body">' +
            '<h6 class="card-title"><i class="bi bi-clock-history me-2"></i>Audit trail</h6>' +
            '<div class="review-timeline">' + items + '</div></div></div>';
    }

    function timelineItem(e) {
        const meta = ACTION_META[e.action] || { icon: 'dot', cls: 'text-muted', label: e.action };
        const transition = (e.from_status && e.to_status)
            ? '<span class="review-transition">' +
              escapeHtml(e.from_status) + ' &rarr; ' + escapeHtml(e.to_status) + '</span>'
            : '';
        const comment = e.comment
            ? '<div class="review-event-comment">' + escapeHtml(e.comment) + '</div>'
            : '';
        return '<div class="review-event">' +
            '<span class="review-event-icon ' + meta.cls + '">' +
            '<i class="bi bi-' + meta.icon + '"></i></span>' +
            '<div class="review-event-body">' +
            '<div class="review-event-head">' +
            '<span class="fw-medium">' + escapeHtml(meta.label) + '</span> ' +
            transition +
            '<span class="review-event-time">' + formatTime(e.created_at) + '</span>' +
            '</div>' +
            '<div class="review-event-actor small text-muted">' +
            escapeHtml(e.actor || 'unknown') + '</div>' +
            comment + '</div></div>';
    }

    const ACTION_META = {
        submitted: { icon: 'eye', cls: 'text-info', label: 'Submitted for review' },
        approved: { icon: 'hand-thumbs-up', cls: 'text-success', label: 'Approved' },
        changes_requested: { icon: 'arrow-counterclockwise', cls: 'text-danger', label: 'Changes requested' },
        published: { icon: 'broadcast', cls: 'text-success', label: 'Published' },
        reopened: { icon: 'unlock', cls: 'text-secondary', label: 'Reopened' },
        commented: { icon: 'chat-left-text', cls: 'text-muted', label: 'Comment' },
    };

    /* ── Helpers ───────────────────────────────────── */
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

    function formatTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        if (isNaN(d.getTime())) return '';
        return '<span class="text-muted small">' +
            escapeHtml(d.toLocaleString()) + '</span>';
    }

    function spinner(text) {
        return '<div class="text-center text-muted small py-3">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            escapeHtml(text) + '</div>';
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }
})();
