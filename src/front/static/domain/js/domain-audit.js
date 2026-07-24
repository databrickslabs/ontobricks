/**
 * Domain → Audit trail
 *
 * One unified, newest-first activity feed for the loaded domain. It
 * interleaves three registry streams returned by GET /domain/audit-trail:
 *   - ontology/mapping change audit (who changed what, and when)
 *   - review/validation decisions (status switches + their comments)
 *   - build-run history (runs + results)
 *
 * Build entries reuse the run-details popup from domain-runs.js
 * (window.showRunDetailsObj) so the full run breakdown stays in one place.
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

    // Change-event action -> { icon, cls, label }. Falls back to a generic
    // label built from the action string for any action not listed here.
    const CHANGE_META = {
        class_added: { icon: 'plus-circle', cls: 'text-success', label: 'Class added' },
        class_updated: { icon: 'pencil', cls: 'text-primary', label: 'Class updated' },
        class_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'Class removed' },
        property_added: { icon: 'plus-circle', cls: 'text-success', label: 'Property added' },
        property_updated: { icon: 'pencil', cls: 'text-primary', label: 'Property updated' },
        property_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'Property removed' },
        mapping_entity_added: { icon: 'plus-circle', cls: 'text-success', label: 'Entity mapping added' },
        mapping_entity_updated: { icon: 'pencil', cls: 'text-primary', label: 'Entity mapping updated' },
        mapping_entity_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'Entity mapping removed' },
        mapping_relationship_added: { icon: 'plus-circle', cls: 'text-success', label: 'Relationship mapping added' },
        mapping_relationship_updated: { icon: 'pencil', cls: 'text-primary', label: 'Relationship mapping updated' },
        mapping_relationship_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'Relationship mapping removed' },
        mapping_excluded: { icon: 'eye-slash', cls: 'text-secondary', label: 'Mappings excluded' },
        mapping_included: { icon: 'eye', cls: 'text-secondary', label: 'Mappings included' },
        shacl_added: { icon: 'plus-circle', cls: 'text-success', label: 'SHACL shape added' },
        shacl_updated: { icon: 'pencil', cls: 'text-primary', label: 'SHACL shape updated' },
        shacl_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'SHACL shape removed' },
        swrl_added: { icon: 'plus-circle', cls: 'text-success', label: 'SWRL rule added' },
        swrl_updated: { icon: 'pencil', cls: 'text-primary', label: 'SWRL rule updated' },
        swrl_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'SWRL rule removed' },
        group_added: { icon: 'plus-circle', cls: 'text-success', label: 'Group added' },
        group_updated: { icon: 'pencil', cls: 'text-primary', label: 'Group updated' },
        group_removed: { icon: 'dash-circle', cls: 'text-danger', label: 'Group removed' },
        ontology_reset: { icon: 'arrow-counterclockwise', cls: 'text-warning', label: 'Ontology reset' },
        mapping_reset: { icon: 'arrow-counterclockwise', cls: 'text-warning', label: 'Mappings reset' },
    };

    let _cache = { events: [], runs: [], changes: [], versions: [], current: '' };
    let _filter = 'all';
    let _version = '';  // '' = all versions

    window.loadDomainAudit = loadAudit;

    document.addEventListener('DOMContentLoaded', () => {
        document.getElementById('btnReloadAudit')?.addEventListener('click', loadAudit);
        document.querySelectorAll('#auditFilter [data-audit-filter]').forEach((btn) => {
            btn.addEventListener('click', () => {
                _filter = btn.dataset.auditFilter;
                document.querySelectorAll('#auditFilter [data-audit-filter]')
                    .forEach((b) => b.classList.toggle('active', b === btn));
                renderTimeline();
            });
        });
        document.getElementById('auditVersionFilter')?.addEventListener('change', (e) => {
            _version = e.target.value;
            renderTimeline();
        });
    });

    function populateVersions(versions, current) {
        const sel = document.getElementById('auditVersionFilter');
        if (!sel) return;
        // Default the dropdown to the current version (if any), else "All".
        _version = (current && versions.indexOf(current) !== -1) ? current : '';
        sel.innerHTML = '<option value="">All versions</option>' +
            versions.map((v) => '<option value="' + esc(v) + '"' +
                (v === _version ? ' selected' : '') + '>v' + esc(v) +
                (v === current ? ' (current)' : '') + '</option>').join('');
        sel.value = _version;
    }

    function esc(s) {
        if (typeof window.escapeHtml === 'function') return window.escapeHtml(s == null ? '' : String(s));
        const div = document.createElement('div');
        div.textContent = String(s == null ? '' : s);
        return div.innerHTML;
    }

    function renderMd(text) {
        if (!text) return '';
        if (typeof window.marked !== 'undefined' && window.marked.parse) {
            try {
                window.marked.setOptions({ breaks: true, gfm: true });
                return window.marked.parse(text);
            } catch (e) { /* fall through */ }
        }
        // Fallback: escape and convert newlines to <br>
        return esc(text).replace(/\n/g, '<br>');
    }

    function fmtTime(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        return isNaN(d.getTime()) ? esc(iso) : esc(d.toLocaleString());
    }

    function tsVal(iso) {
        const d = new Date(iso);
        return isNaN(d.getTime()) ? 0 : d.getTime();
    }

    function fmtDuration(secs) {
        const s = Number(secs) || 0;
        if (s <= 0) return '';
        if (s < 60) return s.toFixed(1) + 's';
        return Math.floor(s / 60) + 'm ' + Math.round(s % 60) + 's';
    }

    async function loadAudit() {
        const body = document.getElementById('auditBody');
        if (!body) return;
        body.innerHTML = '<div class="text-center text-muted small py-5">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading audit trail&hellip;</div>';
        try {
            const resp = await fetch('/domain/audit-trail', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                body.innerHTML = '<div class="alert alert-warning small mb-0">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    esc(data.message || 'Failed to load audit trail') + '</div>';
                return;
            }
            _cache = {
                events: data.events || [],
                runs: data.runs || [],
                changes: data.changes || [],
                versions: data.versions || [],
                current: data.current_version || '',
            };
            populateVersions(_cache.versions, _cache.current);
            renderTimeline();
        } catch (err) {
            body.innerHTML = '<div class="alert alert-danger small mb-0">Network error: ' +
                esc(String(err)) + '</div>';
        }
    }

    function matchesVersion(v) {
        return _version === '' || String(v == null ? '' : v) === _version;
    }

    function shows(kind) {
        return _filter === 'all' || _filter === kind;
    }

    function buildItems() {
        const items = [];
        if (shows('changes')) {
            _cache.changes.forEach((c) => {
                if (matchesVersion(c.version)) {
                    items.push({ kind: 'changes', ts: c.occurred_at || c.created_at, raw: c });
                }
            });
        }
        if (shows('review')) {
            _cache.events.forEach((e) => {
                if (matchesVersion(e.version)) {
                    items.push({ kind: 'review', ts: e.created_at, raw: e });
                }
            });
        }
        if (shows('build')) {
            _cache.runs.forEach((r, i) => {
                if (matchesVersion(r.version)) {
                    items.push({ kind: 'build', ts: r.started_at || r.finished_at, raw: r, idx: i });
                }
            });
        }
        items.sort((a, b) => tsVal(b.ts) - tsVal(a.ts));
        return items;
    }

    function renderTimeline() {
        const body = document.getElementById('auditBody');
        if (!body) return;
        const items = buildItems();
        if (!items.length) {
            body.innerHTML = '<div class="text-center text-muted py-5">' +
                '<i class="bi bi-clock-history d-block mb-2" style="font-size:1.8rem;"></i>' +
                'No activity recorded yet.</div>';
            return;
        }
        body.innerHTML = '<div class="audit-timeline">' +
            items.map((it) => {
                if (it.kind === 'review') return reviewItem(it.raw);
                if (it.kind === 'changes') return changeItem(it.raw);
                return buildItem(it.raw, it.idx);
            }).join('') +
            '</div>';
        body.querySelectorAll('button[data-run-idx]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const run = _cache.runs[Number(btn.dataset.runIdx)];
                if (typeof window.showRunDetailsObj === 'function') {
                    window.showRunDetailsObj(run);
                }
            });
        });
    }

    function node(markerCls, icon, inner) {
        return '<div class="audit-item">' +
            '<div class="audit-marker ' + markerCls + '"><i class="bi bi-' + icon + '"></i></div>' +
            '<div class="audit-content">' + inner + '</div></div>';
    }

    function reviewItem(e) {
        const meta = ACTION_META[e.action] || { icon: 'dot', cls: 'text-muted', label: e.action };
        const transition = (e.from_status && e.to_status)
            ? '<span class="audit-chip">' + esc(e.from_status) + ' &rarr; ' + esc(e.to_status) + '</span>'
            : '';
        const ver = e.version ? '<span class="badge bg-secondary ms-1">v' + esc(e.version) + '</span>' : '';
        const comment = e.comment
            ? '<div class="audit-comment oc-md">' + renderMd(e.comment) + '</div>'
            : '';
        const head = '<div class="audit-head">' +
            '<span class="audit-title ' + meta.cls + '">' + esc(meta.label) + '</span>' +
            ver + transition +
            '<span class="audit-time">' + fmtTime(e.created_at) + '</span></div>';
        const who = '<div class="audit-meta">' + esc(e.actor || 'unknown') + '</div>';
        return node('audit-marker-review', meta.icon, head + comment + who);
    }

    function prettyAction(action) {
        return String(action || '')
            .replace(/_/g, ' ')
            .replace(/^\w/, (c) => c.toUpperCase());
    }

    function changeItem(c) {
        const meta = CHANGE_META[c.action] ||
            { icon: 'pencil-square', cls: 'text-primary', label: prettyAction(c.action) };
        const ver = c.version ? '<span class="badge bg-secondary ms-1">v' + esc(c.version) + '</span>' : '';
        const agent = (c.source === 'agent')
            ? '<span class="badge bg-info-subtle text-info-emphasis ms-1" title="Change made by the AI assistant">' +
              '<i class="bi bi-robot me-1"></i>AI</span>'
            : '';
        const ref = (c.summary || c.entity_ref)
            ? '<div class="audit-comment"><code>' + esc(c.summary || c.entity_ref) + '</code></div>'
            : '';
        const head = '<div class="audit-head">' +
            '<span class="audit-title ' + meta.cls + '">' + esc(meta.label) + '</span>' +
            ver + agent +
            '<span class="audit-time">' + fmtTime(c.occurred_at || c.created_at) + '</span></div>';
        const who = '<div class="audit-meta">' + esc(c.actor || 'unknown') + '</div>';
        return node('audit-marker-change', meta.icon, head + ref + who);
    }

    function buildItem(run, idx) {
        const st = (run.status || '').toLowerCase();
        const map = {
            success: ['text-success', 'check-circle', 'Build succeeded'],
            error: ['text-danger', 'x-circle', 'Build failed'],
            cancelled: ['text-warning', 'slash-circle', 'Build cancelled'],
        };
        const cfg = map[st] || ['text-secondary', 'hdd-stack', 'Build'];
        const ver = run.version ? '<span class="badge bg-secondary ms-1">v' + esc(run.version) + '</span>' : '';
        const dur = fmtDuration(run.duration_s);
        const bits = [];
        if (Number(run.triple_count)) bits.push(esc(Number(run.triple_count).toLocaleString()) + ' triples');
        if (dur) bits.push(dur);
        if (run.graph_engine) bits.push(esc(run.graph_engine));
        const metaLine = bits.length ? '<div class="audit-meta">' + bits.join(' &middot; ') + '</div>' : '';
        const msg = run.error
            ? '<div class="audit-comment text-danger">' + esc(run.error) + '</div>'
            : (run.message ? '<div class="audit-comment oc-md">' + renderMd(run.message) + '</div>' : '');
        const detailsBtn = '<button type="button" class="btn btn-sm btn-outline-primary audit-details" ' +
            'data-run-idx="' + idx + '" title="View build run details">' +
            '<i class="bi bi-eye"></i></button>';
        const head = '<div class="audit-head">' +
            '<span class="audit-title ' + cfg[0] + '">' + esc(cfg[2]) + '</span>' +
            ver +
            '<span class="audit-time">' + fmtTime(run.started_at || run.finished_at) + '</span>' +
            detailsBtn + '</div>';
        return node('audit-marker-build', cfg[1], head + metaLine + msg);
    }
})();
