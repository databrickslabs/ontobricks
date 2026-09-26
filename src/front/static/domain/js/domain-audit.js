/**
 * Domain → Audit trail
 *
 * Ontology and mapping change feeds (plus status and build tabs) from
 * GET /domain/audit-trail. Change events show the asset and, for updates,
 * the previous field values from ``meta.before``.
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
        class_added: { icon: 'plus-circle', cls: 'text-success', label: 'Entity added' },
        class_updated: { icon: 'pencil', cls: 'text-primary', label: 'Entity updated' },
        class_removed: { icon: 'trash', cls: 'text-danger', label: 'Entity removed' },
        property_added: { icon: 'plus-circle', cls: 'text-success', label: 'Property added' },
        property_updated: { icon: 'pencil', cls: 'text-primary', label: 'Property updated' },
        property_removed: { icon: 'trash', cls: 'text-danger', label: 'Property removed' },
        mapping_entity_added: { icon: 'plus-circle', cls: 'text-success', label: 'Entity mapping added' },
        mapping_entity_updated: { icon: 'pencil', cls: 'text-primary', label: 'Entity mapping updated' },
        mapping_entity_removed: { icon: 'trash', cls: 'text-danger', label: 'Entity mapping removed' },
        mapping_relationship_added: { icon: 'plus-circle', cls: 'text-success', label: 'Relationship mapping added' },
        mapping_relationship_updated: { icon: 'pencil', cls: 'text-primary', label: 'Relationship mapping updated' },
        mapping_relationship_removed: { icon: 'trash', cls: 'text-danger', label: 'Relationship mapping removed' },
        mapping_excluded: { icon: 'eye-slash', cls: 'text-secondary', label: 'Mappings excluded' },
        mapping_included: { icon: 'eye', cls: 'text-secondary', label: 'Mappings included' },
        shacl_added: { icon: 'plus-circle', cls: 'text-success', label: 'SHACL shape added' },
        shacl_updated: { icon: 'pencil', cls: 'text-primary', label: 'SHACL shape updated' },
        shacl_removed: { icon: 'trash', cls: 'text-danger', label: 'SHACL shape removed' },
        swrl_added: { icon: 'plus-circle', cls: 'text-success', label: 'SWRL rule added' },
        swrl_updated: { icon: 'pencil', cls: 'text-primary', label: 'SWRL rule updated' },
        swrl_removed: { icon: 'trash', cls: 'text-danger', label: 'SWRL rule removed' },
        group_added: { icon: 'plus-circle', cls: 'text-success', label: 'Group added' },
        group_updated: { icon: 'pencil', cls: 'text-primary', label: 'Group updated' },
        group_removed: { icon: 'trash', cls: 'text-danger', label: 'Group removed' },
        axiom_added: { icon: 'plus-circle', cls: 'text-success', label: 'Axiom added' },
        axiom_updated: { icon: 'pencil', cls: 'text-primary', label: 'Axiom updated' },
        axiom_removed: { icon: 'trash', cls: 'text-danger', label: 'Axiom removed' },
        expression_added: { icon: 'plus-circle', cls: 'text-success', label: 'Expression added' },
        expression_updated: { icon: 'pencil', cls: 'text-primary', label: 'Expression updated' },
        expression_removed: { icon: 'trash', cls: 'text-danger', label: 'Expression removed' },
        ontology_generated: { icon: 'magic', cls: 'text-info', label: 'Ontology generated' },
        ontology_reset: { icon: 'arrow-counterclockwise', cls: 'text-warning', label: 'Ontology reset' },
        mapping_reset: { icon: 'trash', cls: 'text-danger', label: 'Mappings reset' },
        agent_auto_map_run: { icon: 'robot', cls: 'text-info', label: 'Auto-mapping agent run' },
        metadata_table_removed: { icon: 'trash', cls: 'text-danger', label: 'Data source removed' },
    };

    // Terminal statuses of an agent run -> chip styling.
    const RUN_STATUS_CLS = {
        completed: 'bg-success-subtle text-success-emphasis',
        failed: 'bg-danger-subtle text-danger-emphasis',
        cancelled: 'bg-warning-subtle text-warning-emphasis',
    };

    let _cache = { events: [], runs: [], changes: [], versions: [], current: '' };
    let _version = '';  // '' = all versions

    window.loadDomainAudit = loadAudit;

    document.addEventListener('DOMContentLoaded', () => {
        document.getElementById('btnReloadAudit')?.addEventListener('click', loadAudit);
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
        const bodies = [
            document.getElementById('auditOntologyBody'),
            document.getElementById('auditMappingBody'),
            document.getElementById('auditReviewBody'),
            document.getElementById('auditBuildBody'),
        ].filter(Boolean);
        if (!bodies.length) return;
        const loading = '<div class="text-center text-muted small py-5">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading audit trail&hellip;</div>';
        bodies.forEach((el) => { el.innerHTML = loading; });
        try {
            const resp = await fetch('/domain/audit-trail', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!resp.ok || !data.success) {
                const err = '<div class="alert alert-warning small mb-0">' +
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    esc(data.message || 'Failed to load audit trail') + '</div>';
                bodies.forEach((el) => { el.innerHTML = err; });
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
            const fail = '<div class="alert alert-danger small mb-0">Network error: ' +
                esc(String(err)) + '</div>';
            bodies.forEach((el) => { el.innerHTML = fail; });
        }
    }

    function matchesVersion(v) {
        return _version === '' || String(v == null ? '' : v) === _version;
    }

    function isMappingChange(c) {
        const action = String(c.action || '');
        const entityType = String(c.entity_type || '');
        return action.indexOf('mapping_') === 0
            || entityType.indexOf('mapping') === 0
            || action === 'agent_auto_map_run'
            || action === 'metadata_table_removed';
    }

    function emptyState() {
        return '<div class="text-center text-muted py-4">' +
            '<i class="bi bi-clock-history d-block mb-2"></i>' +
            'No activity recorded yet.</div>';
    }

    function fillPane(id, html) {
        const el = document.getElementById(id);
        if (el) el.innerHTML = html;
    }

    function changeItems(mapping) {
        const items = [];
        _cache.changes.forEach((c) => {
            if (!matchesVersion(c.version)) return;
            if (isMappingChange(c) !== mapping) return;
            items.push({ ts: c.occurred_at || c.created_at, raw: c });
        });
        items.sort((a, b) => tsVal(b.ts) - tsVal(a.ts));
        return collapseChanges(items);
    }

    function collapseChanges(items) {
        const seen = {};
        const out = [];
        items.forEach((it) => {
            const c = it.raw || {};
            const before = JSON.stringify((c.meta || {}).before || {});
            const after = JSON.stringify((c.meta || {}).after || {});
            const key = [c.action, c.entity_ref, c.version, before, after].join('\0');
            if (seen[key]) return;
            seen[key] = true;
            out.push(it);
        });
        return out;
    }

    function reviewItems() {
        const items = [];
        _cache.events.forEach((e) => {
            if (matchesVersion(e.version)) items.push({ ts: e.created_at, raw: e });
        });
        items.sort((a, b) => tsVal(b.ts) - tsVal(a.ts));
        return items;
    }

    function buildItems() {
        const items = [];
        _cache.runs.forEach((r, i) => {
            if (matchesVersion(r.version)) {
                items.push({ ts: r.started_at || r.finished_at, raw: r, idx: i });
            }
        });
        items.sort((a, b) => tsVal(b.ts) - tsVal(a.ts));
        return items;
    }

    function renderTimeline() {
        const onto = changeItems(false);
        const maps = changeItems(true);
        const reviews = reviewItems();
        const builds = buildItems();
        fillPane('auditOntologyBody', onto.length
            ? '<div class="audit-timeline">' + onto.map((it) => changeItem(it.raw)).join('') + '</div>'
            : emptyState());
        fillPane('auditMappingBody', maps.length
            ? '<div class="audit-timeline">' + maps.map((it) => changeItem(it.raw)).join('') + '</div>'
            : emptyState());
        fillPane('auditReviewBody', reviews.length
            ? '<div class="audit-timeline">' + reviews.map((it) => reviewItem(it.raw)).join('') + '</div>'
            : emptyState());
        fillPane('auditBuildBody', builds.length
            ? '<div class="audit-timeline">' + builds.map((it) => buildItem(it.raw, it.idx)).join('') + '</div>'
            : emptyState());
        document.getElementById('auditBuildBody')?.querySelectorAll('button[data-run-idx]').forEach((btn) => {
            btn.addEventListener('click', () => {
                const run = _cache.runs[Number(btn.dataset.runIdx)];
                if (typeof window.showRunDetailsObj === 'function') {
                    window.showRunDetailsObj(run);
                }
            });
        });
    }

    function node(markerCls, icon, inner, label) {
        const aria = label
            ? ' title="' + esc(label) + '" aria-label="' + esc(label) + '"'
            : '';
        return '<div class="audit-item">' +
            '<div class="audit-marker ' + markerCls + '"' + aria + '>' +
            '<i class="bi bi-' + icon + '" aria-hidden="true"></i></div>' +
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
        return node('audit-marker-review', meta.icon, head + comment + who, meta.label);
    }

    function isRemoveAction(action) {
        const name = String(action || '');
        return name.indexOf('_removed') !== -1 || name.indexOf('_reset') !== -1;
    }

    function prettyAction(action) {
        return String(action || '')
            .replace(/_/g, ' ')
            .replace(/^\w/, (c) => c.toUpperCase());
    }

    /**
     * Full execution report of one auto-mapping agent run: status chip,
     * counters, and the ordered step log the overlay showed live.
     * Every value is escaped — step content is model output.
     * @param {object} m — the event's `meta` payload
     * @param {string} summary — the event summary (carries the error detail on a failed run)
     * @returns {string}
     */
    function agentRunReport(m, summary) {
        const status = String(m.status || '');
        const chipCls = RUN_STATUS_CLS[status] || 'bg-secondary-subtle text-secondary-emphasis';
        const chip = status
            ? '<span class="badge ' + chipCls + ' me-2">' + esc(status) + '</span>'
            : '';

        const stats = m.stats || {};
        const bits = [];
        if (Number(stats.entities)) bits.push(esc(stats.entities) + ' entities');
        if (Number(stats.relationships)) bits.push(esc(stats.relationships) + ' relationships');
        if (Number(stats.failed)) bits.push(esc(stats.failed) + ' unmapped');
        const errs = stats.chunk_errors || [];
        if (errs.length) bits.push(esc(errs.length) + ' chunk error(s)');
        if (Number(m.duration_ms)) bits.push(fmtDuration(Number(m.duration_ms) / 1000));

        const summaryLine = '<div class="audit-comment">' + chip +
            (bits.length ? '<span class="small text-muted">' + bits.join(' &middot; ') + '</span>' : '') +
            (summary ? '<div class="small mt-1">' + esc(summary) + '</div>' : '') +
            '</div>';

        const steps = Array.isArray(m.steps) ? m.steps : [];
        if (!steps.length) return summaryLine;

        const rows = steps.map((s) => {
            const type = String(s.type || '');
            const label = type === 'tool_call'
                ? '<strong>' + esc(s.tool) + '</strong>(' + esc(s.content || '') + ')'
                : type === 'tool_result'
                    ? '<span class="text-muted">' + esc(s.tool) + ' &rarr; ' + esc(s.content || '') + '</span>'
                    : '<em>' + esc(s.content || 'Output produced') + '</em>';
            const dur = Number(s.ms) ? '<span class="text-muted ms-2">' + esc(s.ms) + 'ms</span>' : '';
            return '<div class="audit-agent-step">' +
                '<span class="audit-agent-type">' + esc(type) + '</span>' +
                '<span class="flex-grow-1">' + label + '</span>' + dur + '</div>';
        });

        return summaryLine +
            '<details class="mt-1">' +
            '<summary class="small text-muted">' +
            'Agent execution report (' + esc(steps.length) + ' steps)</summary>' +
            '<div class="border rounded p-2 mt-1 audit-agent-log">' +
            rows.join('') + '</div></details>';
    }

    function localName(value) {
        const text = String(value || '');
        const hash = text.lastIndexOf('#');
        if (hash >= 0 && hash < text.length - 1) return text.slice(hash + 1);
        const slash = text.lastIndexOf('/');
        if (slash >= 0 && slash < text.length - 1) return text.slice(slash + 1);
        return text;
    }

    const FIELD_LABELS = {
        name: 'name',
        label: 'label',
        description: 'description',
        comment: 'comment',
        parent: 'parent',
        emoji: 'icon',
        table: 'table',
        catalog: 'catalog',
        schema: 'schema',
        sql_query: 'sql',
        id_column: 'id column',
        label_column: 'label column',
        dataProperties: 'attributes',
        properties: 'relationships',
        attribute_mappings: 'attributes',
        virtualAttributes: 'virtual attributes',
        source_table: 'source table',
        target_table: 'target table',
        source_id_column: 'source id',
        target_id_column: 'target id',
        domain: 'domain',
        range: 'range',
        type: 'type',
        antecedent: 'if',
        consequent: 'then',
        members: 'members',
        target_class: 'target',
        property_path: 'path',
        shacl_type: 'constraint',
        message: 'message',
    };

    function prettyField(key) {
        if (FIELD_LABELS[key]) return FIELD_LABELS[key];
        return String(key || '')
            .replace(/_/g, ' ')
            .replace(/([A-Z])/g, ' $1')
            .replace(/\s+/g, ' ')
            .trim()
            .toLowerCase();
    }

    function fmtValue(value) {
        if (value == null || value === '') return 'empty';
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
            return String(value);
        }
        if (Array.isArray(value)) {
            if (!value.length) return 'empty';
            if (value.every((item) => typeof item === 'string')) return value.join(', ');
            return value.length + ' items';
        }
        if (typeof value === 'object') {
            const keys = Object.keys(value);
            return keys.length ? keys.length + ' fields' : 'empty';
        }
        return String(value);
    }

    function missingVal(value) {
        return value == null || value === '' || value === '—';
    }

    function sortedDiffKeys(before, after) {
        const preferred = ['name', 'label', 'table', 'sql_query', 'description', 'comment', 'parent'];
        const keys = Object.keys(Object.assign({}, before, after));
        keys.sort((a, b) => {
            const ia = preferred.indexOf(a);
            const ib = preferred.indexOf(b);
            return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib);
        });
        return keys;
    }

    function asMeta(raw) {
        if (!raw) return {};
        if (typeof raw === 'string') {
            try { return JSON.parse(raw) || {}; } catch (e) { return {}; }
        }
        return raw;
    }

    function actionKind(action) {
        const name = String(action || '');
        if (name.indexOf('_removed') !== -1 || name.indexOf('_reset') !== -1) return 'remove';
        if (name.indexOf('_added') !== -1) return 'add';
        return 'update';
    }

    function defaultField(c) {
        const type = String(c.entity_type || '');
        const action = String(c.action || '');
        if (type === 'class' || action.indexOf('class_') === 0) return 'entity';
        if (type === 'property' || action.indexOf('property_') === 0) return 'relationship';
        if (type.indexOf('mapping') === 0 || action.indexOf('mapping_') === 0) return 'mapping';
        if (type === 'shacl' || action.indexOf('shacl_') === 0) return 'shape';
        if (type === 'swrl' || action.indexOf('swrl_') === 0) return 'rule';
        if (type === 'group' || action.indexOf('group_') === 0) return 'group';
        if (type === 'axiom' || action.indexOf('axiom_') === 0) return 'axiom';
        if (type === 'expression' || action.indexOf('expression_') === 0) return 'expression';
        if (type.indexOf('rule') !== -1 || action.indexOf('rule') !== -1
                || action.indexOf('decision_') === 0
                || action.indexOf('sparql_') === 0
                || action.indexOf('aggregate_') === 0) return 'rule';
        return 'value';
    }

    function diffRowHtml(asset, field, beforeVal, afterVal, showAsset) {
        const oldText = missingVal(beforeVal) ? '' : fmtValue(beforeVal);
        const newText = missingVal(afterVal) ? '' : fmtValue(afterVal);
        const bits = [];
        if (showAsset && asset) {
            bits.push('<span class="audit-asset">' + esc(asset) + '</span>');
        }
        if (field) bits.push('<span class="audit-field">' + esc(field) + '</span>');
        if (oldText) bits.push('<span class="audit-old">' + esc(oldText) + '</span>');
        bits.push('<span class="audit-arrow" aria-hidden="true">→</span>');
        if (newText) bits.push('<span class="audit-new">' + esc(newText) + '</span>');
        const title = [showAsset ? asset : '', field, oldText, '→', newText]
            .filter(Boolean).join(' ');
        return '<div class="audit-diff-row" title="' + esc(title) + '">' +
            bits.join(' ') + '</div>';
    }

    function changeDetail(c) {
        const asset = localName(c.summary || c.entity_ref) || '—';
        const meta = asMeta(c.meta);
        const before = Object.assign({}, meta.before || {});
        const after = Object.assign({}, meta.after || {});
        if (!Object.keys(after).length && before.name !== undefined && asset) {
            after.name = asset;
        }
        let keys = sortedDiffKeys(before, after);
        const kind = actionKind(c.action);
        if (!keys.length) {
            const field = defaultField(c);
            keys = [field];
            if (kind === 'add') after[field] = asset;
            else if (kind === 'remove') before[field] = asset;
            else {
                before[field] = '—';
                after[field] = asset;
            }
        }
        return '<div class="audit-diffs">' +
            keys.slice(0, 4).map((key, i) => diffRowHtml(
                asset,
                prettyField(key),
                Object.prototype.hasOwnProperty.call(before, key) ? before[key] : undefined,
                Object.prototype.hasOwnProperty.call(after, key) ? after[key] : undefined,
                i === 0
            )).join('') +
            '</div>';
    }

    function changeItem(c) {
        const meta = CHANGE_META[c.action] ||
            { icon: isRemoveAction(c.action) ? 'trash' : 'pencil-square',
              cls: 'text-primary', label: prettyAction(c.action) };
        const agent = (c.source === 'agent')
            ? '<span class="badge bg-info-subtle text-info-emphasis" title="Change made by the AI assistant">' +
              '<i class="bi bi-robot me-1"></i>AI</span>'
            : '';
        const time = '<span class="audit-time">' + fmtTime(c.occurred_at || c.created_at) + '</span>';
        const who = '<div class="audit-meta">' + esc(c.actor || 'unknown') + '</div>';
        const marker = isRemoveAction(c.action) ? 'audit-marker-remove' : 'audit-marker-change';
        if (c.action === 'agent_auto_map_run') {
            const head = '<div class="audit-head">' +
                '<span class="audit-asset">Auto-map</span>' + agent + time + '</div>';
            return node(
                marker,
                meta.icon,
                head + agentRunReport(c.meta || {}, c.summary || '') + who,
                meta.label
            );
        }
        const head = '<div class="audit-head">' + changeDetail(c) + agent + time + '</div>';
        return node(marker, meta.icon, head + who, meta.label);
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
        return node('audit-marker-build', cfg[1], head + metaLine + msg, cfg[2]);
    }
})();
