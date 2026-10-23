/**
 * Data Quality Checks of one entity in the Graph Explorer (its SHACL facets).
 *
 * Every enabled shape targeting the entity's class, one of its ancestors, or
 * no class is checked against that entity only (``POST /dtwin/nodes/shacl/start``,
 * then ``/tasks/{id}``). The result opens in a popup, summarises in the node
 * detail panel, and rings the node on the canvas.
 *
 * Results are cached per entity for the lifetime of the page and only
 * recomputed on an explicit Re-check. Each completed check is broadcast as a
 * ``shacl-facets:updated`` event; the panel and the canvas listen for it.
 */

// { "<entityUri>": { data: <response>, checkedAt: Date } }
const _shaclCache = {};
let _shaclCurrentUri = null;
let _shaclFilter = 'all';
let _shaclPollTimer = null;

const SHACL_STATUS = {
    fail:           { icon: 'bi-x-circle-fill',             label: 'failed',         tone: 'danger' },
    error:          { icon: 'bi-exclamation-triangle-fill', label: 'error',          tone: 'warning' },
    not_evaluated:  { icon: 'bi-question-circle',           label: 'not evaluated',  tone: 'draft' },
    not_applicable: { icon: 'bi-dash-circle',               label: 'not applicable', tone: 'info' },
    pass:           { icon: 'bi-check-circle-fill',         label: 'passed',         tone: 'success' }
};

const SHACL_CATEGORIES = [
    { id: 'completeness', label: 'Completeness', icon: 'bi-check2-all' },
    { id: 'cardinality',  label: 'Cardinality',  icon: 'bi-123' },
    { id: 'uniqueness',   label: 'Uniqueness',   icon: 'bi-fingerprint' },
    { id: 'consistency',  label: 'Consistency',  icon: 'bi-link-45deg' },
    { id: 'conformance',  label: 'Conformance',  icon: 'bi-regex' },
    { id: 'structural',   label: 'Structural',   icon: 'bi-diagram-3' }
];

function _shaclEsc(value) {
    if (typeof escapeHtml === 'function') return escapeHtml(value == null ? '' : String(value));
    return String(value == null ? '' : value)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/**
 * Number of shapes the server would report for *className*: enabled shapes
 * on the class, on any ancestor (``parent`` chain), or on no class at all.
 */
function shaclShapeCountFor(className, classes, shapes) {
    if (!className || !Array.isArray(shapes) || !shapes.length) return 0;
    const byName = {};
    (classes || []).forEach(function (c) { if (c && c.name) byName[c.name] = c; });

    const names = new Set();
    const uris = new Set();
    let current = byName[className];
    let name = className;
    while (name && !names.has(name.toLowerCase())) {
        names.add(name.toLowerCase());
        if (current && current.uri) uris.add(current.uri);
        name = current ? (current.parent || '') : '';
        current = byName[name];
    }

    return shapes.filter(function (s) {
        return s && s.enabled !== false;
    }).filter(function (s) {
        const uri = s.target_class_uri || '';
        const target = (s.target_class || '').toLowerCase();
        if (!uri && !target) return true;
        return uris.has(uri) || names.has(target);
    }).length;
}

/**
 * Ring status of a checked node: 'danger' when a Violation failed, 'warning'
 * when a Warning/Info failed or a shape errored, 'success' when every shape
 * passed or did not apply. ``null`` (no ring) when unchecked, empty, or when
 * some shape could not be evaluated without any failure.
 */
function getShaclFacetStatus(entityUri) {
    const entry = _shaclCache[entityUri];
    const facets = entry && entry.data && entry.data.facets;
    if (!facets || !facets.length) return null;
    let warning = false;
    let unchecked = false;
    for (const f of facets) {
        if (f.status === 'fail' && (f.severity || 'sh:Violation') === 'sh:Violation') return 'danger';
        if (f.status === 'fail' || f.status === 'error') warning = true;
        else if (f.status === 'not_evaluated') unchecked = true;
    }
    if (warning) return 'warning';
    return unchecked ? null : 'success';
}

const _shaclColorCache = {};

/** Colour of a ring status, read from the shared ``--db-status-*`` tokens. */
function shaclStatusColor(status) {
    if (!_shaclColorCache[status]) {
        const token = { danger: '--db-status-danger', warning: '--db-status-warning', success: '--db-status-success' }[status];
        const value = token
            ? getComputedStyle(document.documentElement).getPropertyValue(token).trim()
            : '';
        _shaclColorCache[status] = value || 'gray';
    }
    return _shaclColorCache[status];
}

/**
 * Run the facet check for *entityUri*, cache it and broadcast it.
 *
 * @returns {Promise<object|null>} The response, or null when it failed.
 */
function _shaclStopPoll() {
    if (_shaclPollTimer) {
        clearInterval(_shaclPollTimer);
        _shaclPollTimer = null;
    }
}

function _shaclRenderProgressLog(task) {
    const step = document.getElementById('shaclFacetsProgressStep');
    if (step && task && task.message) {
        const running = task.status === 'running' || task.status === 'pending';
        step.innerHTML = (running
            ? '<span class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></span>'
            : '') + _shaclEsc(task.message);
    }
    const el = document.getElementById('shaclFacetsProgressLog');
    if (!el) return;
    if (window.TaskProgressUI && typeof TaskProgressUI.renderActivityLog === 'function') {
        TaskProgressUI.renderActivityLog(el, task || { log_entries: [], status: 'pending' });
        return;
    }
    const entries = (task && task.log_entries) || [];
    if (!entries.length) {
        el.innerHTML = '<div class="small text-muted">Waiting for activity…</div>';
        return;
    }
    const active = task.status === 'running' || task.status === 'pending';
    el.innerHTML = entries.map(function (entry, idx) {
        const running = active && idx === entries.length - 1;
        const icon = running
            ? '<span class="spinner-border spinner-border-sm text-primary flex-shrink-0" role="status"></span>'
            : '<i class="bi bi-check-circle-fill text-success flex-shrink-0 mt-1"></i>';
        return '<div class="task-activity-row d-flex align-items-start gap-2 mb-1 small">' +
            icon + '<span class="flex-grow-1">' + _shaclEsc((entry && entry.message) || '') + '</span></div>';
    }).join('');
    el.scrollTop = el.scrollHeight;
}

function _pollShaclTask(taskId) {
    return new Promise(function (resolve, reject) {
        _shaclStopPoll();
        const tick = async function () {
            try {
                const resp = await fetch('/tasks/' + encodeURIComponent(taskId),
                    { credentials: 'same-origin' });
                const data = await resp.json();
                const task = data.task;
                if (!task) return;
                _shaclRenderProgressLog(task);
                if (task.status === 'completed') {
                    _shaclStopPoll();
                    resolve(task.result || {});
                } else if (task.status === 'failed') {
                    _shaclStopPoll();
                    reject(new Error(task.message || 'Checks failed'));
                }
            } catch (err) {
                console.error('[SHACL facets] Poll error:', err);
            }
        };
        tick();
        _shaclPollTimer = setInterval(tick, 1500);
    });
}

async function checkShaclFacets(entityUri) {
    try {
        const resp = await fetch('/dtwin/nodes/shacl/start', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ entity_uri: entityUri }),
        });
        const started = await resp.json();
        if (!resp.ok || !started.success || !started.task_id) {
            throw new Error(started.message || started.detail || ('HTTP ' + resp.status));
        }
        const data = await _pollShaclTask(started.task_id);
        if (!data || data.success === false) {
            throw new Error((data && (data.message || data.detail)) || 'Checks failed');
        }
        _shaclCache[entityUri] = { data: data, checkedAt: new Date() };
        document.dispatchEvent(new CustomEvent('shacl-facets:updated', { detail: { entityUri: entityUri } }));
        return data;
    } catch (err) {
        console.error('[SHACL facets] Check failed:', err);
        if (typeof showNotification === 'function') {
            showNotification('Could not run the data quality checks: ' + err.message, 'error', 5000);
        }
        return null;
    }
}

// ---------------------------------------------------------------------------
// Popup
// ---------------------------------------------------------------------------

/**
 * Open the facet popup for *entityUri*. A cached result shows immediately;
 * otherwise (or with ``recheck``) the check runs first.
 */
async function openShaclFacetsModal(entityUri, recheck) {
    if (!entityUri) return;
    const modalEl = document.getElementById('shaclFacetsModal');
    if (!modalEl || typeof bootstrap === 'undefined') return;
    if (_shaclCurrentUri !== entityUri) _shaclFilter = 'all';
    _shaclCurrentUri = entityUri;
    bootstrap.Modal.getOrCreateInstance(modalEl).show();

    if (_shaclCache[entityUri] && !recheck) {
        _shaclStopPoll();
        _shaclRenderModal(entityUri);
        return;
    }
    _shaclRenderLoading(entityUri);
    await checkShaclFacets(entityUri);
    if (_shaclCurrentUri === entityUri) _shaclRenderModal(entityUri);
}

function _shaclSetProgressVisible(visible) {
    const el = document.getElementById('shaclFacetsProgress');
    if (el) el.classList.toggle('d-none', !visible);
}

function _shaclRenderLoading(entityUri) {
    _shaclSetText('shaclFacetsEntity', _shaclLocalName(entityUri));
    _shaclSetText('shaclFacetsSubtitle', entityUri);
    _shaclSetText('shaclFacetsCheckedAt', '');
    _shaclSetHtml('shaclFacetsSummary', '');
    _shaclSetHtml('shaclFacetsGauges', '');
    _shaclSetHtml('shaclFacetsBody', '');
    _shaclSetProgressVisible(true);
    _shaclRenderProgressLog({ log_entries: [], status: 'pending', message: 'Running data quality checks…' });
    const btn = document.getElementById('shaclFacetsRecheck');
    if (btn) btn.disabled = true;
}

function _shaclRenderModal(entityUri) {
    const entry = _shaclCache[entityUri];
    const btn = document.getElementById('shaclFacetsRecheck');
    if (btn) btn.disabled = false;
    _shaclSetProgressVisible(false);
    const body = document.getElementById('shaclFacetsBody');
    if (!body) return;
    if (!entry) {
        body.innerHTML = '<p class="text-muted small mb-0">The check did not complete. Use Re-check to try again.</p>';
        return;
    }

    const data = entry.data;
    const classNames = (data.classes || []).map(function (c) { return c.name; }).filter(Boolean);
    _shaclSetText('shaclFacetsEntity', data.entity_label || data.entity_local_id || _shaclLocalName(entityUri));
    const lineage = data.lineage || [];
    _shaclSetText('shaclFacetsSubtitle',
        (lineage.length ? lineage.join(' → ') : classNames.join(', ')) + ' · ' + entityUri);
    _shaclSetText('shaclFacetsCheckedAt', 'Checked at ' + entry.checkedAt.toLocaleTimeString());

    const facets = data.facets || [];
    if (!facets.length) {
        _shaclSetHtml('shaclFacetsSummary', '');
        _shaclSetHtml('shaclFacetsGauges', '');
        body.innerHTML = '<div class="text-center text-muted py-4">' +
            '<i class="bi bi-shield fs-3 d-block mb-2" aria-hidden="true"></i>' +
            'No SHACL shape applies to ' + _shaclEsc(classNames.join(', ') || 'this entity') + '.</div>';
        return;
    }

    const groups = _shaclGroups(facets);
    _shaclSetHtml('shaclFacetsGauges', _shaclGaugesHtml(groups));
    _shaclSetHtml('shaclFacetsSummary', _shaclChipsHtml(data.summary || {}, facets.length));
    body.innerHTML = _shaclCategoriesHtml(groups);
    _shaclApplyFilter();
}

/** Dimensions present in *facets*, in the Data Quality tab order, each with its rows. */
function _shaclGroups(facets) {
    const known = SHACL_CATEGORIES.map(function (c) { return c.id; });
    const extra = Array.from(new Set(facets.map(function (f) { return f.category || 'other'; })))
        .filter(function (id) { return known.indexOf(id) === -1; })
        .map(function (id) { return { id: id, label: id.charAt(0).toUpperCase() + id.slice(1), icon: 'bi-shield' }; });
    return SHACL_CATEGORIES.concat(extra).map(function (cat, idx) {
        const rows = facets.filter(function (f) { return (f.category || 'other') === cat.id; });
        return Object.assign({ rows: rows, collapseId: 'shaclCat' + idx }, cat);
    }).filter(function (g) { return g.rows.length; });
}

/**
 * One half-gauge per dimension: passed / (passed + failed). Shapes that were
 * not applicable, not evaluated or errored are not part of the score; a
 * dimension with none checked shows a dash instead of a misleading 100%.
 */
function _shaclGaugesHtml(groups) {
    return groups.map(function (g) {
        const passed = g.rows.filter(function (f) { return f.status === 'pass'; }).length;
        const failed = g.rows.filter(function (f) { return f.status === 'fail'; }).length;
        const checked = passed + failed;
        const score = checked ? Math.round(passed * 100 / checked) : null;
        const tone = score === null ? 'muted' : score === 100 ? 'success' : score >= 80 ? 'warning' : 'danger';
        const detail = checked ? passed + ' of ' + checked + ' passed' : 'not checked';
        return '<button type="button" class="shacl-gauge shacl-gauge--' + tone + '" data-shacl-gauge="' + g.collapseId + '"' +
            ' aria-controls="' + g.collapseId + '" title="Show ' + _shaclEsc(g.label) + ' checks">' +
            '<svg class="shacl-gauge-arc" viewBox="0 0 100 56" aria-hidden="true">' +
            '<path class="shacl-gauge-track" d="M 10 50 A 40 40 0 0 1 90 50" pathLength="100"/>' +
            (score ? '<path class="shacl-gauge-value" d="M 10 50 A 40 40 0 0 1 90 50" pathLength="100" stroke-dasharray="' + score + ' 100"/>' : '') +
            '</svg>' +
            '<span class="shacl-gauge-score">' + (score === null ? '—' : score + '%') + '</span>' +
            '<span class="shacl-gauge-label"><i class="bi ' + g.icon + ' me-1" aria-hidden="true"></i>' + _shaclEsc(g.label) + '</span>' +
            '<span class="shacl-gauge-detail">' + detail + '</span>' +
            '</button>';
    }).join('');
}

function _shaclExpandSection(collapseId, scroll) {
    const el = document.getElementById(collapseId);
    if (!el || typeof bootstrap === 'undefined') return;
    bootstrap.Collapse.getOrCreateInstance(el, { toggle: false }).show();
    if (scroll) el.closest('.accordion-item').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function _shaclChipsHtml(summary, total) {
    let html = '<button type="button" class="shacl-chip" data-shacl-filter="all" aria-pressed="false">' +
        '<i class="bi bi-list-ul" aria-hidden="true"></i> All <span class="shacl-chip-count">' + total + '</span></button>';
    Object.keys(SHACL_STATUS).forEach(function (status) {
        const count = summary[status] || 0;
        if (!count) return;
        const meta = SHACL_STATUS[status];
        html += '<button type="button" class="shacl-chip shacl-tone-' + meta.tone + '" data-shacl-filter="' + status +
            '" aria-pressed="false"><i class="bi ' + meta.icon + '" aria-hidden="true"></i> ' + meta.label +
            ' <span class="shacl-chip-count">' + count + '</span></button>';
    });
    return html;
}

function _shaclCategoriesHtml(groups) {
    let html = '<div class="accordion shacl-facets-accordion" id="shaclFacetsAccordion">';
    groups.forEach(function (g) {
        const failed = g.rows.filter(function (f) { return f.status === 'fail'; }).length;
        html += '<div class="accordion-item" data-shacl-category="' + _shaclEsc(g.id) + '">' +
            '<h2 class="accordion-header"><button class="accordion-button collapsed py-2" type="button" ' +
            'data-bs-toggle="collapse" data-bs-target="#' + g.collapseId + '" aria-expanded="false" aria-controls="' + g.collapseId + '">' +
            '<i class="bi ' + g.icon + ' me-2" aria-hidden="true"></i>' + _shaclEsc(g.label) +
            '<span class="ms-2 small text-muted">' + g.rows.length + (failed ? ' · ' + failed + ' failed' : '') + '</span>' +
            '</button></h2>' +
            '<div id="' + g.collapseId + '" class="accordion-collapse collapse">' +
            '<div class="accordion-body p-0"><ul class="list-group list-group-flush">' +
            g.rows.map(_shaclFacetRowHtml).join('') +
            '</ul></div></div></div>';
    });
    return html + '</div>';
}

function _shaclFacetRowHtml(f) {
    const meta = SHACL_STATUS[f.status] || SHACL_STATUS.not_evaluated;
    const severity = (f.severity || 'sh:Violation').replace(/^sh:/, '');
    let html = '<li class="list-group-item shacl-facet" data-shacl-status="' + _shaclEsc(f.status) + '">' +
        '<div class="d-flex align-items-start gap-2">' +
        '<span class="shacl-status shacl-tone-' + meta.tone + '" title="' + meta.label + '">' +
        '<i class="bi ' + meta.icon + '" aria-hidden="true"></i>' +
        '<span class="visually-hidden">' + meta.label + '</span></span>' +
        '<div class="flex-grow-1 shacl-facet-main">' +
        '<div class="fw-semibold">' + _shaclEsc(f.label || f.shape_id) +
        (f.status === 'fail' ? ' <span class="badge shacl-severity shacl-severity-' + severity.toLowerCase() + '">' + _shaclEsc(severity) + '</span>' : '') +
        '</div>';

    const target = [f.property_path, f.constraint].filter(Boolean).join(' · ');
    if (target) html += '<div class="small text-muted"><code>' + _shaclEsc(target) + '</code></div>';
    if (f.inherited_from) {
        html += '<div class="small text-muted"><i class="bi bi-diagram-2 me-1" aria-hidden="true"></i>inherited from ' +
            _shaclEsc(f.inherited_from) + '</div>';
    }
    if ((f.status === 'fail' || f.status === 'pass') && f.property_path) {
        const values = f.values || [];
        const found = values.length
            ? values.map(function (v) { return '<code>' + _shaclEsc(v) + '</code>'; }).join(', ')
            : '<span class="text-muted">(none)</span>';
        html += '<div class="small">Found: ' + found +
            (f.values_truncated ? ' <span class="text-muted">+ more values not shown</span>' : '') + '</div>';
    }
    if (f.message && f.status !== 'pass') {
        html += '<div class="small shacl-facet-message">' + _shaclEsc(f.message) + '</div>';
    }
    return html + '</div></div></li>';
}

function _shaclApplyFilter() {
    document.querySelectorAll('#shaclFacetsSummary [data-shacl-filter]').forEach(function (chip) {
        chip.setAttribute('aria-pressed', chip.getAttribute('data-shacl-filter') === _shaclFilter ? 'true' : 'false');
    });
    document.querySelectorAll('#shaclFacetsBody .shacl-facet').forEach(function (row) {
        const show = _shaclFilter === 'all' || row.getAttribute('data-shacl-status') === _shaclFilter;
        row.classList.toggle('d-none', !show);
    });
    document.querySelectorAll('#shaclFacetsBody [data-shacl-category]').forEach(function (item) {
        const hasRows = !!item.querySelector('.shacl-facet:not(.d-none)');
        item.classList.toggle('d-none', !hasRows);
        // A status filter reveals its matches; "All" leaves the sections as they are.
        if (hasRows && _shaclFilter !== 'all') {
            _shaclExpandSection(item.querySelector('.accordion-collapse').id, false);
        }
    });
}

// ---------------------------------------------------------------------------
// Detail panel
// ---------------------------------------------------------------------------

/**
 * Render the Data quality section body of the node detail panel: the shape
 * count and a Check button, then — once checked — the summary with Details
 * and Re-check.
 */
function renderShaclFacetSection(entityUri, shapeCount) {
    return '<div class="shacl-panel" data-shacl-panel="' + _shaclEsc(entityUri) + '" data-shacl-count="' +
        (shapeCount || 0) + '">' + _shaclPanelInner(entityUri, shapeCount) + '</div>';
}

function _shaclPanelInner(entityUri, shapeCount) {
    const entry = _shaclCache[entityUri];
    const safeUri = _shaclEsc(entityUri);
    if (!entry) {
        return '<p class="small text-muted mb-2">' +
            (shapeCount === 1 ? '1 SHACL shape applies' : shapeCount + ' SHACL shapes apply') +
            ' to this entity.</p>' +
            '<button type="button" class="btn btn-sm btn-outline-primary w-100" data-shacl-action="check" data-uri="' + safeUri + '">' +
            '<i class="bi bi-shield-check me-1" aria-hidden="true"></i>Check</button>';
    }
    const s = entry.data.summary || {};
    let chips = '';
    Object.keys(SHACL_STATUS).forEach(function (status) {
        if (!s[status]) return;
        const meta = SHACL_STATUS[status];
        chips += '<span class="shacl-chip shacl-chip--static shacl-tone-' + meta.tone + '">' +
            '<i class="bi ' + meta.icon + '" aria-hidden="true"></i> ' + s[status] + ' ' + meta.label + '</span>';
    });
    return '<div class="shacl-facets-summary mb-2">' + (chips || '<span class="small text-muted">No SHACL shape applies</span>') + '</div>' +
        '<div class="small text-muted mb-2">Checked at ' + entry.checkedAt.toLocaleTimeString() + '</div>' +
        '<div class="d-flex gap-1">' +
        '<button type="button" class="btn btn-sm btn-outline-primary flex-grow-1" data-shacl-action="details" data-uri="' + safeUri + '">' +
        '<i class="bi bi-list-check me-1" aria-hidden="true"></i>Details</button>' +
        '<button type="button" class="btn btn-sm btn-outline-secondary" data-shacl-action="recheck" data-uri="' + safeUri + '">' +
        '<i class="bi bi-arrow-repeat me-1" aria-hidden="true"></i>Re-check</button></div>';
}

function _shaclRefreshPanels(entityUri) {
    document.querySelectorAll('[data-shacl-panel]').forEach(function (panel) {
        if (panel.getAttribute('data-shacl-panel') !== entityUri) return;
        panel.innerHTML = _shaclPanelInner(entityUri, parseInt(panel.getAttribute('data-shacl-count'), 10) || 0);
    });
}

// ---------------------------------------------------------------------------
// Helpers and wiring
// ---------------------------------------------------------------------------

function _shaclSetText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text || '';
}

function _shaclSetHtml(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
}

function _shaclLocalName(uri) {
    return String(uri || '').split('#').pop().split('/').pop();
}

document.addEventListener('click', function (e) {
    const chip = e.target.closest('#shaclFacetsSummary [data-shacl-filter]');
    if (chip) {
        const value = chip.getAttribute('data-shacl-filter');
        _shaclFilter = (value === _shaclFilter) ? 'all' : value;
        _shaclApplyFilter();
        return;
    }
    const gauge = e.target.closest('#shaclFacetsGauges [data-shacl-gauge]');
    if (gauge) {
        _shaclExpandSection(gauge.getAttribute('data-shacl-gauge'), true);
        return;
    }
    if (e.target.closest('#shaclFacetsRecheck')) {
        if (_shaclCurrentUri) openShaclFacetsModal(_shaclCurrentUri, true);
        return;
    }
    if (e.target.closest('#shaclFacetsOpenDq')) {
        const modalEl = document.getElementById('shaclFacetsModal');
        if (modalEl) bootstrap.Modal.getOrCreateInstance(modalEl).hide();
        const link = document.querySelector('[data-section="dataquality"]');
        if (link) link.click();
        return;
    }
    const action = e.target.closest('[data-shacl-action]');
    if (!action) return;
    const uri = action.getAttribute('data-uri');
    const kind = action.getAttribute('data-shacl-action');
    if (kind === 'details') {
        openShaclFacetsModal(uri);
    } else {
        action.disabled = true;
        action.innerHTML = '<span class="spinner-border spinner-border-sm me-1" aria-hidden="true"></span>Checking…';
        checkShaclFacets(uri).then(function (data) {
            if (!data) _shaclRefreshPanels(uri);
        });
    }
});

document.addEventListener('shacl-facets:updated', function (e) {
    _shaclRefreshPanels(e.detail && e.detail.entityUri);
});

window.openShaclFacetsModal = openShaclFacetsModal;
window.renderShaclFacetSection = renderShaclFacetSection;
window.getShaclFacetStatus = getShaclFacetStatus;
window.shaclStatusColor = shaclStatusColor;
window.shaclShapeCountFor = shaclShapeCountFor;
