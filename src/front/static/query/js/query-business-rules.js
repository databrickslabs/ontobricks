/**
 * Entity business rules in the Graph Explorer.
 *
 * A business rule is an IF-THEN rule declared on the entity's ontology class
 * (Ontology > Studio > Actions). Triggering it runs the rule for this
 * entity only and writes the inferred triples to the graph, so it needs the
 * Builder role and an explicit confirmation (request -> confirm token).
 */

function canRunBusinessRules() {
    const perms = window.OB && window.OB.permissions;
    return !perms || typeof perms.hasDomainRole !== 'function' || perms.hasDomainRole('builder');
}

/**
 * Render IF-THEN rule buttons for the Explorer Behaviour section.
 *
 * @param {string} entityUri Instance URI of the node.
 * @param {Array} rules Class business rules: [{ name, description }].
 * @returns {string} HTML for the section body.
 */
function renderBusinessRuleSection(entityUri, rules) {
    const esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { return String(s == null ? '' : s); };
    const allowed = canRunBusinessRules();
    const safeUri = esc(entityUri).replace(/'/g, "\\'");
    return (rules || []).map(function (rule) {
        const name = rule && rule.name;
        if (!name) return '';
        const safeName = esc(name).replace(/'/g, "\\'");
        const title = allowed ? (rule.description || ('Apply ' + name)) : 'Builder role required';
        return '<div class="entity-detail-item"><button type="button" ' +
            (allowed ? 'onclick="openEntityBusinessRuleModal(\'' + safeUri + '\', \'' + safeName + '\')" ' : 'disabled ') +
            'class="btn btn-sm btn-outline-primary w-100" title="' + esc(title) + '">' +
            '<i class="bi bi-diagram-3 me-1"></i>' + esc(name) + '</button></div>';
    }).join('');
}

/**
 * Open the confirmation modal for a business rule, then run it.
 *
 * @param {string} entityUri Full entity URI.
 * @param {string} ruleName Business rule (IF-THEN rule) name.
 */
async function openEntityBusinessRuleModal(entityUri, ruleName) {
    const esc = (typeof escapeHtml === 'function') ? escapeHtml : function (s) { return String(s == null ? '' : s); };
    const modalId = 'entityBusinessRuleModal';
    document.getElementById(modalId)?.remove();

    const modalHtml = `
        <div class="modal fade" id="${modalId}" tabindex="-1" aria-labelledby="${modalId}Label" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered modal-lg modal-dialog-scrollable">
                <div class="modal-content">
                    <div class="modal-header bg-dark text-white py-2">
                        <h5 class="modal-title" id="${modalId}Label">
                            <i class="bi bi-diagram-3 me-2"></i>${esc(ruleName)}
                        </h5>
                        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="${modalId}Body">
                        <div class="text-center text-muted py-4">
                            <div class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></div>
                            Checking the rule…
                        </div>
                    </div>
                    <div class="modal-footer py-2">
                        <button type="button" class="btn btn-sm btn-secondary" data-bs-dismiss="modal">Close</button>
                        <button type="button" class="btn btn-sm btn-primary d-none" id="${modalId}Run">
                            <i class="bi bi-play-fill me-1"></i>Apply rule
                        </button>
                    </div>
                </div>
            </div>
        </div>
    `;
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    const modalEl = document.getElementById(modalId);
    const body = document.getElementById(modalId + 'Body');
    const runBtn = document.getElementById(modalId + 'Run');
    let token = null;
    let consumed = false;

    modalEl.addEventListener('hidden.bs.modal', function () {
        if (token && !consumed) _brPost('/dtwin/nodes/business-rule/cancel', { token: token }).catch(function () {});
        modalEl.remove();
    });
    new bootstrap.Modal(modalEl, { backdrop: true, keyboard: true }).show();

    let pending;
    try {
        const data = await _brPost('/dtwin/nodes/business-rule/request', _brRequestBody(entityUri, ruleName));
        pending = data.pending_business_rule;
        token = pending.token;
    } catch (err) {
        body.innerHTML = `<div class="alert alert-danger mb-0">${esc(err.message)}</div>`;
        return;
    }

    body.innerHTML = `
        ${pending.description ? `<p class="text-muted small mb-2">${esc(pending.description)}</p>` : ''}
        <div class="mb-2 small"><span class="fw-semibold">Entity:</span> <code>${esc(pending.entity_label)}</code></div>
        <div class="p-2 border rounded bg-light mb-3">
            ${_brIfThenHtml(pending.antecedent, pending.consequent, esc)}
        </div>
        <div class="alert alert-warning py-2 mb-0 small">
            <i class="bi bi-exclamation-triangle me-1"></i>
            The rule is evaluated for this entity only. Inferred triples will be
            <strong>${pending.simulation_id ? 'written to the simulation sandbox' : 'written to the graph'}</strong>.
        </div>`;
    runBtn.classList.remove('d-none');

    runBtn.addEventListener('click', async function () {
        runBtn.disabled = true;
        consumed = true;
        body.innerHTML = `
            <div class="text-center text-muted py-4">
                <div class="spinner-border spinner-border-sm me-2" role="status" aria-hidden="true"></div>
                Applying the rule…
            </div>`;
        try {
            const result = await _brPost('/dtwin/nodes/business-rule/confirm', { token: token });
            runBtn.classList.add('d-none');
            body.innerHTML = _brRenderResult(result, esc);
            if (result.materialized_count > 0) {
                if (typeof showNotification === 'function') {
                    showNotification(`${result.materialized_count} triple(s) materialised by ${ruleName}`, 'success');
                }
                await _brRefreshGraph(entityUri);
            } else if (typeof showNotification === 'function') {
                showNotification('The rule produced no new facts for this entity', 'info');
            }
        } catch (err) {
            body.innerHTML = `<div class="alert alert-danger mb-0">${esc(err.message)}</div>`;
        }
    }, { once: true });
}

/** Readable words for the comparison built-ins (``builtin:`` prefix). */
const _BR_BUILTIN_WORDS = {
    greaterThan: '>', greaterThanOrEqual: '≥', lessThan: '<', lessThanOrEqual: '≤',
    equal: '=', notEqual: '≠', startsWith: 'starts with', endsWith: 'ends with', contains: 'contains'
};

/** Atom shape: optional ``prefix:`` then ``name(args)``. */
const _BR_ATOM_RE = /^(?:([A-Za-z]+):)?([A-Za-z_][\w.-]*)\s*\(([\s\S]*)\)$/;

/**
 * Split on top-level separators, ignoring those inside parentheses or quotes.
 *
 * @param {string} text Authored rule text.
 * @param {string[]} separators Single-character separators.
 * @returns {string[]} Trimmed, non-empty parts.
 */
function _brSplitTopLevel(text, separators) {
    const parts = [];
    let depth = 0;
    let quoted = false;
    let cur = '';
    for (const ch of String(text || '')) {
        if (ch === '"') quoted = !quoted;
        else if (!quoted && ch === '(') depth++;
        else if (!quoted && ch === ')') depth--;
        if (!quoted && depth === 0 && separators.includes(ch)) {
            if (cur.trim()) parts.push(cur.trim());
            cur = '';
        } else {
            cur += ch;
        }
    }
    if (cur.trim()) parts.push(cur.trim());
    return parts;
}

/** Variables read as plain names (``?x`` -> ``x``); literals are kept as-is. */
function _brArg(arg, esc) {
    const a = String(arg).trim();
    return esc(a.startsWith('?') ? a.slice(1) : a);
}

/**
 * One atom as a plain sentence: ``Customer(?x)`` -> "x is a Customer",
 * ``hasclaim(?x, ?y)`` -> "x hasclaim y", ``builtin:contains(?s, "a")`` ->
 * "s contains "a"". Anything unrecognised falls back to ``name(args)``.
 */
function _brAtomPhrase(atom, esc) {
    const m = _BR_ATOM_RE.exec(atom);
    if (!m) return esc(atom);
    const prefix = m[1] || '';
    const name = m[2];
    const raw = _brSplitTopLevel(m[3], [',']);
    const args = raw.map(function (a) { return _brArg(a, esc); });
    // 'swrlb' is the builtin prefix of rules saved before the rename.
    if ((prefix === 'builtin' || prefix === 'swrlb') && args.length === 2) {
        return `${args[0]} ${esc(_BR_BUILTIN_WORDS[name] || name)} ${args[1]}`;
    }
    if (args.length === 1) return `${args[0]} is a ${esc(name)}`;
    if (args.length === 2) {
        // A literal object (quoted, numeric or boolean) reads as "attribute = value".
        const literal = /^("|-?\d|true$|false$)/.test(raw[1]);
        return literal
            ? `${args[0]} ${esc(name)} = ${args[1]}`
            : `${args[0]} ${esc(name)} ${args[1]}`;
    }
    return `${esc(name)}(${args.join(', ')})`;
}

function _brSideItems(body, esc) {
    const atoms = _brSplitTopLevel(body, ['^', '∧']);
    if (!atoms.length) return '<li class="text-muted">none</li>';
    return atoms.map(function (a) { return `<li>${_brAtomPhrase(a, esc)}</li>`; }).join('');
}

/**
 * Read-only IF / THEN view of a business rule, in the editor's colours
 * (IF blue, THEN red). The stored IF-THEN text is never shown.
 *
 * @param {string} antecedent Stored IF body.
 * @param {string} consequent Stored THEN body.
 * @param {function(string): string} esc HTML escaper.
 * @returns {string} HTML.
 */
function _brIfThenHtml(antecedent, consequent, esc) {
    return '<div class="small">' +
        '<div class="fw-semibold text-primary">IF</div>' +
        `<ul class="list-unstyled ps-3 mb-2">${_brSideItems(antecedent, esc)}</ul>` +
        '<div class="fw-semibold text-danger">THEN</div>' +
        `<ul class="list-unstyled ps-3 mb-0">${_brSideItems(consequent, esc)}</ul>` +
        '</div>';
}

function _brSimulationId() {
    if (typeof KgGraphSource !== 'undefined') {
        return KgGraphSource.current().simulationId || '';
    }
    const sel = document.getElementById('sgSimulationSelect');
    const raw = (sel && sel.value) ? String(sel.value) : '';
    if (!raw || raw === 'live' || raw === 'live:inferred') return '';
    return raw.replace(/:asserted$/, '');
}

function _brRequestBody(entityUri, ruleName) {
    const body = { entity_uri: entityUri, rule: ruleName };
    const simId = _brSimulationId();
    if (simId) body.simulation_id = simId;
    return body;
}

async function _brPost(url, payload) {
    const resp = await fetch(url, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });
    let data = {};
    try { data = await resp.json(); } catch (e) { /* non-JSON error body */ }
    if (!resp.ok || data.success === false) {
        throw new Error(data.message || data.error || `Request failed (${resp.status})`);
    }
    return data;
}

function _brRenderResult(result, esc) {
    const inferred = result.inferred_count || 0;
    if (!inferred) {
        return '<div class="alert alert-info mb-0"><i class="bi bi-info-circle me-1"></i>The rule produced no new facts for this entity.</div>';
    }
    const local = (typeof _extractLocalName === 'function')
        ? _extractLocalName
        : function (u) { return String(u || '').split(/[#/]/).pop(); };
    const rows = (result.triples || []).map(function (t) {
        return `<tr><td>${esc(local(t.subject))}</td><td>${esc(local(t.predicate))}</td><td>${esc(local(t.object))}</td></tr>`;
    }).join('');
    const more = result.truncated ? `<div class="small text-muted mt-1">Showing the first ${(result.triples || []).length} triples.</div>` : '';
    return `
        <div class="alert alert-success py-2 mb-3"><i class="bi bi-check-circle me-1"></i>
            ${inferred} triple(s) inferred, ${result.materialized_count || 0} written to the ${result.graph_target === 'simulation' ? 'simulation sandbox' : 'graph'}.</div>
        <div class="table-responsive"><table class="table table-sm table-striped table-bordered mb-0">
            <thead class="table-light"><tr><th>Subject</th><th>Predicate</th><th>Object</th></tr></thead>
            <tbody>${rows}</tbody>
        </table></div>${more}`;
}

async function _brRefreshGraph(entityUri) {
    if (typeof SigmaGraph === 'undefined') return;
    let refreshed = false;
    if (typeof SigmaGraph.refreshCurrentExpansion === 'function') {
        refreshed = await SigmaGraph.refreshCurrentExpansion();
    }
    if (!refreshed && typeof SigmaGraph.expandHop === 'function') {
        await SigmaGraph.expandHop(entityUri, 1);
    }
    if (typeof SigmaGraph.selectEntity === 'function') SigmaGraph.selectEntity(entityUri);
}
