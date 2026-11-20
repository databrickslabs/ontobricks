/**
 * OntoBricks — ontology-evolve.js
 * Ontology → Evolve: question → coverage → proposed fix (→ evaluation).
 * Every call names the loaded context folder and parent version explicitly.
 */
window.EvolveModule = {
    _ctx: null,
    _run: null,
    _bound: false,

    STATE_CLASSES: {
        RESOLVED: 'bg-success',
        AMBIGUOUS: 'bg-warning text-dark',
        MISSING_SEMANTICS: 'bg-danger',
        INACCESSIBLE: 'bg-secondary',
        STALE: 'bg-secondary',
        UNVERIFIED: 'bg-warning text-dark',
        UNSUPPORTED: 'bg-dark',
        SOURCE_UNAVAILABLE: 'bg-secondary',
    },

    REASONS: {
        mapping_checked: 'Mapping checked on the current data.',
        value_checked: 'Filter value checked on the current data.',
        relationship_not_in_ontology: 'The data has a join, but the ontology has no relationship for it.',
        several_joins: 'More than one join is valid in the data. Choose one before continuing.',
        several_paths: 'More than one path connects these entities. Choose one.',
        several_classes: 'This phrase could mean more than one entity.',
        value_matches_several_entities: 'More than one entity has this name.',
        mapping_dangling: 'The mapping points at rows that do not exist.',
        relationship_not_mapped: 'The relationship exists but has no mapping.',
        identity_not_unique: 'The entity id is not unique in its table.',
        access_denied: 'You cannot read this source.',
        unavailable: 'The source could not be found or inspected.',
        aggregation_not_supported_v1: 'Totals and other aggregates are not supported yet.',
        filter_op_not_supported_v1: 'Only "equals" filters are supported yet.',
        path_exceeds_budget: 'The entities are too far apart for this check.',
        class_not_in_ontology: 'The ontology has no such entity. Evolve cannot add entities yet.',
        no_join_evidence: 'No join in the data supports a link.',
    },

    DEFAULT_INTENT: {
        output_class: 'Order',
        mentions: ['orders', 'products', 'Acme'],
        filters: [{ class: 'Supplier', attribute: 'supplierName', op: 'eq', value: 'Acme' }],
    },

    async init() {
        if (!this._bound) {
            document.getElementById('evoCheckBtn').addEventListener('click', () => this.check());
            document.getElementById('evoProposeBtn').addEventListener('click', () => this.propose());
            this._bound = true;
        }
        const intentBox = document.getElementById('evoIntent');
        if (!intentBox.value) intentBox.value = JSON.stringify(this.DEFAULT_INTENT, null, 2);
        const caps = await this._get('/evolution/capabilities');
        const disabled = document.getElementById('evoDisabled');
        if (!caps || !caps.enabled) {
            disabled.textContent = 'Evolve is turned off for this deployment (ONTOBRICKS_EVOLUTION_ENABLED).';
            disabled.classList.remove('d-none');
            document.getElementById('evoBody').classList.add('d-none');
            return;
        }
        if (!caps.execution_supported) {
            disabled.textContent = 'Coverage works on this graph backend, but candidate evaluation needs the Lakehouse (Delta) backend.';
            disabled.classList.remove('d-none');
        }
        this._ctx = await this._get('/domain/version-status');
    },

    async _get(url) {
        try {
            const resp = await fetch(url, { credentials: 'same-origin' });
            return resp.ok ? await resp.json() : null;
        } catch (_) {
            return null;
        }
    },

    async _post(url, body) {
        const resp = await fetch(url, {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(Object.assign({ domain: this._folder() }, body)),
        });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok) {
            const msg = (data && (data.message || data.detail || data.error)) || 'Request failed';
            throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
        }
        return data;
    },

    _folder() {
        return (this._ctx && this._ctx.domain_folder) || '';
    },

    async check() {
        let intent;
        try {
            intent = JSON.parse(document.getElementById('evoIntent').value);
        } catch (_) {
            showNotification('The intent is not valid JSON', 'warning');
            return;
        }
        if (!this._folder()) {
            showNotification('Save the context to the registry before using Evolve', 'warning');
            return;
        }
        const btn = document.getElementById('evoCheckBtn');
        btn.disabled = true;
        try {
            const created = await this._post('/evolution/runs', {
                parent_version: String(this._ctx.version || ''),
                question: document.getElementById('evoQuestion').value,
                intent: intent,
                idempotency_key: (crypto.randomUUID && crypto.randomUUID()) || String(Date.now()),
            });
            const run = created.run;
            const assessed = await this._post('/evolution/runs/' + encodeURIComponent(run.id) + '/assess',
                { revision: run.revision });
            this._run = assessed.run;
            this._renderCoverage(this._run.coverage);
            document.getElementById('evoProposalCard').classList.add('d-none');
        } catch (err) {
            showNotification('Coverage check failed: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },

    async propose() {
        if (!this._run) return;
        const btn = document.getElementById('evoProposeBtn');
        btn.disabled = true;
        try {
            const out = await this._post('/evolution/runs/' + encodeURIComponent(this._run.id) + '/proposal',
                { revision: this._run.revision });
            this._run = out.run;
            this._renderProposal(this._run.proposal);
        } catch (err) {
            showNotification('Could not build a proposal: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },

    _renderCoverage(cov) {
        document.getElementById('evoCoverageCard').classList.remove('d-none');
        document.getElementById('evoRunInfo').textContent =
            'Run ' + this._run.id + ' on version ' + this._run.parent_version;
        const badge = document.getElementById('evoReadyBadge');
        badge.className = 'badge ' + (cov.ready ? 'bg-success' : 'bg-secondary');
        badge.textContent = cov.ready ? 'Supported as is' : 'Not supported yet';

        const rows = (cov.obligations || []).map((o) => {
            const cls = this.STATE_CLASSES[o.state] || 'bg-secondary';
            return '<tr><td><code>' + escapeHtml(o.key) + '</code></td>' +
                '<td><span class="badge ' + cls + '">' + escapeHtml(o.state) + '</span></td>' +
                '<td class="small">' + escapeHtml(this.REASONS[o.reason] || o.reason || '') + '</td></tr>';
        });
        document.getElementById('evoObligations').innerHTML = rows.join('');

        const gap = cov.gap;
        document.getElementById('evoGap').innerHTML = !gap ? '' :
            '<div class="alert alert-light border small mb-2"><strong>Gap:</strong> ' + escapeHtml(this._gapText(gap)) + '</div>';

        const join = cov.join || {};
        const cands = join.candidates || [];
        document.getElementById('evoJoins').innerHTML = !cands.length ? '' :
            '<h6 class="small fw-semibold">Joins found in the data</h6><ul class="small evo-joins">' +
            cands.map((c) => '<li' + (join.chosen && join.chosen.source_column === c.source_column &&
                join.chosen.target_column === c.target_column ? ' class="evo-chosen"' : '') + '><code>' +
                escapeHtml(c.source_table.split('.').pop() + '.' + c.source_column + ' = ' +
                    c.target_table.split('.').pop() + '.' + c.target_column) + '</code> — ' +
                escapeHtml(c.evidence.join(', ').replace(/_/g, ' ')) +
                (c.data_valid ? '' : ' <span class="text-danger">(' + escapeHtml(c.reasons.join(', ')) + ')</span>') +
                '</li>').join('') + '</ul>';

        document.getElementById('evoProposeBtn').classList.toggle('d-none', cov.decision_hint !== 'PROPOSE');
    },

    _gapText(gap) {
        if (gap.kind === 'relationship') return 'no relationship from ' + gap.source + ' to ' + gap.target + '.';
        if (gap.kind === 'mapping') return 'the mapping of ' + gap.property + ' does not match the data.';
        if (gap.kind === 'class') return 'the ontology has no entity "' + gap.name + '".';
        return JSON.stringify(gap);
    },

    _renderProposal(proposal) {
        document.getElementById('evoProposalCard').classList.remove('d-none');
        const alt = proposal.alternatives[proposal.selected || 0];
        const val = proposal.validation || { checks: [], passed: false };
        const fact = (label, ok, pending) => '<span class="badge ' +
            (pending ? 'bg-light text-dark border' : (ok ? 'bg-success' : 'bg-danger')) + ' me-1">' +
            escapeHtml(label) + '</span>';
        document.getElementById('evoFacts').innerHTML =
            fact('Join found in the data', true) +
            fact(val.passed ? 'Mapping validated on current data' : 'Mapping failed validation', val.passed) +
            fact('Business meaning: needs review', false, true) +
            fact('Result verified: pending evaluation', false, true);
        document.getElementById('evoOps').innerHTML = alt.operations.map((op) => {
            const p = op.params || {};
            if (op.op === 'add_relationship') {
                return '<div class="evo-op"><strong>Add relationship</strong> <code>' + escapeHtml(op.target.name) +
                    '</code>: ' + escapeHtml(p.domain + ' → ' + p.range) + '<div class="small text-muted">' +
                    escapeHtml(p.description || '') + '</div></div>';
            }
            return '<div class="evo-op"><strong>Set mapping</strong> of <code>' + escapeHtml(op.target.name) +
                '</code><pre class="evo-sql">' + escapeHtml(p.sql_query) + '</pre><div class="small">ids: ' +
                escapeHtml(p.source_id_column + ' → ' + p.target_id_column) + '</div></div>';
        }).join('') + '<p class="small text-muted mt-2">' + escapeHtml(alt.rationale || '') + '</p>';
        document.getElementById('evoChecks').innerHTML = (val.checks || []).map((c) =>
            '<li><i class="bi ' + (c.status === 'pass' ? 'bi-check-circle text-success' :
                c.status === 'fail' ? 'bi-x-circle text-danger' : 'bi-person-check text-secondary') +
            ' me-1"></i>' + escapeHtml(c.name.replace(/_/g, ' ')) +
            (c.reason ? ' — ' + escapeHtml(c.reason.replace(/_/g, ' ')) : '') + '</li>').join('');
        document.getElementById('evoEvaluationCard').classList.toggle('d-none', !val.passed);
    },
};
