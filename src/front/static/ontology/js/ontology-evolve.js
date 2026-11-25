/**
 * OntoBricks — ontology-evolve.js
 * Ontology → Evolve: question → coverage → proposed fix (→ evaluation).
 * Every call names the loaded context folder and parent version explicitly.
 */
window.EvolveModule = {
    _ctx: null,
    _run: null,
    _caps: null,
    _suggestion: null,
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
        several_joins: 'More than one join is valid in the data. Ask the assistant to rank them, then choose one.',
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
        shared_reference_path: 'These entities are connected only because both point at the same third entity (for example the same country). That is rarely what the question means; confirm the path with path_hint.',
        computed_entity_id: 'The entity id is computed in its mapping SQL. Evolve needs a key column on the table.',
        path_hint_dropped_without_citation: 'The assistant named a path without a page to back it; the path was removed so coverage chooses it.',
        unmodelled_direct_link: 'A path exists, but the catalog declares a shorter link the ontology does not model. The existing path may not mean what the question asks; keep it with path_hint.',
    },

    DEFAULT_INTENT: {
        output_class: 'Order',
        mentions: ['orders', 'products', 'Acme'],
        filters: [{ class: 'Supplier', attribute: 'supplierName', op: 'eq', value: 'Acme' }],
    },

    async init() {
        if (!this._bound) {
            document.getElementById('evoCheckBtn').addEventListener('click', () => this.check());
            document.getElementById('evoProposeBtn').addEventListener('click', () => this.propose('rule'));
            document.getElementById('evoProposeAgentBtn').addEventListener('click', () => this.propose('agent'));
            document.getElementById('evoSuggestBtn').addEventListener('click', () => this.suggest());
            document.getElementById('evoAlternatives').addEventListener('change', (e) => {
                if (e.target && e.target.name === 'evoAlt') this.select(Number(e.target.value));
            });
            document.getElementById('evoEvaluateBtn').addEventListener('click', () => this.evaluate());
            document.getElementById('evoAcceptBtn').addEventListener('click', () => this.accept());
            this._bound = true;
        }
        const intentBox = document.getElementById('evoIntent');
        if (!intentBox.value) intentBox.value = JSON.stringify(this.DEFAULT_INTENT, null, 2);
        const caps = await this._get('/evolution/capabilities');
        this._caps = caps;
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
        document.getElementById('evoSuggestBtn').classList.toggle('d-none', !caps.assistant);
        this._ctx = await this._get('/domain/version-status');
    },

    async suggest() {
        const question = document.getElementById('evoQuestion').value.trim();
        if (!question) {
            showNotification('Type the question first', 'warning');
            return;
        }
        if (!this._folder()) {
            showNotification('Save the context to the registry before using Evolve', 'warning');
            return;
        }
        const btn = document.getElementById('evoSuggestBtn');
        btn.disabled = true;
        try {
            const out = await this._post('/evolution/intent/suggest', {
                parent_version: String(this._ctx.version || ''),
                question: question,
            });
            this._suggestion = out.suggestion;
            document.getElementById('evoIntent').value = JSON.stringify(out.suggestion.intent, null, 2);
            this._renderSuggestion(out.suggestion);
        } catch (err) {
            showNotification('The assistant could not suggest an intent: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },

    _citationsHtml(citations) {
        if (!citations || !citations.length) return '';
        return '<div class="small fw-semibold mt-1">From the Genie Pages</div>' + citations.map((c) =>
            '<blockquote class="evo-quote mb-1">' + escapeHtml(this._plainQuote(c.quote)) +
            '<footer class="text-muted">' + escapeHtml(c.title || c.page) + '</footer></blockquote>').join('');
    },

    _plainQuote(text) {
        // Page bodies are markdown; show the quoted words, not the table and emphasis marks.
        return String(text || '').replace(/\*\*|`/g, '').replace(/^\s*\|\s*|\s*\|\s*$/g, '')
            .replace(/\s*\|\s*/g, ': ');
    },

    _renderSuggestion(s) {
        const agent = s.agent || {};
        const pages = ((agent.context_pages || {}).pages || []).length;
        const skipped = ((agent.context_pages || {}).skipped || []).length;
        const dropped = agent.dropped_citations || 0;
        const lines = [
            'Suggested by ' + escapeHtml(agent.model || 'the assistant') + ' from the ontology' +
                (pages ? ' and ' + pages + ' Genie Page' + (pages > 1 ? 's' : '') : ' (no Genie Pages selected)') +
                '. Check it before running coverage.',
        ];
        if (skipped) lines.push(skipped + ' selected page(s) could not be read with your access.');
        if (dropped) lines.push(dropped + ' quote(s) were not found in the pages and were dropped.');
        (s.ambiguities || []).forEach((a) => lines.push('Unclear: ' + escapeHtml(a)));
        (s.notes || []).forEach((n) => lines.push(escapeHtml(this.REASONS[n.split(':')[0]] || n) +
            ' (' + escapeHtml(n.split(':').slice(1).join(':')) + ')'));
        document.getElementById('evoSuggestInfo').innerHTML =
            '<div class="alert alert-light border small mb-0">' + lines.join('<br>') +
            this._citationsHtml(s.citations) + '</div>';
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
                suggestion: this._suggestion,
                idempotency_key: (crypto.randomUUID && crypto.randomUUID()) || String(Date.now()),
            });
            const run = created.run;
            const assessed = await this._post('/evolution/runs/' + encodeURIComponent(run.id) + '/assess',
                { revision: run.revision });
            this._run = assessed.run;
            this._renderCoverage(this._run.coverage);
            document.getElementById('evoProposalCard').classList.add('d-none');
            document.getElementById('evoEvaluationCard').classList.add('d-none');
        } catch (err) {
            showNotification('Coverage check failed: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },

    async select(index) {
        if (!this._run) return;
        try {
            const out = await this._post('/evolution/runs/' + encodeURIComponent(this._run.id) + '/proposal',
                { revision: this._run.revision, selected: index });
            this._run = out.run;
            this._renderProposal(this._run.proposal);
        } catch (err) {
            showNotification('Could not switch to that fix: ' + err.message, 'danger');
        }
    },

    async propose(source) {
        if (!this._run) return;
        const btn = document.getElementById(source === 'agent' ? 'evoProposeAgentBtn' : 'evoProposeBtn');
        btn.disabled = true;
        try {
            const out = await this._post('/evolution/runs/' + encodeURIComponent(this._run.id) + '/proposal',
                { revision: this._run.revision, source: source });
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
        document.getElementById('evoProposeAgentBtn').classList.toggle('d-none',
            !(cov.decision_hint === 'PROPOSE' || cov.choice_needed) || !(this._caps && this._caps.assistant));
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
        const alts = proposal.alternatives || [];
        const mustChoose = proposal.needs_choice && !proposal.choice_confirmed;
        document.getElementById('evoAlternatives').innerHTML = (alts.length < 2 && !proposal.needs_choice) ? '' :
            (mustChoose ? '<div class="alert alert-warning small mb-2">More than one join is valid in the data. ' +
                'The assistant ranked them; pick the one the business means. Evaluation waits for your choice.</div>' : '') +
            '<div class="small fw-semibold mb-1">Fixes the assistant suggests</div>' + alts.map((a, i) =>
                '<div class="form-check small"><input class="form-check-input" type="radio" name="evoAlt" id="evoAlt' + i +
                '" value="' + i + '"' + (i === (proposal.selected || 0) ? ' checked' : '') + '><label class="form-check-label" for="evoAlt' + i +
                '"><code>' + escapeHtml((a.operations[0].target || {}).name || '') + '</code> on <code>' +
                escapeHtml(a.join || '') + '</code></label></div>').join('') +
            (mustChoose ? '<button type="button" class="btn btn-sm btn-outline-primary mt-1" id="evoConfirmChoiceBtn">' +
                'Use this join</button>' : '');
        const confirmBtn = document.getElementById('evoConfirmChoiceBtn');
        if (confirmBtn) {
            confirmBtn.addEventListener('click', () => {
                const picked = document.querySelector('input[name="evoAlt"]:checked');
                this.select(Number(picked ? picked.value : 0));
            });
        }
        document.getElementById('evoCitations').innerHTML = this._citationsHtml(alt.citations);
        document.getElementById('evoFacts').innerHTML =
            (proposal.source === 'agent' ? fact('Suggested by the assistant', false, true) : '') +
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
        document.getElementById('evoEvaluationCard').classList.toggle('d-none', !val.passed || mustChoose);
    },

    OUTCOMES: {
        fixed: ['bg-success', 'Fixed'],
        unchanged: ['bg-light text-dark border', 'Unchanged'],
        already_correct: ['bg-light text-dark border', 'Already correct'],
        new_answer: ['bg-info text-dark', 'Newly answerable'],
        still_broken: ['bg-danger', 'Still wrong'],
        regressed: ['bg-danger', 'Regressed'],
        inconclusive: ['bg-secondary', 'Inconclusive'],
    },

    DECISIONS: {
        PASS: 'bg-success',
        FAIL: 'bg-danger',
        NO_CHANGE: 'bg-secondary',
        INCONCLUSIVE: 'bg-warning text-dark',
        CANCELLED: 'bg-secondary',
    },

    _evaluationBody() {
        const body = { revision: this._run.revision };
        const expected = document.getElementById('evoExpected').value.trim();
        const table = document.getElementById('evoGoldTable').value.trim();
        const qid = document.getElementById('evoGoldQuestion').value.trim();
        if (expected) body.expected_ids = expected.split(',').map((s) => s.trim()).filter(Boolean);
        if (table) body.gold_table = table;
        if (qid) body.target_question_id = qid;
        const checks = document.getElementById('evoChecksInput').value.trim();
        if (checks) body.checks = JSON.parse(checks);
        return body;
    },

    async evaluate() {
        if (!this._run) return;
        let body;
        try {
            body = this._evaluationBody();
        } catch (_) {
            showNotification('The protected questions are not valid JSON', 'warning');
            return;
        }
        const btn = document.getElementById('evoEvaluateBtn');
        btn.disabled = true;
        document.getElementById('evoAcceptBox').classList.add('d-none');
        try {
            const out = await this._post('/evolution/runs/' + encodeURIComponent(this._run.id) + '/evaluate', body);
            this._run = out.run;
            await this._poll();
        } catch (err) {
            showNotification('Evaluation failed: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },

    async _poll() {
        const status = document.getElementById('evoEvalStatus');
        const started = Date.now();
        while (this._run.status === 'RUNNING') {
            const secs = Math.round((Date.now() - started) / 1000);
            status.textContent = 'Building both graphs and comparing… ' + secs + 's';
            await new Promise((r) => setTimeout(r, 3000));
            const data = await this._get('/evolution/runs/' + encodeURIComponent(this._run.id) +
                '?domain=' + encodeURIComponent(this._folder()));
            if (data && data.run) this._run = data.run;
        }
        status.textContent = '';
        this._renderEvaluation(this._run.evaluation || {});
    },

    _ids(arm) {
        if (!arm) return '';
        if (arm.status === 'unresolved') return '<span class="text-muted">cannot be answered</span>';
        if (arm.status === 'error') return '<span class="text-danger">error</span>';
        return escapeHtml((arm.ids || []).join(', ') || '(none)') + (arm.truncated ? ' …' : '');
    },

    _renderEvaluation(ev) {
        const badge = document.getElementById('evoDecisionBadge');
        badge.className = 'badge ' + (this.DECISIONS[ev.decision] || 'bg-secondary');
        badge.textContent = ev.decision || '';
        const rows = (ev.questions || []).map((q) => {
            const out = this.OUTCOMES[q.outcome] || ['bg-secondary', q.outcome];
            return '<tr><td><code>' + escapeHtml(q.question_id) + '</code>' +
                (q.role === 'target' ? ' <span class="badge bg-light text-dark border">question</span>' : '') +
                '</td><td>' + this._ids(q.parent) + '</td><td>' + this._ids(q.candidate) + '</td><td>' +
                escapeHtml(q.expected_ids ? q.expected_ids.join(', ') : '—') + '</td><td><span class="badge ' +
                out[0] + '">' + escapeHtml(out[1]) + '</span></td></tr>';
        });
        const reasons = (ev.reasons || []).map((r) => r.replace(/_/g, ' ')).join('; ');
        const unpinned = (ev.unpinned_sources || []).map((t) => t.split('.').pop()).join(', ');
        document.getElementById('evoEvaluation').innerHTML =
            (rows.length ? '<table class="table table-sm align-middle evo-answers"><thead><tr><th>Question</th>' +
                '<th>Parent</th><th>Candidate</th><th>Expected</th><th>Result</th></tr></thead><tbody>' +
                rows.join('') + '</tbody></table>' : '') +
            (reasons ? '<p class="small text-muted mb-1">' + escapeHtml(reasons) + '</p>' : '') +
            (unpinned ? '<p class="small text-warning-emphasis mb-1">Views have no Delta version, so a change during the run cannot be ruled out: ' +
                escapeHtml(unpinned) + '</p>' : '') +
            (ev.error ? '<p class="small text-danger mb-1">' + escapeHtml(ev.error) + '</p>' : '');
        document.getElementById('evoAcceptBox').classList.toggle('d-none', ev.decision !== 'PASS');
    },

    async accept() {
        if (!this._run) return;
        const ok = await showConfirmDialog({
            title: 'Accept as a new draft version?',
            message: 'This creates the next DRAFT version of the context from the candidate. The parent version is not changed.',
            confirmText: 'Accept',
            confirmClass: 'btn-success',
        });
        if (!ok) return;
        const btn = document.getElementById('evoAcceptBtn');
        btn.disabled = true;
        try {
            const out = await this._post('/evolution/runs/' + encodeURIComponent(this._run.id) + '/accept',
                { revision: this._run.revision });
            this._run = out.run;
            showNotification('Version ' + out.version + ' created as DRAFT. Submit it for review from the registry.', 'success');
            document.getElementById('evoAcceptBox').classList.add('d-none');
        } catch (err) {
            showNotification('Accept failed: ' + err.message, 'danger');
        } finally {
            btn.disabled = false;
        }
    },
};
