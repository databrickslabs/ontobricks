/**
 * Simulation Compare → Evaluate tab. Sends one prompt to the Graph Chat agent
 * twice, against the live graph and against the scenario sandbox, and shows
 * both answers side by side as rendered Markdown, with the agent's steps
 * (tool calls and results) streamed live above each answer. Each turn is
 * stateless (`persist: false`), so neither answer lands in the Graph Chat
 * history.
 *
 * Once both answers succeed, the business-insights agent compares them against
 * the scenario and streams its analysis into a third pane.
 *
 * Scenario and live source come from the Graphs tab (SimulationCompare).
 */
const SimulationEvaluate = {
    _seq: 0,
    _busy: false,
    _hasResult: false,
    _report: null,
    ARGS_PREVIEW: 160,
    KPI_LABELS: [
        ['entity_count', 'Entities'],
        ['live_count', 'Live'],
        ['sandbox_count', 'Sandbox'],
        ['added_count', 'Added'],
        ['removed_count', 'Removed'],
        ['unchanged_count', 'Unchanged'],
    ],

    init() {
        const run = document.getElementById('simEvalRunBtn');
        if (!run) return;
        run.addEventListener('click', () => this.run());

        const prompt = document.getElementById('simEvalPrompt');
        if (prompt) {
            prompt.addEventListener('input', () => this.refresh());
            prompt.addEventListener('keydown', (e) => {
                // Enter submits; Shift+Enter inserts a newline.
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.run();
                }
            });
        }

        const source = document.getElementById('simCompareSource');
        if (source) source.addEventListener('change', () => this.refresh());

        const tab = document.getElementById('simCompare-tab-evaluate');
        if (tab) tab.addEventListener('shown.bs.tab', () => this.refresh());

        const open = document.getElementById('simEvalReportBtn');
        if (open) open.addEventListener('click', () => this.openReport());
        const printBtn = document.getElementById('simEvalReportPrintBtn');
        if (printBtn) printBtn.addEventListener('click', () => this.printReport());

        this.refresh();
    },

    _prompt() {
        const el = document.getElementById('simEvalPrompt');
        return el ? el.value.trim() : '';
    },

    _selectedScenario() {
        const select = document.getElementById('simCompareScenario');
        if (!select || select.disabled || !select.value) return null;
        const known = (SimulationCompare.scenarios || []).find((s) => s.id === select.value);
        const option = select.options[select.selectedIndex];
        const label = (known && known.label) || (option ? option.textContent.trim() : select.value);
        return { id: select.value, label };
    },

    _liveLabel() {
        const source = document.getElementById('simCompareSource');
        const option = source && source.options[source.selectedIndex];
        return option ? option.textContent.trim() : 'Live graph';
    },

    /** Enables Ask both only with a prompt and a ready scenario. */
    refresh() {
        const run = document.getElementById('simEvalRunBtn');
        const hint = document.getElementById('simEvalHint');
        const scenario = this._selectedScenario();
        if (run) run.disabled = this._busy || !scenario || !this._prompt();
        if (hint) {
            hint.textContent = scenario
                ? ''
                : 'Pick a ready scenario above the tabs to ask both graphs.';
        }
        if (!this._busy && !this._hasResult) this._setTitles(scenario);
        const open = document.getElementById('simEvalReportBtn');
        if (open) open.disabled = this._busy || !this._report;
    },

    _setBusy(busy) {
        this._busy = busy;
        this.refresh();
    },

    _setTitles(scenario) {
        const set = (side, text) => {
            const el = document.querySelector(`#simEval${side}Title [data-role="title"]`);
            if (el) el.textContent = text;
        };
        set('Live', this._liveLabel());
        set('Sandbox', scenario ? scenario.label : 'Scenario');
    },

    _els(side) {
        const cap = { live: 'Live', sandbox: 'Sandbox', insights: 'Insights' }[side];
        const steps = document.getElementById(`simEval${cap}Steps`);
        return {
            status: document.getElementById(`simEval${cap}Status`),
            statusText: document.querySelector(`#simEval${cap}Status [data-role="status-text"]`),
            steps,
            list: steps && steps.querySelector('[data-role="list"]'),
            count: steps && steps.querySelector('[data-role="count"]'),
            error: document.getElementById(`simEval${cap}Error`),
            output: document.getElementById(`simEval${cap}Output`),
        };
    },

    _start(side) {
        const el = this._els(side);
        if (el.status) el.status.classList.remove('d-none');
        if (el.statusText) el.statusText.textContent = 'Thinking…';
        if (el.list) el.list.innerHTML = '';
        if (el.count) el.count.textContent = '';
        if (el.steps) {
            el.steps.open = true;
            el.steps.classList.add('d-none');
        }
        if (el.error) el.error.classList.add('d-none');
        if (el.output) el.output.innerHTML = '';
    },

    _step(side, event) {
        const el = this._els(side);
        const tool = event.tool_name || 'tool';
        if (event.step_type === 'tool_call') {
            if (el.statusText) el.statusText.textContent = `Calling ${tool}…`;
            if (!el.list) return;
            const li = document.createElement('li');
            li.dataset.tool = tool;
            const name = document.createElement('code');
            name.textContent = tool;
            li.append('Calling ', name);
            const args = String(event.content || '').trim();
            if (args && args !== '{}') {
                const preview = document.createElement('span');
                preview.className = 'sim-eval-step-args';
                preview.textContent = args.length > this.ARGS_PREVIEW
                    ? `${args.slice(0, this.ARGS_PREVIEW)}…`
                    : args;
                li.appendChild(preview);
            }
            el.list.appendChild(li);
            el.steps.classList.remove('d-none');
            if (el.count) el.count.textContent = `(${el.list.children.length})`;
        } else if (event.step_type === 'tool_result') {
            if (el.statusText) el.statusText.textContent = `${tool} done, thinking…`;
            const pending = el.list && Array.from(el.list.children)
                .reverse()
                .find((li) => li.dataset.tool === tool && !li.dataset.done);
            if (pending) {
                pending.dataset.done = '1';
                const done = document.createElement('span');
                done.className = 'text-success ms-1';
                done.textContent = `✓ ${event.duration_ms || 0} ms`;
                pending.insertBefore(done, pending.querySelector('.sim-eval-step-args'));
            }
        } else if (event.step_type === 'output') {
            if (el.statusText) el.statusText.textContent = 'Writing the answer…';
        }
    },

    _finish(side, { text = '', error = '' } = {}) {
        const el = this._els(side);
        if (el.status) el.status.classList.add('d-none');
        if (el.steps) el.steps.open = false;
        if (el.error) {
            el.error.textContent = error;
            el.error.classList.toggle('d-none', !error);
        }
        if (el.output) {
            el.output.innerHTML = error
                ? ''
                : (text ? renderMarkdown(text) : '<p class="text-muted small mb-0">Empty answer.</p>');
        }
    },

    /** Streams one stateless Graph Chat turn; resolves with the reply text. */
    _ask(body, onStep) {
        return this._sse(
            '/dtwin/assistant/chat/stream',
            { ...body, history: [], persist: false },
            onStep,
        );
    },

    /** POSTs a body to a Server-Sent Events endpoint; resolves with the final reply. */
    async _sse(url, body, onStep) {
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify(body),
        });
        if (!resp.ok || !resp.body) {
            const data = await resp.json().catch(() => ({}));
            throw new Error(data.message || data.detail || `Request failed (${resp.status})`);
        }
        const reader = resp.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        try {
            for (;;) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const parts = buffer.split('\n\n');
                buffer = parts.pop();
                for (const part of parts) {
                    const line = part.trim();
                    if (!line.startsWith('data:')) continue;
                    let event;
                    try {
                        event = JSON.parse(line.slice(5).trim());
                    } catch (_) {
                        continue;
                    }
                    if (event.type === 'step') onStep(event);
                    else if (event.type === 'done') return event.reply || '';
                    else if (event.type === 'error') throw new Error(event.message || 'Agent error');
                }
            }
        } finally {
            reader.releaseLock();
        }
        throw new Error('Stream ended without a final answer.');
    },

    /** Streams the business-insights agent over the two finished answers. */
    async _insights({ seq, prompt, live, sandbox, includeInferred, simulationId }) {
        const current = () => seq === this._seq;
        this._start('insights');
        try {
            const text = await this._sse('/dtwin/assistant/insights/stream', {
                message: prompt,
                live_answer: live,
                sandbox_answer: sandbox,
                include_inferred: includeInferred,
                simulation_id: simulationId,
            }, (event) => { if (current()) this._step('insights', event); });
            if (current()) {
                this._report = {
                    prompt,
                    live,
                    sandbox,
                    narrative: text,
                    scenarioId: simulationId,
                    liveLabel: this._liveLabel(),
                    sandboxLabel: (this._selectedScenario() || {}).label || 'Scenario',
                };
                this._finish('insights', { text });
            }
        } catch (err) {
            if (current()) {
                this._report = null;
                this._finish('insights', { error: err.message });
            }
        }
    },

    /** Shows a muted note in the insights pane when no insights run is active. */
    _insightsIdle(note) {
        const el = this._els('insights');
        if (el.status) el.status.classList.add('d-none');
        if (el.steps) {
            el.steps.open = false;
            el.steps.classList.add('d-none');
        }
        if (el.error) el.error.classList.add('d-none');
        if (el.output) {
            const p = document.createElement('p');
            p.className = 'text-muted small mb-0';
            p.textContent = note;
            el.output.replaceChildren(p);
        }
        this._report = null;
        const open = document.getElementById('simEvalReportBtn');
        if (open) open.disabled = true;
    },

    async run() {
        const prompt = this._prompt();
        const scenario = this._selectedScenario();
        if (!prompt || !scenario || this._busy) return;

        const seq = ++this._seq;
        const includeInferred = SimulationCompare._includeInferred();
        this._hasResult = true;
        this._setTitles(scenario);
        this._insightsIdle('Waiting for both answers…');
        this._setBusy(true);

        const ask = (side, body) => {
            const current = () => seq === this._seq;
            this._start(side);
            return this._ask(body, (event) => { if (current()) this._step(side, event); }).then(
                (text) => {
                    if (current()) this._finish(side, { text });
                    return { ok: true, text };
                },
                (err) => {
                    if (current()) this._finish(side, { error: err.message });
                    return { ok: false, text: '' };
                },
            );
        };

        try {
            const [live, sandbox] = await Promise.all([
                ask('live', { message: prompt, include_inferred: includeInferred, simulation_id: '' }),
                ask('sandbox', { message: prompt, include_inferred: true, simulation_id: scenario.id }),
            ]);
            if (seq !== this._seq) return;
            if (live.ok && sandbox.ok) {
                await this._insights({
                    seq,
                    prompt,
                    live: live.text,
                    sandbox: sandbox.text,
                    includeInferred,
                    simulationId: scenario.id,
                });
            } else {
                this._insightsIdle('Insights need both answers.');
            }
        } finally {
            if (seq === this._seq) this._setBusy(false);
        }
    },

    async openReport() {
        if (!this._report) return;
        const modalEl = document.getElementById('simEvalReportModal');
        const body = document.getElementById('simEvalReportBody');
        if (!modalEl || !body || typeof bootstrap === 'undefined') return;
        body.replaceChildren(this._note('Building the report…'));
        bootstrap.Modal.getOrCreateInstance(modalEl).show();
        try {
            body.replaceChildren(await this._buildReport(this._report));
        } catch (err) {
            body.replaceChildren(this._note(`Could not build the report: ${err.message}`));
        }
    },

    printReport() {
        const html = document.documentElement;
        const previous = document.title;
        const label = (this._selectedScenario() || {}).label || 'scenario';
        document.title = `OntoBricks — ${label} — business insights`;
        html.classList.add('sim-eval-report-printing');
        const restore = () => {
            html.classList.remove('sim-eval-report-printing');
            document.title = previous;
            window.removeEventListener('afterprint', restore);
        };
        window.addEventListener('afterprint', restore);
        window.print();
        setTimeout(restore, 1000);
    },

    async _buildReport(report) {
        const root = document.createElement('article');
        root.className = 'sim-eval-report';
        const scenario = await this._loadScenario(report.scenarioId);
        const title = document.createElement('h1');
        title.className = 'h4 mb-2';
        title.textContent = scenario.label || report.sandboxLabel || 'Business insights';
        root.append(title);
        if (scenario.description) {
            const p = document.createElement('p');
            p.className = 'text-muted';
            p.textContent = scenario.description;
            root.append(p);
        }
        root.append(this._heading('Hypothetical facts'));
        root.append(this._factsList(scenario.patches || []));
        root.append(this._heading('Graph comparison'));
        root.append(this._compareBlock());
        root.append(this._heading('Question'));
        const q = document.createElement('p');
        q.textContent = report.prompt;
        root.append(q);
        root.append(this._heading('Answers'));
        const answers = document.createElement('div');
        answers.className = 'sim-eval-report-answers';
        answers.append(
            this._mdColumn(report.liveLabel, report.live),
            this._mdColumn(report.sandboxLabel, report.sandbox),
        );
        root.append(answers);
        root.append(this._heading('Analysis'));
        const analysis = document.createElement('div');
        analysis.className = 'oc-md';
        analysis.innerHTML = report.narrative
            ? renderMarkdown(report.narrative)
            : '<p class="text-muted small mb-0">No analysis.</p>';
        root.append(analysis);
        return root;
    },

    async _loadScenario(id) {
        const empty = { label: '', description: '', patches: [] };
        if (!id || typeof SimulationPage === 'undefined') return empty;
        try {
            const data = await SimulationPage.api(`/simulation/${encodeURIComponent(id)}`);
            const row = data.simulation || {};
            const spec = row.spec || {};
            return {
                label: row.label || spec.label || '',
                description: spec.description || '',
                patches: spec.patches || [],
            };
        } catch (_) {
            return empty;
        }
    },

    _factsList(patches) {
        if (!patches.length) {
            return this._note('This scenario has no hypothetical facts.');
        }
        const ol = document.createElement('ol');
        patches.forEach((patch) => {
            const li = document.createElement('li');
            li.textContent = this._factLine(patch);
            ol.appendChild(li);
        });
        return ol;
    },

    _factLine(patch) {
        const short = (uri) => (
            typeof SimulationPage !== 'undefined' ? SimulationPage.shortUri(uri) : String(uri || '')
        );
        const term = (uri) => (
            typeof SimulationPage !== 'undefined' ? SimulationPage.termLabel(uri) : String(uri || '')
        );
        const subject = patch.subject_label || short(patch.subject);
        const predicate = patch.predicate_label || short(patch.predicate);
        const object = patch.object_label || term(patch.object);
        if (patch.op === 'remove') {
            return `Retract ${predicate} = ${object} from ${subject}.`;
        }
        if (patch.op === 'add') {
            return `Assert ${predicate} = ${object} on ${subject}.`;
        }
        return `Set ${predicate} of ${subject} to ${object}.`;
    },

    _compareBlock() {
        const wrap = document.createElement('div');
        const data = window.SimulationCompare && SimulationCompare.data;
        const summary = data && data.summary;
        if (!summary) {
            wrap.append(this._note(
                'Run Compare on the Graphs tab to include the overlay and its metrics.',
            ));
            return wrap;
        }
        const dl = document.createElement('dl');
        dl.className = 'sim-eval-report-kpis mb-3';
        this.KPI_LABELS.forEach(([key, label]) => {
            const dt = document.createElement('dt');
            dt.textContent = label;
            const dd = document.createElement('dd');
            const value = summary[key];
            dd.textContent = value == null ? '–' : Number(value).toLocaleString();
            dl.append(dt, dd);
        });
        wrap.append(dl);
        const png = this._graphPng();
        if (png) {
            const img = document.createElement('img');
            img.className = 'sim-eval-report-graph';
            img.alt = 'Live versus sandbox comparison graph';
            img.src = png;
            wrap.append(img);
        } else {
            wrap.append(this._note('The comparison graph is not on screen yet.'));
        }
        return wrap;
    },

    _graphPng() {
        const container = document.getElementById('simCompareCanvas');
        if (!container) return '';
        const canvases = Array.from(container.querySelectorAll('canvas'))
            .filter((c) => c.width && c.height);
        if (!canvases.length) return '';
        const width = Math.max(...canvases.map((c) => c.width));
        const height = Math.max(...canvases.map((c) => c.height));
        const out = document.createElement('canvas');
        out.width = width;
        out.height = height;
        const ctx = out.getContext('2d');
        if (!ctx) return '';
        const fill = getComputedStyle(document.documentElement)
            .getPropertyValue('--db-canvas-warm').trim() || '#FAFAF9';
        ctx.fillStyle = fill;
        ctx.fillRect(0, 0, width, height);
        canvases.forEach((c) => ctx.drawImage(c, 0, 0));
        try {
            return out.toDataURL('image/png');
        } catch (_) {
            return '';
        }
    },

    _mdColumn(title, markdown) {
        const col = document.createElement('section');
        const h = document.createElement('h3');
        h.className = 'h6';
        h.textContent = title;
        const body = document.createElement('div');
        body.className = 'oc-md';
        body.innerHTML = markdown
            ? renderMarkdown(markdown)
            : '<p class="text-muted small mb-0">Empty answer.</p>';
        col.append(h, body);
        return col;
    },

    _heading(text) {
        const h = document.createElement('h2');
        h.textContent = text;
        return h;
    },

    _note(text) {
        const p = document.createElement('p');
        p.className = 'text-muted small mb-0';
        p.textContent = text;
        return p;
    },
};

window.SimulationEvaluate = SimulationEvaluate;
document.addEventListener('DOMContentLoaded', () => SimulationEvaluate.init());
