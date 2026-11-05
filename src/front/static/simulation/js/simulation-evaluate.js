/**
 * Simulation Compare → Evaluate tab. Sends one prompt to the Graph Chat agent
 * twice, against the live graph and against the scenario sandbox, and shows
 * both answers side by side as rendered Markdown, with the agent's steps
 * (tool calls and results) streamed live above each answer. Each turn is
 * stateless (`persist: false`), so neither answer lands in the Graph Chat
 * history.
 *
 * Scenario and live source come from the Graphs tab (SimulationCompare).
 */
const SimulationEvaluate = {
    _seq: 0,
    _busy: false,
    _hasResult: false,
    ARGS_PREVIEW: 160,

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
        const cap = side === 'live' ? 'Live' : 'Sandbox';
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
    async _ask(body, onStep) {
        const resp = await fetch('/dtwin/assistant/chat/stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin',
            body: JSON.stringify({ ...body, history: [], persist: false }),
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

    async run() {
        const prompt = this._prompt();
        const scenario = this._selectedScenario();
        if (!prompt || !scenario || this._busy) return;

        const seq = ++this._seq;
        const includeInferred = SimulationCompare._includeInferred();
        this._hasResult = true;
        this._setTitles(scenario);
        this._setBusy(true);

        const ask = (side, body) => {
            const current = () => seq === this._seq;
            this._start(side);
            return this._ask(body, (event) => { if (current()) this._step(side, event); }).then(
                (text) => { if (current()) this._finish(side, { text }); },
                (err) => { if (current()) this._finish(side, { error: err.message }); },
            );
        };

        try {
            await Promise.all([
                ask('live', { message: prompt, include_inferred: includeInferred, simulation_id: '' }),
                ask('sandbox', { message: prompt, include_inferred: true, simulation_id: scenario.id }),
            ]);
        } finally {
            if (seq === this._seq) this._setBusy(false);
        }
    },
};

window.SimulationEvaluate = SimulationEvaluate;
document.addEventListener('DOMContentLoaded', () => SimulationEvaluate.init());
