/**
 * Cohort Discovery Module — UX for the deterministic Stage 1 engine.
 *
 * Owns the form state, debounced live counters, dry-run + materialise
 * round-trips, the four constraint primitives and the Materialise-targets
 * modal.  Stage 2 (NL agent) layers on top via the same JSON contract.
 */
const CohortModule = {
    // ---- internal state -------------------------------------------------
    rule: null,                 // current draft (CohortRule JSON)
    rules: [],                  // saved rules list
    activeRuleId: null,         // id of the loaded saved rule, if any
    classes: [],                // ontology classes loaded once
    properties: [],             // ontology properties loaded once
    objectProperties: [],       // ObjectProperty subset
    lastPreview: null,          // last DetectionResult JSON
    dirty: false,
    _counterTimers: {},

    // ---- bootstrap ------------------------------------------------------

    async init() {
        await this._loadOntology();
        await this.loadRules();
        // Honour a deep-link from the run page's "view/edit" button:
        // ``/ontology/?section=cohorts&rule=<id>`` lands on the design
        // surface with that rule preloaded into the form.
        const params = new URLSearchParams(window.location.search || '');
        const ruleParam = params.get('rule');
        if (ruleParam && (this.rules || []).some(r => r.id === ruleParam)) {
            this.loadRule(ruleParam);
        } else {
            this.newRule(false);
        }
    },

    async _loadOntology() {
        try {
            const r = await fetch('/ontology/get-loaded-ontology', { credentials: 'include' });
            if (r.ok) {
                const data = await r.json();
                const ont = data?.ontology || data || {};
                this.classes = ont.classes || [];
                this.properties = ont.properties || [];
            }
        } catch {
            this.classes = [];
            this.properties = [];
        }
        // fall back: read from document if injected
        if (!this.classes.length) {
            try {
                const ont = window.__ontology__ || {};
                this.classes = ont.classes || [];
                this.properties = ont.properties || [];
            } catch { /* noop */ }
        }
        this.objectProperties = (this.properties || []).filter(p =>
            (p.type || p.kind || '').toLowerCase().includes('object')
        );
        this._populateClassSelect();
    },

    _populateClassSelect() {
        const sel = document.getElementById('cohortClassUri');
        if (!sel) return;
        sel.innerHTML = '<option value="">— select an entity —</option>';
        const classes = (this.classes || []).slice().sort((a, b) =>
            (a.label || a.name || a.uri || '').localeCompare(b.label || b.name || b.uri || '')
        );
        for (const c of classes) {
            const uri = c.uri || c.iri || c.id || '';
            const label = c.label || c.name || uri;
            if (!uri) continue;
            const opt = document.createElement('option');
            opt.value = uri;
            opt.textContent = label;
            sel.appendChild(opt);
        }
        sel.onchange = () => {
            this.markDirty();
            this._refreshClassCount();
            this._renderLinks();
        };
    },

    _classByUri(uri) {
        if (!uri) return null;
        return (this.classes || []).find(cl =>
            (cl.uri || cl.iri || cl.id || '') === uri
        ) || null;
    },

    _classNameByUri(uri) {
        const c = this._classByUri(uri);
        return c ? (c.name || c.label || '') : '';
    },

    _classLabelByUri(uri) {
        const c = this._classByUri(uri);
        return c ? (c.label || c.name || uri) : uri;
    },

    _objectPropsForSource(sourceUri) {
        const srcName = this._classNameByUri(sourceUri || '');
        if (!srcName) return this.objectProperties || [];
        const matched = (this.objectProperties || []).filter(p =>
            (p.domain || '') === srcName
        );
        return matched.length ? matched : (this.objectProperties || []);
    },

    _compatibleViaProperties(sourceUri, targetUri) {
        const props = this._objectPropsForSource(sourceUri);
        if (!targetUri) return props;
        const targetName = this._classNameByUri(targetUri);
        if (!targetName) return props;
        const matched = props.filter(p => (p.range || '') === targetName);
        return matched.length ? matched : props;
    },

    _compatibleTargetClasses(sourceUri, viaUri) {
        const props = this._objectPropsForSource(sourceUri);
        const filtered = viaUri
            ? props.filter(p => (p.uri || p.iri || p.id || '') === viaUri)
            : props;
        const ranges = new Set();
        for (const p of filtered) {
            const r = (p.range || '').trim();
            if (r) ranges.add(r);
        }
        const all = this.classes || [];
        if (!ranges.size) return all;
        const out = all.filter(c => ranges.has(c.name || c.label || ''));
        return out.length ? out : all;
    },

    _normalizeLink(lk) {
        if (!lk) return;
        if (!Array.isArray(lk.path) || !lk.path.length) {
            const via = lk.via || '';
            const target = lk.shared_class || '';
            if (via || target) {
                lk.path = [{ via, target_class: target, where: [] }];
            } else {
                lk.path = [{ via: '', target_class: '', where: [] }];
            }
        }
        for (const h of lk.path) {
            if (!Array.isArray(h.where)) h.where = [];
        }
        delete lk.shared_class;
        delete lk.via;
    },

    // ---- Saved rules list ----------------------------------------------

    async loadRules() {
        try {
            const r = await fetch('/dtwin/cohorts/rules', { credentials: 'include' });
            if (!r.ok) throw new Error(await r.text());
            const data = await r.json();
            this.rules = data.rules || [];
        } catch (e) {
            this.rules = [];
        }
        this._renderRulesList();
    },

    _renderRulesList() {
        const list = document.getElementById('cohortRulesList');
        const empty = document.getElementById('cohortRulesEmpty');
        const counter = document.getElementById('cohortRulesCount');
        if (counter) counter.textContent = this.rules.length;
        if (!list) return;
        list.innerHTML = '';
        if (!this.rules.length) {
            if (empty) list.appendChild(empty);
            return;
        }
        // Edit link is shown only on the run page (Digital Twin → Cohorts):
        // the design page is itself the editor, so an extra link there would
        // just reload the same view.
        const onRunPage = !document.getElementById('cohortRuleLabel');
        for (const r of this.rules) {
            const row = document.createElement('div');
            row.className = 'cohort-rule-item' + (
                r.id === this.activeRuleId ? ' active' : ''
            );
            const name = r.label || r.id;
            const ucIcon = r.output && r.output.uc_table && r.output.uc_table.table_name
                ? '<i class="bi bi-database-fill ms-1" title="UC table configured"></i>'
                : '';
            row.innerHTML = `
                <button type="button" class="cohort-rule-item-main">
                    <div class="cohort-rule-name">${this._esc(name)}</div>
                    <div class="cohort-rule-meta text-muted small">
                        <span class="badge bg-secondary bg-opacity-25 text-dark">
                            ${this._esc(this._localName(r.class_uri || ''))}
                        </span>
                        ${ucIcon}
                    </div>
                </button>
                ${onRunPage
                    ? `<a class="cohort-rule-edit" title="View / edit rule definition"
                          href="/ontology/?section=cohorts&rule=${encodeURIComponent(r.id)}">
                           <i class="bi bi-pencil-square"></i>
                       </a>`
                    : ''}
            `;
            row.querySelector('.cohort-rule-item-main')
                .addEventListener('click', () => this.loadRule(r.id));
            list.appendChild(row);
        }
    },

    loadRule(rule_id) {
        const r = this.rules.find(x => x.id === rule_id);
        if (!r) return;
        this.activeRuleId = rule_id;
        this.rule = JSON.parse(JSON.stringify(r));
        this.lastPreview = null;
        this._resetPreviewPane();
        this._hydrateForm();
        this._renderRulesList();
        this._showBuildTab();
        this._setStatus(`Loaded rule "${r.label || r.id}"`);
    },

    newRule(reset = true) {
        this.activeRuleId = null;
        this.rule = {
            id: '',
            label: '',
            class_uri: '',
            links: [],
            links_combine: 'any',
            compatibility: [],
            group_type: 'connected',
            min_size: 2,
            max_triples: 500000,
            output: { graph: true },
            description: '',
            enabled: true,
        };
        this.lastPreview = null;
        this._resetPreviewPane();
        if (reset) this._hydrateForm();
        this._renderRulesList();
        this._renderRuleSummary();
        this._setStatus('Drafting a new rule.');
    },

    _resetPreviewPane() {
        const body = document.getElementById('cohortPreviewBody');
        if (body) {
            const onRunPage = !document.getElementById('cohortRuleLabel');
            body.innerHTML = onRunPage
                ? `<div class="text-muted small fst-italic py-3">
                       Pick a rule from the right pane and click
                       <em>Preview cohorts</em>.
                   </div>`
                : `<div class="text-muted small fst-italic py-3">
                       No preview yet. Run <em>Preview cohorts</em> from the
                       <strong>Build rule</strong> tab to see results here.
                   </div>`;
        }
        const explainResult = document.getElementById('cohortExplainResult');
        if (explainResult) explainResult.innerHTML = '';
        const traceResult = document.getElementById('cohortTraceResult');
        if (traceResult) traceResult.innerHTML = '';
        const matBtn = document.getElementById('cohortMaterializeBtn');
        if (matBtn) matBtn.disabled = true;
    },

    /**
     * Render a compact read-only summary of the active rule.
     *
     * Only used on the Digital Twin run page (#cohortRuleSummary lives
     * there). On the design page this is a no-op.
     */
    _renderRuleSummary() {
        const el = document.getElementById('cohortRuleSummary');
        if (!el) return;
        if (!this.activeRuleId || !this.rule) {
            el.innerHTML = `
                <div class="alert alert-light border small mb-0">
                    <i class="bi bi-info-circle me-1"></i>
                    No rule selected. Pick a saved rule from the
                    <em>Saved rules</em> list on the right.
                </div>`;
            return;
        }
        const r = this.rule;
        const className = this._classLabelByUri(r.class_uri || '') || '—';
        const linkCount = (r.links || []).length;
        const compatCount = (r.compatibility || []).length;
        const ucCfg = r.output?.uc_table;
        const ucCell = ucCfg && ucCfg.table_name
            ? `<code>${this._esc(ucCfg.catalog || '')}.${this._esc(ucCfg.schema || '')}.${this._esc(ucCfg.table_name)}</code>`
            : '<span class="text-muted">graph triples only</span>';
        el.innerHTML = `
            <div class="card border-primary border-opacity-25 bg-light bg-opacity-50">
                <div class="card-body py-2 px-3">
                    <div class="d-flex justify-content-between align-items-start">
                        <div>
                            <strong>${this._esc(r.label || r.id)}</strong>
                            ${r.description ? `<span class="text-muted small ms-2">— ${this._esc(r.description)}</span>` : ''}
                        </div>
                        <code class="small text-muted">${this._esc(r.id)}</code>
                    </div>
                    <div class="small text-muted mt-1">
                        <span class="me-3"><i class="bi bi-people me-1"></i>${this._esc(className)}</span>
                        <span class="me-3"><i class="bi bi-link-45deg me-1"></i>${linkCount} link${linkCount === 1 ? '' : 's'}</span>
                        <span class="me-3"><i class="bi bi-funnel me-1"></i>${compatCount} compat</span>
                        <span class="me-3"><i class="bi bi-diagram-3 me-1"></i>${this._esc(r.group_type || 'connected')} ≥${r.min_size || 2}</span>
                        <span><i class="bi bi-cloud-upload me-1"></i>${ucCell}</span>
                    </div>
                </div>
            </div>`;
    },

    // ---- Form <-> rule synchronisation ---------------------------------

    _hydrateForm() {
        // Build-form fields only exist on the Ontology design page.
        // The Digital Twin run page has no form — skip per-field updates
        // there but still render the rule summary + saved rules list.
        const setVal = (id, v) => { const el = document.getElementById(id); if (el) el.value = v; };
        const setText = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
        const setChecked = (id, b) => { const el = document.getElementById(id); if (el) el.checked = b; };

        setVal('cohortRuleLabel', this.rule.label || '');
        setVal('cohortRuleDescription', this.rule.description || '');
        setText('cohortRuleId', this.rule.id || '(auto)');
        setVal('cohortClassUri', this.rule.class_uri || '');
        setVal('cohortMinSize', this.rule.min_size || 2);
        setChecked('cohortCombineAny', (this.rule.links_combine || 'any') !== 'all');
        setChecked('cohortCombineAll', (this.rule.links_combine || 'any') === 'all');
        setChecked('cohortGroupConnected', (this.rule.group_type || 'connected') !== 'strict');
        setChecked('cohortGroupStrict', (this.rule.group_type || 'connected') === 'strict');

        this._renderLinks();
        this._renderCompat();
        this._refreshClassCount();
        this._refreshEdgeCount();
        this._refreshNodeCount();
        this._renderRuleSummary();

        const btnMat = document.getElementById('cohortMaterializeBtn');
        if (btnMat) btnMat.disabled = !this.lastPreview;
        const btnPrev = document.getElementById('cohortPreviewBtn');
        if (btnPrev) btnPrev.disabled = !this.rule?.class_uri;
    },

    _hookLabelInput() {
        const el = document.getElementById('cohortRuleLabel');
        if (!el || el._cohortHooked) return;
        el._cohortHooked = true;
        // Rule names are constrained to camelCase (alphanumeric only) so
        // they map cleanly onto the rule id and onto downstream identifiers
        // (UC table suffixes, registry keys, URL params). Strip invalid
        // chars on input, camelize whole-text paste, and validate on save.
        el.addEventListener('input', () => {
            const cursor = el.selectionStart || 0;
            const before = el.value.slice(0, cursor);
            const cleaned = el.value.replace(/[^A-Za-z0-9]/g, '');
            if (cleaned !== el.value) {
                const cleanedBefore = before.replace(/[^A-Za-z0-9]/g, '');
                el.value = cleaned;
                try { el.setSelectionRange(cleanedBefore.length, cleanedBefore.length); }
                catch { /* setSelectionRange not supported on type=number, etc. */ }
            }
            this.rule.label = el.value;
            if (!this.activeRuleId) {
                this.rule.id = this.rule.label;
                document.getElementById('cohortRuleId').textContent = this.rule.id || '(auto)';
            }
            this.markDirty();
        });
        el.addEventListener('paste', (ev) => {
            const cd = ev.clipboardData || window.clipboardData;
            const pasted = cd ? cd.getData('text') : '';
            if (!pasted) return;
            ev.preventDefault();
            const camelized = this._toCamelCase(pasted);
            if (!camelized) return;
            const start = el.selectionStart || 0;
            const end = el.selectionEnd || 0;
            const next = el.value.slice(0, start) + camelized + el.value.slice(end);
            el.value = next;
            try { el.setSelectionRange(start + camelized.length, start + camelized.length); }
            catch { /* noop */ }
            el.dispatchEvent(new Event('input', { bubbles: true }));
        });
        const desc = document.getElementById('cohortRuleDescription');
        if (desc && !desc._cohortHooked) {
            desc._cohortHooked = true;
            desc.addEventListener('input', () => {
                this.rule.description = desc.value;
                this.markDirty();
            });
        }
        const minSize = document.getElementById('cohortMinSize');
        if (minSize && !minSize._cohortHooked) {
            minSize._cohortHooked = true;
            minSize.addEventListener('input', () => {
                this.rule.min_size = parseInt(minSize.value || '2', 10);
                this.markDirty();
            });
        }
    },

    markDirty() {
        this.dirty = true;
        this._collectFromForm();
        this._setStatus('Unsaved changes.', 'warn');
        this._scheduleCounters();
    },

    _collectFromForm() {
        // Only the design page (Ontology > Cohorts) has the build form.
        // Run page (Digital Twin > Cohorts) is read-only — bail without
        // touching ``this.rule`` so the loaded saved rule isn't corrupted.
        if (!document.getElementById('cohortRuleLabel')) return;
        if (!this.rule) this.rule = {};
        const r = this.rule;
        r.label = document.getElementById('cohortRuleLabel').value || '';
        r.description = document.getElementById('cohortRuleDescription').value || '';
        if (!this.activeRuleId) r.id = r.label;
        r.class_uri = document.getElementById('cohortClassUri').value || '';
        r.min_size = parseInt(document.getElementById('cohortMinSize').value || '2', 10) || 2;
        r.links_combine = document.querySelector('input[name="cohortLinksCombine"]:checked')?.value || 'any';
        r.group_type = document.querySelector('input[name="cohortGroupType"]:checked')?.value || 'connected';
    },

    _sanitizedRulePayload() {
        const rule = JSON.parse(JSON.stringify(this.rule || {}));
        const links = Array.isArray(rule.links) ? rule.links : [];
        for (const lk of links) {
            const path = Array.isArray(lk.path) ? lk.path : [];
            for (const h of path) {
                if (Array.isArray(h.where)) {
                    h.where = h.where.filter(w => w && w.property && String(w.property).trim());
                }
            }
        }
        if (Array.isArray(rule.compatibility)) {
            rule.compatibility = rule.compatibility.filter(
                c => c && c.property && String(c.property).trim()
            );
        }
        return rule;
    },

    async _readErrorMessage(response) {
        try {
            const txt = await response.text();
            try {
                const obj = JSON.parse(txt);
                return obj.detail || obj.message || txt;
            } catch {
                return txt;
            }
        } catch {
            return `HTTP ${response.status}`;
        }
    },

    _scheduleCounters() {
        clearTimeout(this._counterTimers.edge);
        clearTimeout(this._counterTimers.node);
        clearTimeout(this._counterTimers.cls);
        this._counterTimers.cls = setTimeout(() => this._refreshClassCount(), 250);
        this._counterTimers.edge = setTimeout(() => this._refreshEdgeCount(), 350);
        this._counterTimers.node = setTimeout(() => this._refreshNodeCount(), 350);
    },

    // ---- Live counters --------------------------------------------------

    async _refreshClassCount() {
        const el = document.getElementById('cohortClassCount');
        if (!el) return;
        const class_uri = this.rule?.class_uri;
        if (!class_uri) { el.textContent = '—'; return; }
        el.textContent = '…';
        try {
            const url = `/dtwin/cohorts/preview/class-stats?class_uri=${encodeURIComponent(class_uri)}`;
            const r = await fetch(url, { credentials: 'include' });
            const data = await r.json();
            el.textContent = (data.instance_count ?? '—').toLocaleString();
        } catch {
            el.textContent = '—';
        }
    },

    async _refreshEdgeCount() {
        const el = document.getElementById('cohortEdgeCount');
        if (!el) return;
        if (!this.rule?.class_uri) { el.textContent = '—'; return; }
        el.textContent = '…';
        try {
            const r = await fetch('/dtwin/cohorts/preview/edge-count', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.rule),
            });
            const data = await r.json();
            el.textContent = (data.edge_count ?? '—').toLocaleString();
        } catch { el.textContent = '—'; }
    },

    async _refreshNodeCount() {
        const el = document.getElementById('cohortMatchCount');
        if (!el) return;
        if (!this.rule?.class_uri) { el.textContent = '—'; return; }
        el.textContent = '…';
        try {
            const r = await fetch('/dtwin/cohorts/preview/node-count', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.rule),
            });
            const data = await r.json();
            const m = data.matching_count ?? 0;
            const t = data.total_count ?? 0;
            el.textContent = `${m.toLocaleString()} / ${t.toLocaleString()}`;
        } catch { el.textContent = '—'; }
    },

    // ---- Section 3 (Linked when) ---------------------------------------

    addLink() {
        if (!this.rule.links) this.rule.links = [];
        this.rule.links.push({ path: [{ via: '', target_class: '' }] });
        this._renderLinks();
        this.markDirty();
    },

    addHop(linkIdx) {
        const lk = (this.rule.links || [])[linkIdx];
        if (!lk) return;
        this._normalizeLink(lk);
        lk.path.push({ via: '', target_class: '', where: [] });
        this._renderLinks();
        this.markDirty();
    },

    addHopWhere(linkIdx, hopIdx, type = 'value_equals') {
        const lk = (this.rule.links || [])[linkIdx];
        if (!lk) return;
        this._normalizeLink(lk);
        const hop = lk.path[hopIdx];
        if (!hop) return;
        const tpl = { type, property: '' };
        if (type === 'value_in') tpl.values = [];
        if (type === 'value_range') { tpl.min = null; tpl.max = null; }
        if (type === 'value_equals') tpl.value = '';
        hop.where.push(tpl);
        this._renderLinks();
        this.markDirty();
    },

    removeHopWhere(linkIdx, hopIdx, whereIdx) {
        const lk = (this.rule.links || [])[linkIdx];
        if (!lk || !Array.isArray(lk.path)) return;
        const hop = lk.path[hopIdx];
        if (!hop || !Array.isArray(hop.where)) return;
        hop.where.splice(whereIdx, 1);
        this._renderLinks();
        this.markDirty();
    },

    removeHop(linkIdx, hopIdx) {
        const lk = (this.rule.links || [])[linkIdx];
        if (!lk || !Array.isArray(lk.path)) return;
        if (lk.path.length <= 1) return;
        lk.path.splice(hopIdx, 1);
        this._renderLinks();
        this.markDirty();
    },

    removeLink(linkIdx) {
        if (!this.rule.links) return;
        this.rule.links.splice(linkIdx, 1);
        this._renderLinks();
        this.markDirty();
    },

    _renderLinks() {
        const list = document.getElementById('cohortLinksList');
        if (!list) return;
        list.innerHTML = '';
        const links = this.rule.links || [];
        if (!links.length) {
            list.innerHTML = '<div class="text-muted small fst-italic">No linkage rules — every compatible pair will be linked directly.</div>';
            return;
        }
        for (let i = 0; i < links.length; i++) {
            this._normalizeLink(links[i]);
            list.appendChild(this._renderLinkCard(i, links[i]));
        }
        // Trunk geometry depends on actual rendered heights of the
        // bullet row and where pane — measure them once after layout
        // and expose via CSS variables so the dashed line connects
        // cleanly to each parent bullet.
        requestAnimationFrame(() => this._relayoutCohortTrunks(list));
    },

    _relayoutCohortTrunks(root) {
        if (!root) return;
        // Each .cohort-tree-hop bullet drives the horizontal connector
        // (::before of the hop) — capture its y from the hop's top.
        root.querySelectorAll('.cohort-tree-hop').forEach(hop => {
            const node = hop.querySelector(':scope > .cohort-tree-node');
            if (!node) return;
            const hopRect = hop.getBoundingClientRect();
            const bullet = node.querySelector(':scope > .cohort-tree-bullet');
            if (!bullet || hopRect.height === 0) return;
            const bRect = bullet.getBoundingClientRect();
            const y = (bRect.top + bRect.height / 2) - hopRect.top;
            hop.style.setProperty('--cohort-bullet-y', `${y}px`);
        });

        const setTrunkVars = (hops, parentBullet) => {
            if (!hops || !hops.firstElementChild || !parentBullet) return;
            const firstHop = hops.querySelector(':scope > .cohort-tree-hop');
            if (!firstHop) return;
            const firstBullet = firstHop.querySelector(
                ':scope > .cohort-tree-node > .cohort-tree-bullet');
            if (!firstBullet) return;
            const hRect = hops.getBoundingClientRect();
            const pRect = parentBullet.getBoundingClientRect();
            const fRect = firstBullet.getBoundingClientRect();
            const parentCenter = pRect.top + pRect.height / 2;
            const firstCenter = fRect.top + fRect.height / 2;
            // up = vertical gap between the parent bullet and the
            //      hops container top (the trunk extension above the
            //      container); 0 if the parent bullet is below the
            //      container's top.
            const up = Math.max(0, hRect.top - parentCenter);
            // height = total trunk length from the parent-bullet
            //          extension down to the first child's bullet
            //          (clamped to the container's actual top when
            //          the parent already sits below — eg. legacy
            //          edge cases). Stops the line from running past
            //          the bullet so we don't need a paint-over mask.
            const height = Math.max(0, firstCenter - parentCenter);
            hops.style.setProperty('--cohort-trunk-up', `${up}px`);
            hops.style.setProperty('--cohort-trunk-height', `${height}px`);
        };

        root.querySelectorAll('.cohort-tree').forEach(tree => {
            const rootEl = tree.querySelector(':scope > .cohort-tree-root');
            const hopsRoot = tree.querySelector(':scope > .cohort-tree-hops-root');
            const rootBullet = rootEl
                ? rootEl.querySelector(':scope > .cohort-tree-bullet')
                : null;
            setTrunkVars(hopsRoot, rootBullet);
        });
        root.querySelectorAll('.cohort-tree-hops-children').forEach(children => {
            const parentHop = children.parentElement;
            if (!parentHop || !parentHop.classList.contains('cohort-tree-hop')) return;
            const parentBullet = parentHop.querySelector(
                ':scope > .cohort-tree-node > .cohort-tree-bullet');
            setTrunkVars(children, parentBullet);
        });
    },

    _renderLinkCard(linkIdx, lk) {
        const card = document.createElement('div');
        card.className = 'cohort-link-card';
        const rootUri = this.rule.class_uri || '';
        const rootLabel = this._classLabelByUri(rootUri) || 'entity?';
        card.innerHTML = `
            <div class="cohort-link-card-header">
                <span class="cohort-link-card-title">Path ${linkIdx + 1}</span>
                <button class="btn btn-sm btn-link text-danger p-0 cohort-link-del"
                        title="Remove path">
                    <i class="bi bi-x-lg"></i>
                </button>
            </div>
            <div class="cohort-tree">
                <div class="cohort-tree-root" title="${this._esc(rootUri)}">
                    <span class="cohort-tree-bullet"><i class="bi bi-circle-fill"></i></span>
                    <span class="cohort-class-chip cohort-class-chip-root">
                        ${this._esc(rootLabel)}
                    </span>
                    <span class="cohort-tree-root-hint">source</span>
                </div>
                <div class="cohort-tree-hops cohort-tree-hops-root"></div>
            </div>
            <button class="btn btn-sm btn-link cohort-hop-add ps-0 mt-1">
                <i class="bi bi-plus-lg me-1"></i>Add hop
            </button>
        `;
        const rootHopList = card.querySelector('.cohort-tree-hops-root');
        // Nest each hop inside the previous one so the connector trunk
        // moves rightward — hop N's vertical line starts at hop N-1's
        // target bullet, not at the rule's source trunk.
        let parent = rootHopList;
        let prevSourceUri = rootUri;
        for (let h = 0; h < lk.path.length; h++) {
            const hop = lk.path[h];
            const isTerminal = h === lk.path.length - 1;
            const hopWrap = this._renderHopRow(linkIdx, h, hop, prevSourceUri, isTerminal);
            parent.appendChild(hopWrap);
            parent = hopWrap.querySelector('.cohort-tree-hops-children');
            prevSourceUri = hop.target_class || '';
        }
        card.querySelector('.cohort-link-del').onclick = () =>
            this.removeLink(linkIdx);
        card.querySelector('.cohort-hop-add').onclick = () =>
            this.addHop(linkIdx);
        return card;
    },

    _renderHopRow(linkIdx, hopIdx, hop, sourceUri, isTerminal) {
        const viaProps = this._compatibleViaProperties(sourceUri, hop.target_class);
        if (hop.via && !viaProps.some(p =>
            (p.uri || p.iri || p.id || '') === hop.via)) {
            hop.via = '';
        }
        const targetClasses = this._compatibleTargetClasses(sourceUri, hop.via);
        if (hop.target_class && !targetClasses.some(c =>
            (c.uri || c.iri || c.id || '') === hop.target_class)) {
            hop.target_class = '';
        }
        const viaDisabled = !sourceUri;
        const targetDisabled = !sourceUri || !hop.via;
        const wrap = document.createElement('div');
        wrap.className = 'cohort-tree-hop';
        if (isTerminal) wrap.classList.add('cohort-tree-hop-terminal');
        const whereCount = (hop.where || []).length;
        const targetChipClass = isTerminal
            ? 'cohort-class-chip cohort-class-chip-terminal'
            : 'cohort-class-chip';
        wrap.innerHTML = `
            <div class="cohort-tree-edge">
                <span class="cohort-tree-edge-prefix">via</span>
                <select class="form-select form-select-sm cohort-input cohort-hop-via"
                        ${viaDisabled ? 'disabled' : ''}>
                    <option value="">${viaDisabled
                        ? 'pick previous entity first'
                        : (viaProps.length ? '— predicate —' : 'no compatible relationship')}</option>
                    ${viaProps.map(p => {
                        const uri = p.uri || p.iri || p.id || '';
                        const lbl = p.label || p.name || uri;
                        return `<option value="${this._esc(uri)}" ${uri === hop.via ? 'selected' : ''}>${this._esc(lbl)}</option>`;
                    }).join('')}
                </select>
                <i class="bi bi-arrow-down cohort-tree-edge-arrow"></i>
            </div>
            <div class="cohort-tree-node">
                <span class="cohort-tree-bullet">
                    <i class="bi ${isTerminal ? 'bi-flag-fill' : 'bi-circle'}"></i>
                </span>
                <span class="${targetChipClass}">
                    <select class="cohort-input cohort-hop-target cohort-class-chip-select"
                            ${targetDisabled ? 'disabled' : ''}>
                        <option value="">${targetDisabled
                            ? 'pick predicate first'
                            : (targetClasses.length ? '— entity —' : 'no compatible target')}</option>
                        ${targetClasses.map(c => {
                            const uri = c.uri || c.iri || c.id || '';
                            const lbl = c.label || c.name || uri;
                            return `<option value="${this._esc(uri)}" ${uri === hop.target_class ? 'selected' : ''}>${this._esc(lbl)}</option>`;
                        }).join('')}
                    </select>
                </span>
                ${isTerminal ? '<span class="cohort-hop-terminal-badge" title="Two members are linked when they reach the same instance of this entity">shared</span>' : ''}
                ${whereCount ? `<span class="cohort-hop-where-badge" title="${whereCount} where filter${whereCount === 1 ? '' : 's'} on this hop">
                    <i class="bi bi-funnel-fill"></i>${whereCount}
                </span>` : ''}
                ${hopIdx > 0 ? `<button class="btn btn-sm btn-link text-danger cohort-hop-del p-0 ms-auto" title="Remove hop">
                    <i class="bi bi-x-lg"></i>
                </button>` : ''}
            </div>
            <div class="cohort-hop-where"></div>
            <div class="cohort-tree-hops cohort-tree-hops-children"></div>
        `;
        const wherePane = wrap.querySelector('.cohort-hop-where');
        wherePane.appendChild(this._renderHopWherePane(linkIdx, hopIdx, hop));
        wrap.querySelector('.cohort-hop-via').onchange = (e) => {
            hop.via = e.target.value;
            const stillValid = this._compatibleTargetClasses(sourceUri, hop.via)
                .some(c => (c.uri || c.iri || c.id || '') === hop.target_class);
            if (!stillValid) hop.target_class = '';
            this._renderLinks();
            this.markDirty();
        };
        wrap.querySelector('.cohort-hop-target').onchange = (e) => {
            hop.target_class = e.target.value;
            this._renderLinks();
            this.markDirty();
        };
        const del = wrap.querySelector('.cohort-hop-del');
        if (del) del.onclick = () => this.removeHop(linkIdx, hopIdx);
        return wrap;
    },

    _renderHopWherePane(linkIdx, hopIdx, hop) {
        const pane = document.createElement('div');
        pane.className = 'cohort-hop-where-pane';
        const targetLabel = this._classLabelByUri(hop.target_class) || 'target';
        const disabled = !hop.target_class;
        const headerHtml = `
            <div class="cohort-hop-where-header">
                <span class="cohort-hop-where-title">
                    where this <strong>${this._esc(targetLabel)}</strong>…
                </span>
                <div class="cohort-hop-where-actions">
                    <button class="btn btn-sm btn-link p-0 cohort-hop-where-add" data-type="value_equals" ${disabled ? 'disabled' : ''}>
                        <i class="bi bi-plus-lg"></i> equals
                    </button>
                    <button class="btn btn-sm btn-link p-0 cohort-hop-where-add" data-type="value_in" ${disabled ? 'disabled' : ''}>
                        <i class="bi bi-plus-lg"></i> in any
                    </button>
                    <button class="btn btn-sm btn-link p-0 cohort-hop-where-add" data-type="value_range" ${disabled ? 'disabled' : ''}>
                        <i class="bi bi-plus-lg"></i> between
                    </button>
                </div>
            </div>
        `;
        pane.innerHTML = headerHtml + '<div class="cohort-hop-where-list"></div>';
        const list = pane.querySelector('.cohort-hop-where-list');
        const where = hop.where || [];
        if (!where.length) {
            list.innerHTML = `<div class="text-muted small fst-italic">
                No filter — every ${this._esc(targetLabel)} reachable through <code>${this._esc(this._localName(hop.via) || '?')}</code> qualifies.
            </div>`;
        } else {
            for (let w = 0; w < where.length; w++) {
                list.appendChild(this._renderHopWhereRow(linkIdx, hopIdx, w, where[w], hop.target_class));
            }
        }
        pane.querySelectorAll('.cohort-hop-where-add').forEach(btn => {
            btn.onclick = () => this.addHopWhere(linkIdx, hopIdx, btn.dataset.type);
        });
        return pane;
    },

    _renderHopWhereRow(linkIdx, hopIdx, whereIdx, w, targetClassUri) {
        const row = document.createElement('div');
        row.className = 'cohort-row cohort-hop-where-row';
        const propOptions = (this.properties || []).map(p => {
            const uri = p.uri || p.iri || p.id || '';
            const lbl = p.label || p.name || uri;
            return `<option value="${this._esc(uri)}" ${uri === w.property ? 'selected' : ''}>${this._esc(lbl)}</option>`;
        }).join('');
        const propSelect = `
            <select class="form-select form-select-sm cohort-input cohort-hop-where-prop">
                <option value="">— property —</option>
                ${propOptions}
            </select>
        `;
        let inner = '';
        if (w.type === 'value_equals') {
            inner = `${propSelect}<span class="cohort-row-label">equal to</span>
                <input type="text" class="form-control form-control-sm cohort-input cohort-hop-where-value"
                       value="${this._esc(w.value ?? '')}" placeholder="value">
                <button class="btn btn-sm btn-outline-secondary cohort-hop-where-sample" title="Sample values from graph">
                    <i class="bi bi-arrow-down-square"></i>
                </button>`;
        } else if (w.type === 'value_in') {
            inner = `${propSelect}<span class="cohort-row-label">in any of</span>
                <input type="text" class="form-control form-control-sm cohort-input cohort-hop-where-values"
                       value="${this._esc((w.values || []).join(', '))}" placeholder="comma-separated values">`;
        } else if (w.type === 'value_range') {
            inner = `${propSelect}<span class="cohort-row-label">between</span>
                <input type="number" class="form-control form-control-sm cohort-input cohort-hop-where-min"
                       value="${w.min ?? ''}" placeholder="min">
                <span class="cohort-row-label">and</span>
                <input type="number" class="form-control form-control-sm cohort-input cohort-hop-where-max"
                       value="${w.max ?? ''}" placeholder="max">`;
        }
        row.innerHTML = `${inner}
            <button class="btn btn-sm btn-link text-danger cohort-hop-where-del" title="Remove filter">
                <i class="bi bi-x-lg"></i>
            </button>`;
        const propEl = row.querySelector('.cohort-hop-where-prop');
        if (propEl) propEl.addEventListener('change', () => {
            w.property = propEl.value;
            this.markDirty();
        });
        const valEl = row.querySelector('.cohort-hop-where-value');
        if (valEl) valEl.addEventListener('input', () => {
            w.value = valEl.value;
            this.markDirty();
        });
        const valuesEl = row.querySelector('.cohort-hop-where-values');
        if (valuesEl) valuesEl.addEventListener('input', () => {
            w.values = valuesEl.value.split(',').map(s => s.trim()).filter(Boolean);
            this.markDirty();
        });
        const minEl = row.querySelector('.cohort-hop-where-min');
        if (minEl) minEl.addEventListener('input', () => {
            w.min = minEl.value === '' ? null : parseFloat(minEl.value);
            this.markDirty();
        });
        const maxEl = row.querySelector('.cohort-hop-where-max');
        if (maxEl) maxEl.addEventListener('input', () => {
            w.max = maxEl.value === '' ? null : parseFloat(maxEl.value);
            this.markDirty();
        });
        const delEl = row.querySelector('.cohort-hop-where-del');
        if (delEl) delEl.onclick = () =>
            this.removeHopWhere(linkIdx, hopIdx, whereIdx);
        const sampleEl = row.querySelector('.cohort-hop-where-sample');
        if (sampleEl) sampleEl.onclick = async () => {
            const choice = await this._pickSampleValue({
                classUri: targetClassUri,
                property: w.property,
                currentValue: w.value ?? '',
                metaLabel: `${w.property || '(no property)'} on hop target`,
            });
            if (choice == null) return;
            w.value = choice;
            this._renderLinks();
            this.markDirty();
        };
        return row;
    },

    // ---- Section 4 (Compatibility) -------------------------------------

    addCompat(type) {
        if (!this.rule.compatibility) this.rule.compatibility = [];
        const tpl = { type, property: '' };
        if (type === 'value_in') tpl.values = [];
        if (type === 'value_range') { tpl.min = null; tpl.max = null; }
        this.rule.compatibility.push(tpl);
        this._renderCompat();
        this.markDirty();
    },

    _renderCompat() {
        const list = document.getElementById('cohortCompatList');
        if (!list) return;
        list.innerHTML = '';
        const items = this.rule.compatibility || [];
        if (!items.length) {
            list.innerHTML = '<div class="text-muted small fst-italic">No compatibility constraints — every member of the entity is eligible.</div>';
            return;
        }
        for (let i = 0; i < items.length; i++) {
            const cc = items[i];
            const row = document.createElement('div');
            row.className = 'cohort-row';
            const propSelect = `
                <select class="form-select form-select-sm cohort-input cohort-compat-prop" data-idx="${i}">
                    <option value="">— property —</option>
                    ${(this.properties || []).map(p => {
                        const uri = p.uri || p.iri || p.id || '';
                        const lbl = p.label || p.name || uri;
                        return `<option value="${this._esc(uri)}" ${uri === cc.property ? 'selected' : ''}>${this._esc(lbl)}</option>`;
                    }).join('')}
                </select>
            `;
            let inner = '';
            if (cc.type === 'same_value') {
                inner = `<span class="cohort-row-label">same value of</span>${propSelect}`;
            } else if (cc.type === 'value_equals') {
                inner = `${propSelect}<span class="cohort-row-label">equal to</span>
                    <input type="text" class="form-control form-control-sm cohort-input cohort-compat-value"
                           data-idx="${i}" value="${this._esc(cc.value ?? '')}" placeholder="value">
                    <button class="btn btn-sm btn-outline-secondary cohort-sample" data-idx="${i}" title="Sample values from graph">
                        <i class="bi bi-arrow-down-square"></i>
                    </button>`;
            } else if (cc.type === 'value_in') {
                inner = `${propSelect}<span class="cohort-row-label">in any of</span>
                    <input type="text" class="form-control form-control-sm cohort-input cohort-compat-values"
                           data-idx="${i}" value="${this._esc((cc.values || []).join(', '))}"
                           placeholder="comma-separated values">`;
            } else if (cc.type === 'value_range') {
                inner = `${propSelect}<span class="cohort-row-label">between</span>
                    <input type="number" class="form-control form-control-sm cohort-input cohort-compat-min"
                           data-idx="${i}" value="${cc.min ?? ''}" placeholder="min">
                    <span class="cohort-row-label">and</span>
                    <input type="number" class="form-control form-control-sm cohort-input cohort-compat-max"
                           data-idx="${i}" value="${cc.max ?? ''}" placeholder="max">`;
            }
            row.innerHTML = `${inner}
                <button class="btn btn-sm btn-link text-danger cohort-row-del" data-idx="${i}" title="Remove">
                    <i class="bi bi-x-lg"></i>
                </button>`;
            list.appendChild(row);
        }
        // wire up
        list.querySelectorAll('.cohort-compat-prop').forEach(el => {
            el.addEventListener('change', () => {
                const idx = parseInt(el.dataset.idx, 10);
                this.rule.compatibility[idx].property = el.value;
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-compat-value').forEach(el => {
            el.addEventListener('input', () => {
                const idx = parseInt(el.dataset.idx, 10);
                this.rule.compatibility[idx].value = el.value;
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-compat-values').forEach(el => {
            el.addEventListener('input', () => {
                const idx = parseInt(el.dataset.idx, 10);
                this.rule.compatibility[idx].values = el.value
                    .split(',')
                    .map(s => s.trim())
                    .filter(Boolean);
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-compat-min').forEach(el => {
            el.addEventListener('input', () => {
                const idx = parseInt(el.dataset.idx, 10);
                const v = el.value;
                this.rule.compatibility[idx].min = v === '' ? null : parseFloat(v);
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-compat-max').forEach(el => {
            el.addEventListener('input', () => {
                const idx = parseInt(el.dataset.idx, 10);
                const v = el.value;
                this.rule.compatibility[idx].max = v === '' ? null : parseFloat(v);
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-row-del').forEach(el => {
            el.addEventListener('click', () => {
                const idx = parseInt(el.dataset.idx, 10);
                this.rule.compatibility.splice(idx, 1);
                this._renderCompat();
                this.markDirty();
            });
        });
        list.querySelectorAll('.cohort-sample').forEach(el => {
            el.addEventListener('click', async () => {
                const idx = parseInt(el.dataset.idx, 10);
                const cc = this.rule.compatibility[idx];
                const choice = await this._pickSampleValue({
                    classUri: this.rule.class_uri,
                    property: cc.property,
                    currentValue: cc.value ?? '',
                    metaLabel: `${cc.property || '(no property)'} on rule entity`,
                });
                if (choice == null) return;
                cc.value = choice;
                this._renderCompat();
                this.markDirty();
            });
        });
    },

    // ---- Preview --------------------------------------------------------

    async preview() {
        this._collectFromForm();
        this._setStatus('Computing preview…');
        // 60s client-side budget: long enough for cold SQL warehouses but
        // tight enough to surface a clear error instead of letting the
        // browser/proxy hang silently when a backend dependency stalls.
        const PREVIEW_TIMEOUT_MS = 60000;
        const ctrl = new AbortController();
        const timer = setTimeout(() => ctrl.abort(), PREVIEW_TIMEOUT_MS);
        try {
            const r = await fetch('/dtwin/cohorts/dry-run', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.rule),
                signal: ctrl.signal,
            });
            if (!r.ok) {
                const err = await r.text();
                this._setStatus('Preview failed.', 'err');
                this._renderPreviewBody(`<div class="text-danger small">${this._esc(err)}</div>`);
                return;
            }
            const data = await r.json();
            this.lastPreview = data;
            this._renderPreview(data);
            const btnMat = document.getElementById('cohortMaterializeBtn');
            if (btnMat) btnMat.disabled = false;
            this._setStatus(`Preview: ${data.cohorts?.length || 0} cohorts in ${data.stats?.elapsed_ms || 0} ms.`);
        } catch (e) {
            const aborted = e && (e.name === 'AbortError' || ctrl.signal.aborted);
            const msg = aborted
                ? `Preview timed out after ${PREVIEW_TIMEOUT_MS / 1000}s. The graph backend may be cold or busy — try again in a moment.`
                : 'Preview failed.';
            this._setStatus(msg, 'err');
            this._renderPreviewBody(`<div class="text-danger small">${this._esc(msg)}</div>`);
        } finally {
            clearTimeout(timer);
        }
    },

    _renderPreview(data) {
        const cohorts = data.cohorts || [];
        const stats = data.stats || {};
        const html = `
            <div class="cohort-preview-summary mb-2">
                <div><strong>${cohorts.length}</strong> cohorts</div>
                <div class="small text-muted">
                    ${stats.grouped_member_count ?? 0} of ${stats.class_member_count ?? 0} members grouped
                    · ${stats.edge_count ?? 0} edges
                    · ${stats.elapsed_ms ?? 0} ms
                </div>
            </div>
            <div class="cohort-preview-list">
                ${cohorts.slice(0, 50).map((c) => `
                    <div class="cohort-preview-item">
                        <div class="d-flex justify-content-between align-items-start">
                            <div>
                                <span class="badge bg-primary bg-opacity-75">#${c.idx}</span>
                                <strong>${c.size}</strong> members
                            </div>
                            <code class="small text-muted" title="Cohort group hash">${this._esc(this._localName(c.id))}</code>
                        </div>
                        <ul class="cohort-preview-members">
                            ${(c.members || []).slice(0, 5).map(m =>
                                this._renderCohortMember(m)
                            ).join('')}
                            ${(c.members || []).length > 5 ? `<li class="cohort-preview-more text-muted">+${c.members.length - 5} more</li>` : ''}
                        </ul>
                    </div>
                `).join('')}
            </div>
        `;
        this._renderPreviewBody(html);
    },

    _renderCohortMember(m) {
        // Backwards-compatible: pre-enrichment members were plain URI strings.
        const obj = (m && typeof m === 'object') ? m : { uri: m, id: this._localName(m || ''), label: '' };
        const uri = obj.uri || '';
        const id = obj.id || this._localName(uri);
        const label = obj.label || '';
        const headline = label || id || uri || '(unknown)';
        const sub = label && id ? id : '';
        // The badge navigates to the knowledge-graph view focused on the
        // member entity. ``/dtwin/?section=sigmagraph&focus=<uri>`` is the
        // same deep-link pattern Domain.py mints for cross-domain entity
        // resolution and `query.js` uses for the bridge flow. We do an
        // in-page navigation (no ``target="_blank"``) so the user stays
        // inside the Digital Twin shell rather than spawning a tab.
        const graphHref = uri
            ? `/dtwin/?section=sigmagraph&focus=${encodeURIComponent(uri)}`
            : '';
        const badge = graphHref
            ? `<a class="cohort-preview-member-badge"
                  href="${this._esc(graphHref)}"
                  title="Open ${this._esc(headline)} in the knowledge graph">
                   <i class="bi bi-diagram-3 me-1"></i>${this._esc(headline)}
                   <i class="bi bi-arrow-right-short ms-1 cohort-preview-member-badge-arrow"></i>
               </a>`
            : `<span class="cohort-preview-member-badge cohort-preview-member-badge-disabled">
                   <i class="bi bi-diagram-3 me-1"></i>${this._esc(headline)}
               </span>`;
        // Single-line layout: ``[badge] [id-chip] (uri)``. The URI
        // sits inline as muted info text in parens; long URIs wrap
        // naturally because the row is a ``flex-wrap`` container, but
        // each member still occupies its own ``<li>`` so the visual
        // density is one entity per line.
        return `
            <li class="cohort-preview-member cohort-preview-member-row">
                ${badge}
                ${sub ? `<code class="cohort-preview-member-id">${this._esc(sub)}</code>` : ''}
                ${uri ? `<span class="cohort-preview-member-uri"
                              title="Entity URI — click to copy"
                              data-copy="${this._esc(uri)}">(${this._esc(uri)})</span>` : ''}
            </li>
        `;
    },

    _renderPreviewBody(html) {
        const el = document.getElementById('cohortPreviewBody');
        if (!el) return;
        el.innerHTML = html;
        el.querySelectorAll('.cohort-preview-member-uri[data-copy]').forEach(node => {
            node.addEventListener('click', async () => {
                const uri = node.getAttribute('data-copy') || '';
                if (!uri || !navigator.clipboard) return;
                try {
                    await navigator.clipboard.writeText(uri);
                    const prev = node.textContent;
                    node.textContent = 'copied ✓';
                    node.classList.add('copied');
                    setTimeout(() => {
                        node.textContent = prev;
                        node.classList.remove('copied');
                    }, 900);
                } catch { /* ignore */ }
            });
        });
    },

    // ---- Save / delete --------------------------------------------------

    async save() {
        this._collectFromForm();
        // Notifications are routed through the navbar Notification Center
        // (`showNotification`) — same as every other save flow in the app.
        // The inline status rail is reserved for transient working states
        // (Computing preview…, Unsaved changes…) and is cleared here.
        this._setStatus('');
        if (!this.rule.label || !this.rule.class_uri) {
            this._notify('A label and an entity are required.', 'warning');
            return;
        }
        if (!this._isValidRuleName(this.rule.label)) {
            this._notify(
                'Rule name must be camelCase (letters and digits only, '
                + 'starting with a letter) — e.g. ExemptStaffingPool.',
                'warning',
            );
            return;
        }
        try {
            const r = await fetch('/dtwin/cohorts/rules', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this._sanitizedRulePayload()),
            });
            if (!r.ok) {
                const msg = await this._readErrorMessage(r);
                this._notify(`Save failed: ${msg}`, 'error');
                return;
            }
            const data = await r.json();
            this.activeRuleId = data.rule.id;
            this.dirty = false;
            await this.loadRules();
            const name = data.rule.label || data.rule.id;
            this._notify(`Cohort rule "${name}" saved`, 'success');
        } catch (e) {
            this._notify('Save failed', 'error');
        }
    },

    async deleteRule(rule_id) {
        if (!confirm('Delete this rule?')) return;
        this._setStatus('');
        try {
            const r = await fetch(`/dtwin/cohorts/rules/${encodeURIComponent(rule_id)}`, {
                method: 'DELETE', credentials: 'include',
            });
            if (!r.ok) throw new Error(await r.text());
            if (this.activeRuleId === rule_id) this.newRule(true);
            await this.loadRules();
            this._notify('Cohort rule deleted', 'success');
        } catch (e) {
            this._notify('Delete failed', 'error');
        }
    },

    /**
     * Route a user-facing message through the global Notification Center.
     *
     * Falls back to ``console`` if the helper isn't loaded yet (e.g. during
     * very early init before navbar.js paints). The shape mirrors what the
     * rest of the app uses — see global/js/utils.js#showNotification.
     */
    _notify(message, type = 'info', duration) {
        if (typeof window !== 'undefined' && typeof window.showNotification === 'function') {
            window.showNotification(message, type, duration);
        } else {
            // Best-effort fallback so messages aren't silently dropped.
            const tag = type === 'error' ? 'error' : type === 'warning' ? 'warn' : 'log';
            // eslint-disable-next-line no-console
            console[tag](`[cohorts] ${message}`);
        }
    },

    // ---- Outputs / materialise -----------------------------------------

    openOutputsModal() {
        this._collectFromForm();
        const out = this.rule.output || (this.rule.output = { graph: true });
        document.getElementById('cohortOutputGraph').checked = out.graph !== false;
        const ucEnabled = !!(out.uc_table && out.uc_table.table_name);
        document.getElementById('cohortOutputUC').checked = ucEnabled;
        document.getElementById('cohortUCConfig').classList.toggle('d-none', !ucEnabled);
        document.getElementById('cohortUCCatalog').value = out.uc_table?.catalog || '';
        document.getElementById('cohortUCSchema').value = out.uc_table?.schema || '';
        document.getElementById('cohortUCTableName').value = out.uc_table?.table_name || '';
        document.getElementById('cohortUCProbeResult').innerHTML = '';
        // Inline the active rule name into the helper hints so the user
        // sees the exact predicate / table that will be written, not a
        // ``<RuleName>`` placeholder. Falls back to the placeholder when
        // the rule has no name yet (fresh draft).
        this._refreshOutputHints();
        document.getElementById('cohortOutputUC').onchange = (ev) => {
            document.getElementById('cohortUCConfig').classList.toggle('d-none', !ev.target.checked);
            if (ev.target.checked && !document.getElementById('cohortUCCatalog').value) {
                this.suggestUCTarget();
            }
        };
        new bootstrap.Modal('#cohortOutputsModal').show();
    },

    _refreshOutputHints() {
        const ruleName = (this.rule && (this.rule.label || this.rule.id)) || '';
        const predHint = document.getElementById('cohortGraphPredHint');
        if (predHint) {
            predHint.textContent = ruleName
                ? `:inCohort${ruleName}`
                : ':inCohort<RuleName>';
        }
        const tableHint = document.getElementById('cohortUcTableHint');
        if (tableHint) {
            tableHint.textContent = ruleName
                ? `cohorts_${this._toSnakeCase(ruleName)}`
                : 'cohorts_<rule_name>';
        }
    },

    // ---- UC target helpers (Auto-pick / Test write access) -------------
    //
    // Both buttons must always give the user *some* visible feedback —
    // silent failure was the recurring complaint. Strategy:
    //   1. While the request is in flight, render a subtle "Working…"
    //      line into #cohortUCProbeResult so the click registers.
    //   2. On HTTP error, parse the OntoBricks error envelope
    //      (`{error, message, detail, request_id}`) and surface the
    //      message + detail inline; also emit a toast for prominence.
    //   3. On success, suggest writes the values + a one-line
    //      provenance note; probe renders the per-check rows it
    //      already had, but now also handles the
    //      empty-checks edge case (e.g. malformed envelope).

    _renderProbeStatus(html) {
        const el = document.getElementById('cohortUCProbeResult');
        if (el) el.innerHTML = html;
    },

    _renderProbeWorking(label) {
        this._renderProbeStatus(`
            <div class="text-muted">
                <span class="spinner-border spinner-border-sm me-2" role="status"></span>
                ${this._esc(label)}
            </div>
        `);
    },

    _renderProbeError(prefix, data, fallback) {
        const message = (data && (data.message || data.error)) || fallback || 'Request failed';
        const detail = data && data.detail;
        this._renderProbeStatus(`
            <div class="text-danger">
                <i class="bi bi-x-circle me-1"></i>
                <strong>${this._esc(prefix)}:</strong> ${this._esc(message)}
                ${detail ? `<div class="text-muted small mt-1"><code>${this._esc(detail)}</code></div>` : ''}
            </div>
        `);
    },

    async _ucFetch(url, init) {
        const r = await fetch(url, { credentials: 'include', ...(init || {}) });
        let data = {};
        try { data = await r.json(); } catch { /* non-JSON body */ }
        return { ok: r.ok, status: r.status, data };
    },

    async suggestUCTarget() {
        this._renderProbeWorking('Auto-picking a Unity Catalog target…');
        // Pass the active rule name so the backend can propose
        // ``cohorts_<rule_name>`` instead of falling back to a
        // domain-level slug. The label is the source of truth (id
        // mirrors it for new rules) and is camelCase by construction.
        const ruleName = (this.rule && (this.rule.label || this.rule.id)) || '';
        const url = ruleName
            ? `/dtwin/cohorts/uc/suggest-target?rule_name=${encodeURIComponent(ruleName)}`
            : '/dtwin/cohorts/uc/suggest-target';
        let resp;
        try {
            resp = await this._ucFetch(url);
        } catch (e) {
            this._renderProbeError('Auto-pick failed (network)', null, String(e));
            this._notify('Auto-pick failed: network error', 'error');
            return;
        }
        if (!resp.ok) {
            this._renderProbeError('Auto-pick failed', resp.data, `HTTP ${resp.status}`);
            this._notify(
                `Auto-pick failed: ${resp.data.message || resp.data.error || 'HTTP ' + resp.status}`,
                'error',
            );
            return;
        }
        const { catalog = '', schema = '', table_name = '', provenance } = resp.data;
        document.getElementById('cohortUCCatalog').value = catalog;
        document.getElementById('cohortUCSchema').value = schema;
        document.getElementById('cohortUCTableName').value = table_name;
        const fq = [catalog, schema, table_name].filter(Boolean).join('.');
        const provenanceText = this._formatProvenance(provenance);
        this._renderProbeStatus(`
            <div class="text-success">
                <i class="bi bi-magic me-1"></i>
                Picked <code>${this._esc(fq || '(empty)')}</code>
                ${provenanceText ? `<span class="text-muted">(${this._esc(provenanceText)})</span>` : ''}
            </div>
        `);
        this._notify(`Auto-picked ${fq || 'empty target'}`, 'success');
    },

    // Backend returns provenance as either:
    //   - a string (e.g. "registry") for single-source picks, or
    //   - a dict (e.g. {catalog: "registry", schema: "first source table"})
    //     when catalog and schema came from different fallbacks.
    // Render both shapes as a compact human-readable string.
    _formatProvenance(provenance) {
        if (!provenance) return '';
        if (typeof provenance === 'string') return provenance;
        if (typeof provenance !== 'object') return String(provenance);
        const entries = Object.entries(provenance).filter(([, v]) => v);
        if (!entries.length) return '';
        const distinctSources = new Set(entries.map(([, v]) => String(v)));
        if (distinctSources.size === 1) {
            return `from ${[...distinctSources][0]}`;
        }
        return entries.map(([k, v]) => `${k} from ${v}`).join(', ');
    },

    async probeUCTarget() {
        const target = {
            catalog: document.getElementById('cohortUCCatalog').value,
            schema: document.getElementById('cohortUCSchema').value,
            table_name: document.getElementById('cohortUCTableName').value,
        };
        if (!target.catalog || !target.schema || !target.table_name) {
            this._renderProbeError(
                'Test write access',
                { message: 'Catalog, schema and table name are all required.' },
            );
            this._notify('Fill in catalog, schema and table name first.', 'warning');
            return;
        }
        this._renderProbeWorking(
            `Probing write access to ${target.catalog}.${target.schema}.${target.table_name}…`,
        );
        let resp;
        try {
            resp = await this._ucFetch('/dtwin/cohorts/uc/probe-write', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(target),
            });
        } catch (e) {
            this._renderProbeError('Probe failed (network)', null, String(e));
            this._notify('Probe failed: network error', 'error');
            return;
        }
        if (!resp.ok) {
            this._renderProbeError('Probe failed', resp.data, `HTTP ${resp.status}`);
            this._notify(
                `Probe failed: ${resp.data.message || resp.data.error || 'HTTP ' + resp.status}`,
                'error',
            );
            return;
        }
        const checks = resp.data.checks || [];
        if (!checks.length) {
            this._renderProbeError(
                'Probe returned no checks',
                resp.data,
                'The server returned an empty result — try again or check server logs.',
            );
            this._notify('Probe returned no checks', 'warning');
            return;
        }
        const rowsHtml = checks.map(c => `
            <div class="cohort-probe-row ${c.status}">
                <i class="bi bi-${c.status === 'ok' ? 'check-circle text-success'
                    : c.status === 'warning' ? 'exclamation-triangle text-warning'
                    : 'x-circle text-danger'} me-1"></i>
                <strong>${this._esc(c.name)}:</strong> ${this._esc(c.message)}
            </div>
        `).join('');
        const okAll = resp.data.ok === true;
        const headline = okAll
            ? '<div class="text-success mb-1"><i class="bi bi-shield-check me-1"></i>Write access looks good.</div>'
            : '<div class="text-danger mb-1"><i class="bi bi-shield-exclamation me-1"></i>Write access has issues — see below.</div>';
        this._renderProbeStatus(headline + rowsHtml);
        this._notify(
            okAll ? 'Write access OK' : 'Write access has issues',
            okAll ? 'success' : 'warning',
        );
    },

    async saveOutputs() {
        const graph = document.getElementById('cohortOutputGraph').checked;
        const ucOn = document.getElementById('cohortOutputUC').checked;
        if (!this.rule.output) this.rule.output = {};
        this.rule.output.graph = graph;
        if (ucOn) {
            this.rule.output.uc_table = {
                catalog: document.getElementById('cohortUCCatalog').value,
                schema: document.getElementById('cohortUCSchema').value,
                table_name: document.getElementById('cohortUCTableName').value,
            };
        } else {
            delete this.rule.output.uc_table;
        }
        bootstrap.Modal.getInstance(document.getElementById('cohortOutputsModal')).hide();

        // Outputs is configured from the Digital Twin run page where there is
        // no Save-rule button — persist the change to the registry directly so
        // it survives reloads. On the design page (with the Save form) we just
        // mark dirty; the user persists everything together via Save rule.
        const onRunPage = !document.getElementById('cohortRuleLabel');
        if (onRunPage && this.activeRuleId) {
            try {
                const r = await fetch('/dtwin/cohorts/rules', {
                    method: 'POST',
                    credentials: 'include',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(this._sanitizedRulePayload()),
                });
                if (!r.ok) {
                    const msg = await this._readErrorMessage(r);
                    this._notify(`Save outputs failed: ${msg}`, 'error');
                    return;
                }
                await this.loadRules();
                this._notify('Cohort outputs saved', 'success');
                this._renderRuleSummary();
            } catch (e) {
                this._notify('Save outputs failed', 'error');
            }
            return;
        }
        this.markDirty();
        this._setStatus('Outputs configured (remember to Save the rule).', 'ok');
    },

    openMaterializeModal() {
        if (!this.lastPreview) {
            this._notify('Run Preview first.', 'warning');
            return;
        }
        if (!this.activeRuleId) {
            this._notify('Save the rule before materialising.', 'warning');
            return;
        }
        const triples = (this.lastPreview.cohorts?.length || 0) * 4
            + (this.lastPreview.stats?.grouped_member_count || 0);
        const ucT = this.rule.output?.uc_table;
        const ucLine = ucT && ucT.table_name
            ? `<div class="form-check">
                   <input class="form-check-input" type="checkbox" id="cohortDoUC" checked>
                   <label class="form-check-label" for="cohortDoUC">
                       Unity Catalog table — ${this.lastPreview.stats?.grouped_member_count || 0}
                       rows → <code>${this._esc(ucT.catalog)}.${this._esc(ucT.schema)}.${this._esc(ucT.table_name)}</code>
                   </label>
               </div>`
            : '';
        const inCohortPred = `:inCohort${this._esc(this.rule.id || this.rule.label || '')}`;
        const graphLine = this.rule.output?.graph !== false
            ? `<div class="form-check">
                   <input class="form-check-input" type="checkbox" id="cohortDoGraph" checked>
                   <label class="form-check-label" for="cohortDoGraph">
                       Graph triples — ${triples} <code>${inCohortPred}</code> + cohort entity triples
                   </label>
               </div>`
            : '';
        document.getElementById('cohortMaterializeBody').innerHTML = `
            ${graphLine}${ucLine}
            <div class="text-muted small mt-2">Replacing previous outputs for this rule (idempotent).</div>
        `;
        new bootstrap.Modal('#cohortMaterializeModal').show();
    },

    async materialize() {
        try {
            const r = await fetch('/dtwin/cohorts/materialize', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ rule_id: this.activeRuleId }),
            });
            const data = await r.json();
            if (!r.ok) {
                this._notify(`Materialise failed: ${data.detail || ''}`, 'error');
                return;
            }
            const msg = `Wrote ${data.materialized_triples || 0} triples` +
                (data.uc_rows_written ? `, ${data.uc_rows_written} UC rows` : '');
            this._notify(msg, 'success');
            bootstrap.Modal.getInstance(document.getElementById('cohortMaterializeModal'))?.hide();
        } catch (e) {
            this._notify('Materialise failed.', 'error');
        }
    },

    // ---- Path trace (per-hop diagnostic) -------------------------------

    async tracePaths() {
        this._collectFromForm();
        const target = document.getElementById('cohortTraceResult');
        if (!target) return;
        if (!(this.rule.links || []).length) {
            target.innerHTML = `<div class="text-muted">
                No linkage rules — every compatible pair is linked directly,
                so there's no path to trace.
            </div>`;
            return;
        }
        target.innerHTML = `<div class="text-muted">Computing trace…</div>`;
        try {
            const r = await fetch('/dtwin/cohorts/preview/path-trace', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    class_uri: this.rule.class_uri,
                    links: this.rule.links,
                    compatibility: this.rule.compatibility || [],
                }),
            });
            if (!r.ok) {
                const err = await r.text();
                target.innerHTML = `<div class="text-danger">${this._esc(err)}</div>`;
                return;
            }
            const data = await r.json();
            target.innerHTML = this._renderTraceHtml(data);
        } catch (e) {
            target.innerHTML = `<div class="text-danger">${this._esc(String(e))}</div>`;
        }
    },

    _renderTraceHtml(data) {
        const memberCount = data.class_member_count ?? 0;
        const survivors = data.survivor_count ?? 0;
        const links = data.links || [];
        const head = `<div class="cohort-trace-head small text-muted mb-2">
            Stage 1: <strong>${memberCount}</strong> ${this._esc(this._localName(data.class_uri || ''))} members
            → Stage 3a: <strong>${survivors}</strong> survivor${survivors === 1 ? '' : 's'}
            after compatibility.
        </div>`;
        if (!links.length) {
            return head + `<div class="text-muted small">No links to trace.</div>`;
        }
        const cards = links.map((lt, i) => this._renderTraceLink(lt, i)).join('');
        return head + cards;
    },

    _renderTraceLink(lt, idx) {
        const hops = lt.hops || [];
        const collapsedAt = hops.findIndex(h => (h.out_frontier ?? 0) === 0);
        const totalEdges = lt.edge_count ?? 0;
        const summary = totalEdges > 0
            ? `<span class="badge bg-success bg-opacity-25 text-success">${totalEdges} edge${totalEdges === 1 ? '' : 's'}</span>`
            : `<span class="badge bg-danger bg-opacity-25 text-danger">0 edges</span>`;
        const rows = hops.map((h, k) => {
            const collapse = collapsedAt === k;
            const dropped = (h.dropped_type || 0) + (h.dropped_where || 0);
            const reasons = [];
            if (h.dropped_type) reasons.push(`${h.dropped_type} dropped by <code>target_class</code>`);
            if (h.dropped_where) reasons.push(`${h.dropped_where} dropped by <code>where</code>`);
            const reasonsHtml = reasons.length ? reasons.join('; ') : '—';
            const arrow = collapse ? '<i class="bi bi-x-circle-fill text-danger ms-1"></i>' : '';
            return `<tr class="${collapse ? 'cohort-trace-collapse' : ''}">
                <td class="text-muted">${k + 1}</td>
                <td>
                    <code>${this._esc(this._localName(h.via) || '?')}</code>
                    →
                    <code>${this._esc(this._localName(h.target_class) || '?')}</code>
                    ${h.where_count ? `<i class="bi bi-funnel-fill ms-1 text-info" title="${h.where_count} where filter${h.where_count === 1 ? '' : 's'}"></i>` : ''}
                </td>
                <td class="text-end"><strong>${h.in_frontier ?? 0}</strong></td>
                <td class="text-end">${h.neighbours_raw ?? 0}</td>
                <td class="text-end text-muted" title="${this._esc(reasonsHtml)}">${dropped}</td>
                <td class="text-end"><strong>${h.out_frontier ?? 0}</strong>${arrow}</td>
            </tr>`;
        }).join('');
        const diag = collapsedAt >= 0
            ? `<div class="cohort-trace-diag small mt-2">
                <i class="bi bi-info-circle text-danger me-1"></i>
                Path collapses at hop ${collapsedAt + 1}
                (<code>${this._esc(this._localName(hops[collapsedAt].via) || '?')}</code>
                → <code>${this._esc(this._localName(hops[collapsedAt].target_class) || '?')}</code>):
                ${hops[collapsedAt].neighbours_raw === 0
                    ? 'no neighbours found via this predicate. Check the predicate URI and whether your data actually uses it.'
                    : (hops[collapsedAt].dropped_type === hops[collapsedAt].neighbours_raw
                        ? "every neighbour was rejected by <code>target_class</code>. The hop's target entity URI probably doesn't match the URIs used in <code>rdf:type</code> triples."
                        : (hops[collapsedAt].dropped_where === (hops[collapsedAt].neighbours_raw - hops[collapsedAt].dropped_type)
                            ? 'every neighbour was rejected by the hop <code>where</code> filter. Check the property URI, the literal value (case / type), and the <code>allow_missing</code> flag.'
                            : 'a mix of filters dropped every neighbour — open the rule JSON and confirm each filter against actual data.')
                    )
                }
            </div>`
            : '';
        return `<div class="cohort-trace-link card mb-2">
            <div class="card-body p-2">
                <div class="d-flex justify-content-between align-items-center mb-2">
                    <strong class="small">Path ${idx + 1}</strong>
                    ${summary}
                </div>
                <table class="table table-sm cohort-trace-table mb-0 small">
                    <thead>
                        <tr class="text-muted">
                            <th>#</th>
                            <th>hop</th>
                            <th class="text-end" title="distinct nodes at hop entry">in</th>
                            <th class="text-end" title="raw outbound (subject, predicate) edges">raw</th>
                            <th class="text-end" title="rejected by target_class + where">drop</th>
                            <th class="text-end" title="distinct surviving neighbours">out</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
                ${diag}
            </div>
        </div>`;
    },

    // ---- Explain (Why? / Why not?) -------------------------------------

    async explain() {
        const target = (document.getElementById('cohortExplainTarget').value || '').trim();
        if (!target) return;
        // Sync the form into this.rule before posting (mirrors what
        // preview() does). Without this, an explain run right after
        // the user tweaked the form would post a stale rule and the
        // diagnostic would describe a configuration the user no
        // longer has on screen.
        if (typeof this._collectFromForm === 'function'
                && document.getElementById('cohortRuleLabel')) {
            this._collectFromForm();
        }
        const r = await fetch('/dtwin/cohorts/explain', {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ rule: this.rule, target }),
        });
        const data = await r.json();
        let html = '';
        if (!data.in_class) {
            html = `<div class="text-muted small">${this._esc(data.reason || 'Not in target entity.')}</div>`;
        } else if (data.in_cohort) {
            html = `<div class="small">
                <strong>${this._esc(this._localName(target))}</strong> is in cohort #${data.in_cohort.idx}
                (size ${data.in_cohort.size}).
            </div>`;
        } else {
            const failing = data.failing_constraints || [];
            html = `<div class="small">
                <strong>${this._esc(this._localName(target))}</strong> did not end up in any cohort.
                ${failing.length ? `<ul class="mb-0">${failing.map(f =>
                    `<li>${this._esc(f.type)} on <code>${this._esc(this._localName(f.property))}</code>:
                     actual <code>${this._esc(String(f.actual ?? '∅'))}</code>,
                     expected <code>${this._esc(JSON.stringify(f.expected))}</code></li>`
                ).join('')}</ul>` : '<div class="text-muted">No incompatibilities — likely no edge to a cohort.</div>'}
            </div>`;
        }
        document.getElementById('cohortExplainResult').innerHTML = html;
    },

    // ---- Sample-values picker ------------------------------------------
    //
    // Replaces the old ``window.prompt`` call: shows a Bootstrap modal
    // listing values fetched from ``/dtwin/cohorts/sample-values`` with
    // a filter and a free-form custom-value input. Returns a Promise
    // that resolves with the picked string or ``null`` on cancel.

    async _pickSampleValue({ classUri, property, currentValue = '', metaLabel = '' }) {
        if (!classUri || !property) {
            this._setStatus('Pick a target entity and property first.', 'warn');
            return null;
        }
        const modalEl = document.getElementById('cohortSampleValuesModal');
        if (!modalEl) {
            this._setStatus('Sample values dialog is missing.', 'err');
            return null;
        }
        const listEl = modalEl.querySelector('#cohortSampleValuesList');
        const filterEl = modalEl.querySelector('#cohortSampleValuesFilter');
        const customEl = modalEl.querySelector('#cohortSampleValuesCustom');
        const metaEl = modalEl.querySelector('#cohortSampleValuesMeta');
        const applyBtn = modalEl.querySelector('#cohortSampleValuesApplyBtn');
        listEl.innerHTML = '<div class="text-muted small fst-italic px-2 py-3">Loading…</div>';
        filterEl.value = '';
        customEl.value = currentValue || '';
        metaEl.textContent = metaLabel || `${property} on ${classUri}`;
        const modal = bootstrap.Modal.getOrCreateInstance(modalEl);
        modal.show();
        let values = [];
        try {
            const r = await fetch('/dtwin/cohorts/sample-values', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ class_uri: classUri, property, limit: 50 }),
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const data = await r.json();
            values = (data.values || []).map(v => String(v));
        } catch (e) {
            listEl.innerHTML = `<div class="text-danger small px-2 py-3">Failed to load values: ${this._esc(e.message || e)}</div>`;
        }

        const renderList = () => {
            const needle = (filterEl.value || '').trim().toLowerCase();
            const visible = needle
                ? values.filter(v => v.toLowerCase().includes(needle))
                : values;
            if (!visible.length) {
                listEl.innerHTML = '<div class="text-muted small fst-italic px-2 py-3">No values match.</div>';
                return;
            }
            listEl.innerHTML = visible.map(v => {
                const sel = v === customEl.value ? ' active' : '';
                return `<button type="button"
                                class="list-group-item list-group-item-action small${sel}"
                                data-value="${this._esc(v)}">${this._esc(v)}</button>`;
            }).join('');
        };
        if (values.length) renderList();

        return new Promise(resolve => {
            let resolved = false;
            const finish = (val) => {
                if (resolved) return;
                resolved = true;
                listEl.removeEventListener('click', onListClick);
                filterEl.removeEventListener('input', onFilterInput);
                applyBtn.removeEventListener('click', onApply);
                modalEl.removeEventListener('hidden.bs.modal', onHidden);
                modal.hide();
                resolve(val);
            };
            const onListClick = (e) => {
                const btn = e.target.closest('[data-value]');
                if (!btn) return;
                customEl.value = btn.dataset.value;
                listEl.querySelectorAll('.list-group-item').forEach(el =>
                    el.classList.toggle('active', el === btn));
            };
            const onFilterInput = () => renderList();
            const onApply = () => {
                const v = customEl.value;
                finish(v === '' ? null : v);
            };
            const onHidden = () => finish(null);
            listEl.addEventListener('click', onListClick);
            filterEl.addEventListener('input', onFilterInput);
            applyBtn.addEventListener('click', onApply);
            modalEl.addEventListener('hidden.bs.modal', onHidden);
        });
    },

    // ---- Stage 2: NL agent ---------------------------------------------

    async runAgent() {
        const promptEl = document.getElementById('cohortAgentPrompt');
        const btn = document.getElementById('cohortAgentRunBtn');
        const replyEl = document.getElementById('cohortAgentReply');
        const traceEl = document.getElementById('cohortAgentTraceBody');
        if (!promptEl) return;
        const prompt = (promptEl.value || '').trim();
        if (!prompt) {
            this._setStatus('Type a prompt for the agent first.', 'warn');
            return;
        }

        const originalBtnHtml = btn ? btn.innerHTML : '';
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1" role="status" aria-hidden="true"></span>Thinking…';
        }
        if (replyEl) {
            replyEl.classList.remove('d-none');
            replyEl.className = 'small mt-2 text-muted';
            replyEl.textContent = 'The agent is introspecting the ontology…';
        }
        if (traceEl) traceEl.innerHTML = '<em>Running…</em>';
        this._setStatus('Running cohort agent…', 'info');

        try {
            const r = await fetch('/dtwin/cohorts/agent', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ prompt }),
            });
            if (!r.ok) {
                const txt = await r.text();
                throw new Error(`HTTP ${r.status}: ${txt.slice(0, 240)}`);
            }
            const data = await r.json();

            this._renderAgentTrace(data);

            if (data && data.rule) {
                this._applyRuleFromAgent(data.rule);
                if (replyEl) {
                    replyEl.className = 'small mt-2 text-success';
                    const md = (data.reply || 'Rule generated. Review the form, then save.').toString();
                    replyEl.textContent = md;
                }
                this._setStatus(
                    `Agent proposed rule "${data.rule.label || data.rule.id}". Review and save.`,
                    'ok',
                );
            } else {
                if (replyEl) {
                    replyEl.className = 'small mt-2 text-warning';
                    replyEl.textContent = (data && data.reply)
                        || 'The agent did not produce a rule. Try rephrasing your request.';
                }
                this._setStatus('Agent returned no rule.', 'warn');
            }
        } catch (e) {
            if (replyEl) {
                replyEl.className = 'small mt-2 text-danger';
                replyEl.textContent = `Agent failed: ${e.message || e}`;
            }
            if (traceEl) traceEl.innerHTML = '<em class="text-danger">Run failed.</em>';
            this._setStatus('Agent failed. See banner.', 'err');
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = originalBtnHtml || '<i class="bi bi-magic me-1"></i>Generate rule';
            }
        }
    },

    _renderAgentTrace(data) {
        const traceEl = document.getElementById('cohortAgentTraceBody');
        if (!traceEl) return;
        const tools = (data && data.tools) || [];
        const iters = (data && data.iterations) || 0;
        const usage = (data && data.usage) || {};
        if (!tools.length) {
            traceEl.innerHTML = `<em>No tool calls (iterations: ${iters}).</em>`;
            return;
        }
        const rows = tools.map((t, i) => `
            <li>
                <span class="text-muted">${i + 1}.</span>
                <code>${this._esc(t.name || '?')}</code>
                <span class="text-muted">${t.duration_ms || 0} ms</span>
            </li>
        `).join('');
        const tokens = (usage.prompt_tokens || 0) + (usage.completion_tokens || 0);
        traceEl.innerHTML = `
            <div class="mb-1">
                ${tools.length} tool call${tools.length === 1 ? '' : 's'}
                · ${iters} iteration${iters === 1 ? '' : 's'}
                · ${tokens} tokens
            </div>
            <ol class="mb-0 ps-3">${rows}</ol>
        `;
    },

    _applyRuleFromAgent(rule) {
        if (!rule || typeof rule !== 'object') return;
        this.activeRuleId = null;
        this.rule = JSON.parse(JSON.stringify(rule));
        if (!this.rule.output) this.rule.output = { graph: true };
        if (!this.rule.min_size || this.rule.min_size < 2) this.rule.min_size = 2;
        if (!this.rule.links_combine) this.rule.links_combine = 'any';
        if (!this.rule.group_type) this.rule.group_type = 'connected';
        // The agent has historically returned labels like "Exempt staffing
        // pool" — coerce to the camelCase form the form now enforces, and
        // mirror it onto ``id`` so downstream lookups stay consistent.
        const camel = this._toCamelCase(this.rule.label || this.rule.id || '');
        if (camel) {
            this.rule.label = camel;
            this.rule.id = camel;
        }
        this.dirty = true;
        this._hydrateForm();
        this._renderRulesList();
        this._showBuildTab();
    },

    _showBuildTab() {
        const btn = document.getElementById('cohortTabBuildBtn');
        if (!btn || typeof bootstrap === 'undefined' || !bootstrap.Tab) return;
        try { bootstrap.Tab.getOrCreateInstance(btn).show(); } catch { /* noop */ }
    },

    // ---- helpers --------------------------------------------------------

    /**
     * Normalize an arbitrary string into a camelCase rule name.
     *
     * Splits on any non-alphanumeric run, capitalises the first letter of
     * each segment, preserves digits, and drops anything that isn't
     * ``[A-Za-z0-9]``. A leading digit is dropped because rule names also
     * serve as JS-friendly identifiers downstream (UC suffixes, etc.).
     */
    _toCamelCase(s) {
        const raw = (s || '').toString();
        const segments = raw.split(/[^A-Za-z0-9]+/).filter(Boolean);
        if (!segments.length) return '';
        const out = segments
            .map(seg => seg.charAt(0).toUpperCase() + seg.slice(1))
            .join('');
        return out.replace(/^[0-9]+/, '');
    },

    _isValidRuleName(s) {
        return /^[A-Za-z][A-Za-z0-9]*$/.test((s || '').toString());
    },

    /**
     * Convert a camelCase / PascalCase rule name to snake_case.
     *
     * Mirrors ``CohortService._snake_case`` (Python) so the
     * UC-table hint shown in the Configure-outputs modal matches what
     * the Auto-pick endpoint will actually return:
     * ``ExemptStaffingPool`` → ``exempt_staffing_pool`` and
     * ``URLPathUsers`` → ``url_path_users``. Returns ``''`` on empty
     * input so callers can render a placeholder instead of an empty
     * suffix.
     */
    _toSnakeCase(s) {
        if (!s) return '';
        let spaced = String(s)
            .replace(/(?<=[a-z0-9])([A-Z])/g, '_$1')
            .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2');
        return spaced.toLowerCase()
            .replace(/[^a-z0-9]+/g, '_')
            .replace(/^_+|_+$/g, '');
    },

    _localName(uri) {
        if (!uri) return '';
        const i = Math.max(uri.lastIndexOf('/'), uri.lastIndexOf('#'));
        return i >= 0 ? uri.slice(i + 1) : uri;
    },

    _esc(s) {
        return (s ?? '').toString()
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    },

    _setStatus(msg, kind = 'info') {
        const el = document.getElementById('cohortStatus');
        if (!el) return;
        const colors = {
            info: 'text-muted', warn: 'text-warning', err: 'text-danger', ok: 'text-success',
        };
        el.className = `ms-auto small ${colors[kind] || colors.info}`;
        el.textContent = msg;
    },
};

document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('cohorts-section')) {
        try {
            CohortModule.init();
            CohortModule._hookLabelInput();
        } catch (e) {
            console.error('CohortModule init failed', e);
        }
    }
});
