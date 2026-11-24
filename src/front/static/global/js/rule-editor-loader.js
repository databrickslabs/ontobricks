/**
 * Opens the Ontology rule editors (Simple = IF-THEN graph editor,
 * Complex = SPARQL editor) in place, from Graph → Inference or
 * Simulation → Reasoning.
 *
 * The editor markup is already in the page (#if-then-section; its list is
 * hidden by CSS, the modals are not). The
 * editor scripts and CSS are loaded on first use, then the modules are
 * initialised in standalone mode. The core ontology script is deliberately
 * not loaded: it sets up Ontology page state on load.
 */
const RuleEditorLoader = {
    _ready: null,

    // Asset version comes from this script tag's data-asset-version attribute
    // (read at load time, while this script is executing).
    _assetVersion: (document.currentScript && document.currentScript.dataset.assetVersion) || '',

    _assets: [
        { css: 'ontology/css/ontology-if-then.css' },
        { css: 'ontology/css/ontology-business-rules.css' },
        { js: 'ontology/js/ontology-if-then.js' },
        { js: 'ontology/js/ontology-business-rules.js' },
    ],

    _url(path) {
        const v = this._assetVersion;
        return `/static/${path}${v ? `?v=${encodeURIComponent(v)}` : ''}`;
    },

    _addCss(path) {
        return new Promise((resolve, reject) => {
            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = this._url(path);
            link.onload = resolve;
            link.onerror = () => reject(new Error(path));
            document.head.appendChild(link);
        });
    },

    _addScript(path) {
        return new Promise((resolve, reject) => {
            const s = document.createElement('script');
            s.src = this._url(path);
            s.onload = resolve;
            s.onerror = () => reject(new Error(path));
            document.head.appendChild(s);
        });
    },

    // Scripts load one after another: ontology-business-rules.js reads
    // IfThenModule at call time, so the order matters.
    load() {
        if (!this._ready) {
            this._ready = (async () => {
                for (const a of this._assets) {
                    if (a.css) await this._addCss(a.css);
                    else await this._addScript(a.js);
                }
                await IfThenModule.initStandalone();
                await BusinessRulesModule.initStandalone();
            })().catch((err) => {
                this._ready = null;
                throw err;
            });
        }
        return this._ready;
    },

    _canEdit() {
        return !!(window.OB && typeof window.OB.canEditOntology === 'function' && window.OB.canEditOntology());
    },

    async open(key, name, { canEdit = this._canEdit() } = {}) {
        try {
            await this.load();
        } catch (err) {
            console.error('[RuleEditor] Could not load the rule editor:', err);
            if (typeof showNotification === 'function') showNotification('Could not open the rule editor', 'error');
            return;
        }

        if (key === 'if_then') {
            await IfThenModule.loadRules();
            const index = IfThenModule.rules.findIndex((r) => r.name === name);
            if (index < 0) return this._missing(key, name);
            if (canEdit) IfThenModule.editRule(index);
            else IfThenModule.viewRule(index);
            return;
        }

        if (key === 'sparql_rules') {
            await BusinessRulesModule._loadRules('sparql_rules');
            const index = BusinessRulesModule.sparqlRules.findIndex((r) => r.name === name);
            if (index < 0) return this._missing(key, name);
            BusinessRulesModule.sparqlOpenEditor(index, { readOnly: !canEdit });
        }
    },

    _missing(key, name) {
        if (typeof showNotification === 'function') {
            showNotification(`Rule "${name}" no longer exists. The list has been refreshed.`, 'warning');
        }
        document.dispatchEvent(new CustomEvent('ob:rules-saved', { detail: { key } }));
    },

    // Wire one PhaseTree: a rule click opens the editor; a Save refreshes that
    // phase's rule list and keeps the ticked rules ticked.
    attach(tree, root) {
        root.addEventListener('ob:rule-open', (e) => {
            this.open(e.detail.key, e.detail.value);
        });
        document.addEventListener('ob:rules-saved', (e) => {
            const key = e.detail && e.detail.key;
            if (key !== 'if_then' && key !== 'sparql_rules') return;
            this._refreshList(tree, key);
        });
    },

    async _refreshList(tree, key) {
        const selection = tree.selectedRules();
        const url = key === 'if_then' ? '/ontology/if-then/list' : '/ontology/rules/sparql_rules/list';
        try {
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = await resp.json();
            tree.renderRules(key, PhaseTree.fromNamedRules(data.rules));
            tree.applyRules(selection);
        } catch (err) {
            console.error('[RuleEditor] Could not refresh the rule list:', err);
        }
    },
};

window.RuleEditorLoader = RuleEditorLoader;
