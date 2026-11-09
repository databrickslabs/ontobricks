/**
 * Phase / rule picker for the simulation workbench — the same PhaseTree as
 * Knowledge Graph → Inference (Simple / IF-THEN and Complex / SPARQL rules).
 */
const SimulationRules = {
    _tree: null,
    _loaded: false,
    _pending: null,

    bind() {
        if (this._tree) return;
        const root = document.getElementById('simWbPhases');
        if (!root || !window.PhaseTree) return;
        this._tree = new PhaseTree(root);
        this.load();
    },

    async load() {
        if (this._loaded) {
            this.apply(this._pending);
            return;
        }
        if (window.SimulationWorkbench) SimulationWorkbench.setTabBusy('reasoning', true);
        let rules = { if_then: [], sparql_rules: [] };
        try {
            const [ifThen, sparql] = await Promise.all([
                SimulationPage.api('/ontology/if-then/list'),
                SimulationPage.api('/ontology/rules/sparql_rules/list'),
            ]);
            rules = { if_then: ifThen.rules, sparql_rules: sparql.rules };
        } catch (err) {
            console.error('[Simulation] Could not load reasoning rules:', err);
        } finally {
            this._tree.expandableKeys().forEach((type) => {
                this._tree.renderRules(type, PhaseTree.fromNamedRules(rules[type]));
            });
            this._loaded = true;
            this.apply(this._pending);
            if (window.SimulationWorkbench) SimulationWorkbench.setTabBusy('reasoning', false);
        }
    },

    apply(ruleNames) {
        this._pending = ruleNames || null;
        if (!this._tree) return;
        if (this._loaded) this._tree.applyRules(this._pending);
        else this._tree.sync();
    },

    collectPhases() {
        return this._tree ? this._tree.phases() : {};
    },

    collectRuleNames(phases) {
        return this._tree ? this._tree.selectedRules(phases) : {};
    },
};

window.SimulationRules = SimulationRules;
