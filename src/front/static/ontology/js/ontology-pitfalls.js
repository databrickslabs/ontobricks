/**
 * OntoBricks — ontology-pitfalls.js
 * Ontology Pitfalls Analysis module (D2KLab P1.1–P4.7).
 */
window.PitfallsModule = (function () {

    // Pitfall IDs that do NOT require ML / SentenceTransformer (fast, graph-only)
    const FAST_PATTERNS = ['P1.1', 'P1.2', 'P1.3', 'P2.1', 'P2.2', 'P2.4', 'P2.5', 'P3.1', 'P3.2', 'P3.3', 'P4.7'];

    // Static taxonomy — mirrors PITFALL_TAXONOMY in runner.py (no server round-trip needed)
    const STATIC_TAXONOMY = [
        { category: 'Logical Issues',              pitfall_id: 'P1.1', title: 'Parent disjoint with children',                      description: 'A class is declared disjoint from one of its own subclasses. This is a contradiction: no individual can simultaneously belong to both a class and its subclass if they are disjoint, making the subclass unsatisfiable (always empty).' },
        { category: 'Logical Issues',              pitfall_id: 'P1.2', title: 'Entity as subclass of both parent and grandparent',   description: 'A class is explicitly declared as a direct subclass of both a parent class and one of that parent\'s ancestors. The declaration to the ancestor is redundant because subclass transitivity already implies it, and it can mislead reasoners.' },
        { category: 'Logical Issues',              pitfall_id: 'P1.3', title: 'Logical inconsistencies',                             description: 'Axioms in the ontology produce unsatisfiable classes — e.g., a class is simultaneously defined as a subclass of two disjoint classes, or a restriction forces contradictory types. An OWL reasoner would flag these classes as equivalent to owl:Nothing.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.1', title: 'Not connected hierarchies',                           description: 'The ontology contains isolated class trees with no common root other than owl:Thing. Disconnected hierarchies often indicate that separate sub-ontologies were merged without alignment, or that top-level grouping concepts are missing.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.2', title: 'Single subclass parent',                              description: 'A class has exactly one direct subclass. This is often a sign of unnecessary intermediate nodes: if a parent has only one child, the hierarchy could usually be flattened without loss of semantics, unless the parent is used in axioms independently.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.3', title: 'Superfluous disjointness',                            description: 'Two classes are declared disjoint when one is already a subclass of the other, or when disjointness is already implied by the hierarchy. The explicit disjointness assertion is redundant and may create unintended logical side-effects.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.4', title: 'Single subproperty parent',                           description: 'A property has exactly one direct sub-property. Mirrors P2.2 for properties: if a property hierarchy node has only one child, it may be a superfluous intermediate that adds no modelling value.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.5', title: 'Range/Domain expansion',                              description: 'A sub-property declares a domain or range that is broader than its parent property\'s domain/range. This violates the inheritance contract: a sub-property should restrict (narrow) rather than expand the domain/range it inherits.' },
        { category: 'Structural Issues',           pitfall_id: 'P2.6', title: 'Possible hierarchy among properties',                 description: 'Two or more properties have names so similar that one may be a specialisation of the other, yet no rdfs:subPropertyOf link is declared between them. This check flags candidate pairs worth reviewing for a missing sub-property relationship.' },
        { category: 'Redundancy / Naming Issues',  pitfall_id: 'P3.1', title: 'Properties replicating standard RDF ones',            description: 'The ontology defines custom properties that duplicate well-known RDF/RDFS/OWL vocabulary (e.g., a custom \'hasLabel\' property when rdfs:label already exists). Using standard vocabulary improves interoperability and avoids redundant machinery.' },
        { category: 'Redundancy / Naming Issues',  pitfall_id: 'P3.2', title: 'Range in property title',                             description: 'A property name encodes its range type (e.g., \'hasPersonName\', \'containsEvent\'). Embedding the range in the label couples naming to structure: renaming or changing the range requires renaming the property, and it conflates two distinct modelling concerns.' },
        { category: 'Redundancy / Naming Issues',  pitfall_id: 'P3.3', title: 'Possible hierarchy among classes',                    description: 'Two or more class names are so similar that a subclass relationship may exist but is not declared. This check flags candidate pairs for human review to determine whether a missing rdfs:subClassOf should be added.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.1', title: 'Symmetric property not declared symmetric',           description: 'A property appears to be used symmetrically in the data (whenever A→B exists, B→A also exists) but is not declared owl:SymmetricProperty. Declaring it symmetric allows reasoners to infer the reverse direction automatically.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.2', title: 'Inverse property pairs not declared inverse',         description: 'Two properties appear to be mutual inverses in usage (A prop1 B and B prop2 A), but owl:inverseOf is not declared between them. Declaring the inverse relationship enables reasoners to infer one direction from the other.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.3', title: 'Transitive property not declared transitive',         description: 'A property shows transitive usage patterns (A→B, B→C implies A→C) but is not declared owl:TransitiveProperty. Adding the declaration lets reasoners close the transitive chain automatically.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.4', title: 'Missing domain or range',                             description: 'An object property or datatype property has no rdfs:domain or rdfs:range declaration. Without these constraints, reasoners cannot infer the types of subjects or objects, reducing the expressiveness and validation power of the ontology.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.5', title: 'Unconnected classes (no usage)',                      description: 'One or more classes are never used as domain, range, or type in any property assertion or restriction. Such orphan classes add vocabulary without contributing to the ontology\'s modelling or reasoning.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.6', title: 'Semantically equivalent labels',                      description: 'Two or more classes or properties carry labels that are semantically similar or synonymous (detected via sentence-embedding cosine similarity). These may be duplicates or candidates for merging, equivalence declarations, or at least a clarifying note.' },
        { category: 'Semantic Issues',             pitfall_id: 'P4.7', title: 'Missing labels',                                      description: 'One or more classes or properties have no rdfs:label. Labels are essential for human-readable documentation, UI display, and tool interoperability. Every named entity in a published ontology should carry at least one label.' },
    ];

    const CATEGORY_META = {
        'Logical Issues':           { icon: 'bi-exclamation-triangle', badge: 'bg-danger' },
        'Structural Issues':        { icon: 'bi-diagram-3',            badge: 'bg-warning text-dark' },
        'Redundancy / Naming Issues': { icon: 'bi-tag',                badge: 'bg-secondary' },
        'Semantic Issues':          { icon: 'bi-braces',               badge: 'bg-info' },
    };

    let _taxonomy = [];
    let _selectedPatterns = new Set();
    let _pollTimer = null;
    let _initialized = false;

    // ── Init ──────────────────────────────────────────────────────────────────

    function init() {
        if (_initialized) return;
        _initialized = true;
        document.getElementById('pitfallsRunBtn')?.addEventListener('click', run);
        document.getElementById('pitfallsSelectAllBtn')?.addEventListener('click', selectAll);
        document.getElementById('pitfallsFastBtn')?.addEventListener('click', selectFast);
        // Taxonomy is static — render immediately, no server round-trip needed
        _taxonomy = STATIC_TAXONOMY;
        _renderPatternSelector();
        selectAll();
    }

    // ── Pattern selector ──────────────────────────────────────────────────────

    function _renderPatternSelector() {
        const body = document.getElementById('pitfallsPatternBody');
        if (!body) return;

        // Group by category
        const byCategory = {};
        for (const entry of _taxonomy) {
            const cat = entry.category;
            if (!byCategory[cat]) byCategory[cat] = [];
            byCategory[cat].push(entry);
        }

        const isFast = id => FAST_PATTERNS.includes(id);

        let html = '<div class="row g-2">';
        for (const [cat, entries] of Object.entries(byCategory)) {
            const meta = CATEGORY_META[cat] || { icon: 'bi-circle', badge: 'bg-light text-dark' };
            html += `
                <div class="col-12 col-md-6">
                    <div class="small fw-semibold text-muted mb-1">
                        <i class="bi ${meta.icon} me-1"></i>${cat}
                    </div>`;
            for (const e of entries) {
                const desc = e.description ? e.description.replace(/"/g, '&quot;') : '';
                const speedTag = isFast(e.pitfall_id)
                    ? '<i class="bi bi-lightning-fill text-warning ms-1" title="Fast check (no ML required)" style="font-size:.7em"></i>'
                    : '<i class="bi bi-cpu text-info ms-1" title="Requires ML dependencies" style="font-size:.7em"></i>';
                const infoIcon = desc
                    ? `<i class="bi bi-info-circle text-muted ms-1"
                          data-bs-toggle="tooltip" data-bs-placement="right"
                          data-bs-custom-class="pitfall-tooltip"
                          title="${desc}"
                          style="font-size:.75em;cursor:help;"></i>`
                    : '';
                html += `
                    <div class="form-check form-check-sm d-flex align-items-center gap-1">
                        <input class="form-check-input pitfall-chk flex-shrink-0" type="checkbox"
                               id="pf-${e.pitfall_id}" value="${e.pitfall_id}"
                               onchange="PitfallsModule._onCheckChange()">
                        <label class="form-check-label small mb-0" for="pf-${e.pitfall_id}">
                            <span class="badge ${meta.badge} me-1" style="font-size:.65em">${e.pitfall_id}</span>${e.title}${speedTag}${infoIcon}
                        </label>
                    </div>`;
            }
            html += '</div>';
        }
        html += '</div>';
        body.innerHTML = html;

        // Initialise Bootstrap tooltips for all info icons
        body.querySelectorAll('[data-bs-toggle="tooltip"]').forEach(el => {
            new bootstrap.Tooltip(el, { trigger: 'hover focus', html: false });
        });
    }

    function _onCheckChange() {
        _selectedPatterns.clear();
        document.querySelectorAll('.pitfall-chk:checked').forEach(cb => {
            _selectedPatterns.add(cb.value);
        });
        _updateSelectedCount();
    }

    function _updateSelectedCount() {
        const el = document.getElementById('pitfallsSelectedCount');
        if (el) el.textContent = `${_selectedPatterns.size} selected`;
    }

    function _setCheckboxes(ids) {
        document.querySelectorAll('.pitfall-chk').forEach(cb => {
            cb.checked = ids.includes(cb.value);
        });
        _onCheckChange();
    }

    function selectAll() {
        const all = _taxonomy.map(e => e.pitfall_id);
        _setCheckboxes(all);
    }

    function selectFast() {
        _setCheckboxes(FAST_PATTERNS);
    }

    // ── Run analysis ──────────────────────────────────────────────────────────

    function run() {
        if (_selectedPatterns.size === 0) {
            showNotification('Please select at least one pitfall pattern to run the analysis.', 'warning');
            return;
        }

        _showProgress(0, 'Starting analysis…');
        _hideResults();

        const patterns = Array.from(_selectedPatterns);

        fetch('/ontology/pitfalls/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ patterns }),
        })
            .then(r => r.json())
            .then(data => {
                if (!data.success) {
                    _showError(data.error || 'Failed to start analysis.');
                    return;
                }
                _pollResults(data.task_id);
            })
            .catch(err => {
                _showError('Network error: ' + err.message);
            });
    }

    // ── Polling ───────────────────────────────────────────────────────────────

    function _pollResults(taskId) {
        if (_pollTimer) clearTimeout(_pollTimer);

        function poll() {
            fetch(`/ontology/pitfalls/results/${taskId}`)
                .then(r => r.json())
                .then(data => {
                    const status = data.status;
                    _showProgress(data.progress || 0, data.message || '');

                    if (status === 'completed') {
                        _hideProgress();
                        _renderResults(data.result);
                    } else if (status === 'failed') {
                        _hideProgress();
                        _showError(data.error || 'Analysis failed.');
                    } else {
                        _pollTimer = setTimeout(poll, 1500);
                    }
                })
                .catch(() => {
                    _pollTimer = setTimeout(poll, 3000);
                });
        }

        poll();
    }

    // ── Render results ────────────────────────────────────────────────────────

    function _renderResults(result) {
        if (!result) { _showError('No result data.'); return; }

        const grouped = result.grouped_results || {};
        const meta = result.metadata || {};

        // Total issue count across all pitfalls
        let totalIssues = 0;
        for (const pitfallResult of Object.values(result.results || {})) {
            const count = pitfallResult.count || pitfallResult.multi_domain_count || 0;
            totalIssues += typeof count === 'number' ? count : 0;
        }

        // Summary banner
        const bannerEl = document.getElementById('pitfallsSummaryBanner');
        const iconEl = document.getElementById('pitfallsSummaryIcon');
        const titleEl = document.getElementById('pitfallsSummaryTitle');
        const detailEl = document.getElementById('pitfallsSummaryDetail');

        if (totalIssues === 0) {
            bannerEl.className = 'alert alert-success d-flex align-items-center gap-3 mb-3';
            iconEl.className = 'bi bi-check-circle-fill fs-4';
            titleEl.textContent = 'No pitfalls detected';
        } else {
            bannerEl.className = 'alert alert-warning d-flex align-items-center gap-3 mb-3';
            iconEl.className = 'bi bi-exclamation-triangle-fill fs-4';
            titleEl.textContent = `${totalIssues} issue${totalIssues !== 1 ? 's' : ''} found`;
        }
        detailEl.textContent =
            `${meta.classes || 0} classes · ${meta.object_properties || 0} object properties · ` +
            `${meta.datatype_properties || 0} datatype properties · ` +
            `${result.selected_pitfalls.length} patterns checked`;

        // Accordion
        const accordion = document.getElementById('pitfallsAccordion');
        accordion.innerHTML = '';

        for (const [category, pitfalls] of Object.entries(grouped)) {
            const catMeta = CATEGORY_META[category] || { icon: 'bi-circle', badge: 'bg-secondary' };
            const catId = 'pf-cat-' + category.replace(/[^a-z0-9]/gi, '-').toLowerCase();

            let catTotal = 0;
            for (const { result: r } of Object.values(pitfalls)) {
                catTotal += r.count || r.multi_domain_count || 0;
            }

            const item = document.createElement('div');
            item.className = 'accordion-item';
            item.innerHTML = `
                <h2 class="accordion-header">
                    <button class="accordion-button ${catTotal === 0 ? 'collapsed' : ''}" type="button"
                            data-bs-toggle="collapse" data-bs-target="#${catId}">
                        <i class="bi ${catMeta.icon} me-2"></i>
                        ${category}
                        <span class="badge ${catMeta.badge} ms-2">${catTotal}</span>
                    </button>
                </h2>
                <div id="${catId}" class="accordion-collapse collapse ${catTotal > 0 ? 'show' : ''}">
                    <div class="accordion-body p-2" id="${catId}-body"></div>
                </div>`;
            accordion.appendChild(item);

            const body = document.getElementById(`${catId}-body`);
            for (const [pitfallId, { title, result: r }] of Object.entries(pitfalls)) {
                body.appendChild(_buildPitfallCard(pitfallId, title, r, catMeta));
            }
        }

        _showResults(totalIssues);
    }

    function _buildPitfallCard(pitfallId, title, result, catMeta) {
        const count = result.count ?? result.multi_domain_count ?? 0;
        const items = result.items || result.multi_domain_items || [];
        const hasError = !!result.error;

        const card = document.createElement('div');
        card.className = 'card mb-2';

        let bodyHtml = '';
        if (hasError) {
            bodyHtml = `<div class="alert alert-warning small py-1 mb-0"><i class="bi bi-exclamation-circle me-1"></i>${result.error}</div>`;
        } else if (count === 0) {
            bodyHtml = `<span class="text-muted small"><i class="bi bi-check-circle text-success me-1"></i>No issues found</span>`;
        } else {
            let listHtml = '<ul class="list-unstyled mb-0 small">';
            const visible = items.slice(0, 10);
            for (const item of visible) {
                const label = item.class_label || item.child_label || item.property_label ||
                              item.p1_label || item.class_1_label || item.short_label || JSON.stringify(item);
                const extra = item.parent_label ? ` → <span class="text-muted">${item.parent_label}</span>` :
                              item.class_2_label ? ` ≈ <span class="text-muted">${item.class_2_label}</span>` : '';
                listHtml += `<li class="py-1 border-bottom"><i class="bi bi-dot me-1 text-warning"></i>${label}${extra}</li>`;
            }
            if (items.length > 10) {
                listHtml += `<li class="text-muted small pt-1">… and ${items.length - 10} more</li>`;
            }
            listHtml += '</ul>';
            bodyHtml = listHtml;
        }

        card.innerHTML = `
            <div class="card-header py-1 px-3 d-flex justify-content-between align-items-center"
                 style="cursor:pointer" onclick="this.nextElementSibling.classList.toggle('d-none')">
                <span class="small fw-semibold">
                    <span class="badge ${catMeta.badge} me-1" style="font-size:.65em">${pitfallId}</span>
                    ${title}
                </span>
                <span class="badge ${count > 0 ? 'bg-warning text-dark' : 'bg-success'}">${count}</span>
            </div>
            <div class="card-body py-2 px-3 ${count === 0 && !hasError ? 'd-none' : ''}">
                ${bodyHtml}
            </div>`;

        return card;
    }

    // ── UI helpers ────────────────────────────────────────────────────────────

    function _showProgress(pct, msg) {
        const el = document.getElementById('pitfallsProgress');
        const bar = document.getElementById('pitfallsProgressBar');
        const msgEl = document.getElementById('pitfallsProgressMsg');
        const runBtn = document.getElementById('pitfallsRunBtn');

        if (el) el.classList.remove('d-none');
        if (bar) bar.style.width = (pct || 0) + '%';
        if (msgEl) msgEl.textContent = msg || '';
        if (runBtn) runBtn.disabled = true;
    }

    function _hideProgress() {
        const el = document.getElementById('pitfallsProgress');
        const runBtn = document.getElementById('pitfallsRunBtn');
        if (el) el.classList.add('d-none');
        if (runBtn) runBtn.disabled = false;
    }

    function _showResults(totalIssues) {
        const el = document.getElementById('pitfallsResults');
        const emptyEl = document.getElementById('pitfallsEmpty');
        const badge = document.getElementById('pitfallsResultsBadge');
        if (el) el.classList.remove('d-none');
        if (emptyEl) emptyEl.classList.add('d-none');
        if (badge) {
            badge.textContent = totalIssues;
            badge.className = `badge ms-1 small ${totalIssues > 0 ? 'bg-warning text-dark' : 'bg-success'}`;
        }
        // Switch to Results tab automatically
        const tab = document.getElementById('pitfalls-results-tab');
        if (tab) bootstrap.Tab.getOrCreateInstance(tab).show();
    }

    function _hideResults() {
        const el = document.getElementById('pitfallsResults');
        const badge = document.getElementById('pitfallsResultsBadge');
        if (el) el.classList.add('d-none');
        if (badge) badge.classList.add('d-none');
    }

    function _showError(msg) {
        const emptyEl = document.getElementById('pitfallsEmpty');
        if (emptyEl) {
            emptyEl.classList.remove('d-none');
            emptyEl.innerHTML = `
                <i class="bi bi-exclamation-triangle-fill display-6 mb-3 d-block text-danger"></i>
                <p class="fw-semibold text-danger">Analysis error</p>
                <p class="small text-muted">${msg}</p>`;
        }
        _hideProgress();
        // Switch to Results tab to show the error
        const tab = document.getElementById('pitfalls-results-tab');
        if (tab) bootstrap.Tab.getOrCreateInstance(tab).show();
    }

    function _showDepsWarning(msg) {
        const body = document.getElementById('pitfallsPatternBody');
        const runBtn = document.getElementById('pitfallsRunBtn');
        if (body) {
            body.innerHTML = `
                <div class="alert alert-warning small mb-0">
                    <i class="bi bi-exclamation-triangle me-1"></i>
                    <strong>Optional dependencies missing.</strong> ${msg}<br>
                    Install with: <code>pip install .[pitfalls]</code>
                </div>`;
        }
        if (runBtn) runBtn.disabled = true;
    }

    // ── Public API ────────────────────────────────────────────────────────────
    return { init, run, selectAll, selectFast, _onCheckChange };

})();
