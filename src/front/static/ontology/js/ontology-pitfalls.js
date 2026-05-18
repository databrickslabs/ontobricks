/**
 * OntoBricks — ontology-pitfalls.js
 * Ontology Pitfalls Analysis module (D2KLab P1.1–P4.7).
 */
window.PitfallsModule = (function () {

    // Pitfall IDs that do NOT require ML / SentenceTransformer (fast, graph-only)
    const FAST_PATTERNS = ['P1.1', 'P1.2', 'P1.3', 'P2.1', 'P2.2', 'P2.4', 'P2.5', 'P3.1', 'P3.2', 'P3.3', 'P4.7'];

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
        _loadTaxonomy();
    }

    function _loadTaxonomy() {
        fetch('/ontology/pitfalls/taxonomy')
            .then(r => r.json())
            .then(data => {
                if (!data.success) {
                    _showDepsWarning(data.error || 'Optional dependencies not installed.');
                    return;
                }
                _taxonomy = data.taxonomy || [];
                _renderPatternSelector();
                selectAll();
            })
            .catch(err => {
                console.error('PitfallsModule: taxonomy load failed', err);
                _showDepsWarning('Could not load pitfall taxonomy.');
            });
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
            alert('Please select at least one pitfall pattern.');
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
