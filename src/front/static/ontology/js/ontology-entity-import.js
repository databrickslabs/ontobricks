/**
 * OntoBricks - ontology-entity-import.js
 * Studio "Import entity": copy entities (and the relationships between them)
 * from another registry domain. The backend owns the import rules; the
 * helpers below mirror them for the live preview only.
 */
(function () {
    'use strict';

    const NAME_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

    function isValidName(name) {
        return NAME_RE.test(name || '');
    }

    function entityByName(catalog, name) {
        return (catalog.entities || []).find(e => e.name === name) || null;
    }

    function relatedEntities(catalog, name) {
        const out = [];
        (catalog.relationships || []).forEach(r => {
            if (r.domain === name && r.range !== name) {
                out.push({ name: r.range, relation: r.name, direction: 'out' });
            } else if (r.range === name && r.domain !== name) {
                out.push({ name: r.domain, relation: r.name, direction: 'in' });
            }
        });
        const self = entityByName(catalog, name);
        if (self && self.parent && entityByName(catalog, self.parent)) {
            out.push({ name: self.parent, relation: 'subClassOf', direction: 'parent' });
        }
        (catalog.entities || []).forEach(e => {
            if (e.parent === name) out.push({ name: e.name, relation: 'subClassOf', direction: 'child' });
        });
        return out;
    }

    function resolver(catalog, selectedNames, renames) {
        const selected = new Set(selectedNames);
        return function (name) {
            if (renames[name] && selected.has(name)) return { name: renames[name], imported: true };
            const entity = entityByName(catalog, name);
            if (entity && entity.exists) return { name, imported: false };
            if (selected.has(name)) return { name, imported: true };
            return { name: null, imported: false };
        };
    }

    function previewImport(catalog, selectedNames, renames) {
        const resolve = resolver(catalog, selectedNames, renames || {});
        const entities = [...new Set(selectedNames)].filter(n => resolve(n).imported).length;
        const relationships = (catalog.relationships || []).filter(r => {
            const d = resolve(r.domain);
            const g = resolve(r.range);
            return d.name && g.name && (d.imported || g.imported);
        }).length;
        return { entities, relationships };
    }

    window.EntityImport = { isValidName, relatedEntities, previewImport };

    // ------------------------------------------------------------------
    // Modal wiring
    // ------------------------------------------------------------------

    const ARROWS = { out: '→', in: '←', parent: '↑', child: '↓' };
    const EMPTY_RELATED = '<p class="text-muted small mb-0">Select an entity to see its related entities.</p>';
    const state = { domain: '', catalog: null, selected: new Set(), renames: {}, mode: {} };
    const $ = id => document.getElementById(id);

    function counted(n, singular, plural) {
        return `${n} ${n === 1 ? singular : plural}`;
    }

    function resetSelection(domain) {
        Object.assign(state, { domain, catalog: null, selected: new Set(), renames: {}, mode: {} });
    }

    function showError(message) {
        const el = $('entityImportError');
        el.textContent = message || '';
        el.classList.toggle('d-none', !message);
    }

    async function getJson(url, options) {
        const resp = await fetch(url, Object.assign({ credentials: 'same-origin' }, options || {}));
        const body = await resp.json().catch(() => ({}));
        if (!resp.ok || body.success === false) {
            const reason = [body.message, body.detail].filter(Boolean).join(': ');
            throw new Error(reason || `Request failed (${resp.status})`);
        }
        return body;
    }

    async function openModal() {
        resetSelection('');
        showError('');
        $('entityImportBody').classList.add('d-none');
        $('entityImportSearch').value = '';
        const select = $('entityImportDomain');
        select.innerHTML = '<option value="">Select a domain…</option>';
        render();
        bootstrap.Modal.getOrCreateInstance($('entityImportModal')).show();
        try {
            const body = await getJson('/ontology/bridges/domains');
            const domains = body.domains || [];
            if (!domains.length) {
                showError('No other domain in the registry.');
                return;
            }
            domains.forEach(d => select.add(new Option(d.name, d.name)));
        } catch (err) {
            showError(err.message);
        }
    }

    async function loadCatalog(domain) {
        resetSelection(domain);
        showError('');
        $('entityImportSearch').value = '';
        $('entityImportBody').classList.add('d-none');
        render();
        if (!domain) return;
        $('entityImportList').innerHTML =
            '<div class="text-muted small"><span class="spinner-border spinner-border-sm me-2"></span>Loading…</div>';
        $('entityImportRelated').innerHTML = EMPTY_RELATED;
        $('entityImportBody').classList.remove('d-none');
        try {
            const catalog = await getJson(`/ontology/entity-import/domains/${encodeURIComponent(domain)}/catalog`);
            if (state.domain !== domain) return;
            state.catalog = catalog;
        } catch (err) {
            if (state.domain !== domain) return;
            $('entityImportBody').classList.add('d-none');
            showError(err.message);
        }
        render();
    }

    function toggle(name, checked) {
        if (checked) {
            state.selected.add(name);
        } else {
            state.selected.delete(name);
            delete state.renames[name];
            delete state.mode[name];
        }
        render();
    }

    function conflictControls(e) {
        const name = escapeHtml(e.name);
        const renaming = state.mode[e.name] === 'rename';
        const value = state.renames[e.name] || '';
        const input = renaming
            ? `<input type="text" class="form-control form-control-sm mt-2 ${isValidName(value) ? '' : 'is-invalid'}"
                      data-rename="${name}" value="${escapeHtml(value)}" aria-label="New name for ${name}">`
            : '';
        return `
            <div class="btn-group btn-group-sm mt-2" role="group" aria-label="Conflict for ${name}">
                <button type="button" class="btn btn-outline-secondary ${renaming ? '' : 'active'}" data-mode="keep" data-name="${name}">Keep existing</button>
                <button type="button" class="btn btn-outline-secondary ${renaming ? 'active' : ''}" data-mode="rename" data-name="${name}">Rename</button>
            </div>${input}`;
    }

    function entityRow(e) {
        const name = escapeHtml(e.name);
        const checked = state.selected.has(e.name);
        const exists = e.exists ? '<span class="badge text-bg-warning ms-2">Exists</span>' : '';
        return `
            <div class="list-group-item">
                <div class="form-check mb-0">
                    <input class="form-check-input" type="checkbox" id="ei-${name}" data-entity="${name}" ${checked ? 'checked' : ''}>
                    <label class="form-check-label" for="ei-${name}">
                        ${escapeHtml(e.emoji || '📦')} ${escapeHtml(e.label || e.name)}
                        <span class="text-muted small">· ${e.attributes} attr.</span>${exists}
                    </label>
                </div>${e.exists && checked ? conflictControls(e) : ''}
            </div>`;
    }

    function relatedBlock(name) {
        const rows = relatedEntities(state.catalog, name).map(r => {
            const id = escapeHtml(`eir-${name}-${r.direction}-${r.relation}-${r.name}`);
            return `
                <div class="form-check">
                    <input class="form-check-input" type="checkbox" id="${id}" data-entity="${escapeHtml(r.name)}"
                           ${state.selected.has(r.name) ? 'checked' : ''}>
                    <label class="form-check-label small" for="${id}">
                        ${ARROWS[r.direction]} ${escapeHtml(r.relation)} <strong>${escapeHtml(r.name)}</strong>
                    </label>
                </div>`;
        }).join('');
        return `
            <div class="mb-3">
                <div class="fw-semibold small mb-1">${escapeHtml(name)}</div>
                ${rows || '<p class="text-muted small mb-0">No related entities.</p>'}
            </div>`;
    }

    function effectiveRenames() {
        const out = {};
        Object.keys(state.mode).forEach(n => {
            if (state.mode[n] === 'rename') out[n] = (state.renames[n] || '').trim();
        });
        return out;
    }

    function renderSummary() {
        const confirm = $('entityImportConfirm');
        const summary = $('entityImportSummary');
        if (!state.catalog) {
            confirm.disabled = true;
            summary.textContent = '';
            return;
        }
        const renames = effectiveRenames();
        const preview = previewImport(state.catalog, [...state.selected], renames);
        summary.textContent = `${counted(preview.entities, 'entity', 'entities')} and `
            + `${counted(preview.relationships, 'relationship', 'relationships')} will be imported`;
        confirm.disabled = preview.entities === 0 || !Object.values(renames).every(isValidName);
    }

    function render() {
        if (state.catalog) {
            const filter = ($('entityImportSearch').value || '').toLowerCase();
            const rows = state.catalog.entities
                .filter(e => !filter || (e.label || e.name).toLowerCase().includes(filter))
                .map(entityRow).join('');
            $('entityImportList').innerHTML = rows || '<p class="text-muted small mb-0">No entities.</p>';
            const selected = [...state.selected];
            $('entityImportRelated').innerHTML = selected.length
                ? selected.map(relatedBlock).join('')
                : EMPTY_RELATED;
        }
        renderSummary();
    }

    async function runImport() {
        $('entityImportConfirm').disabled = true;
        showError('');
        try {
            const result = await getJson('/ontology/entity-import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    domain: state.domain,
                    entities: [...state.selected],
                    renames: effectiveRenames(),
                }),
            });
            await loadOntologyFromSession();
            initOntologyMap();
            bootstrap.Modal.getOrCreateInstance($('entityImportModal')).hide();
            showNotification(
                `Imported ${counted(result.imported.length, 'entity', 'entities')} and `
                    + `${counted(result.relationships.length, 'relationship', 'relationships')}`,
                'success'
            );
            const skipped = result.skipped.map(s => s.name)
                .concat(result.skipped_relationships.map(s => s.name));
            if (skipped.length) {
                showNotification(`Kept existing / skipped: ${skipped.join(', ')}`, 'warning');
            }
        } catch (err) {
            showError(err.message);
            renderSummary();
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        const button = $('mapImportEntity');
        const modal = $('entityImportModal');
        if (!button || !modal) return;
        button.addEventListener('click', openModal);
        $('entityImportDomain').addEventListener('change', e => loadCatalog(e.target.value));
        $('entityImportSearch').addEventListener('input', render);
        $('entityImportConfirm').addEventListener('click', runImport);
        modal.addEventListener('change', e => {
            const name = e.target.dataset && e.target.dataset.entity;
            if (name) toggle(name, e.target.checked);
        });
        modal.addEventListener('click', e => {
            const btn = e.target.closest('[data-mode]');
            if (!btn) return;
            const name = btn.dataset.name;
            state.mode[name] = btn.dataset.mode;
            if (btn.dataset.mode === 'rename' && !state.renames[name]) {
                state.renames[name] = `${name}_${state.domain.replace(/[^A-Za-z0-9_]/g, '_')}`;
            }
            render();
        });
        // Typing a new name only refreshes the summary so the field keeps focus.
        $('entityImportList').addEventListener('input', e => {
            const name = e.target.dataset && e.target.dataset.rename;
            if (!name) return;
            state.renames[name] = e.target.value;
            e.target.classList.toggle('is-invalid', !isValidName(e.target.value.trim()));
            renderSummary();
        });
    });
})();
