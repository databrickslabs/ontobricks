/**
 * OntoBricks - ontology-import.js
 * Import section: OWL, RDFS, FIBO, CDISC, IOF, FHIR
 *
 * OWL and RDFS support two modes:
 *   replace — calls /parse-owl or /parse-rdfs (overwrites)
 *   append  — two-phase: analyse conflicts -> resolve -> merge-import
 *
 * All in-page status feedback goes through the ImportStatus helper which
 * drives the persistent #importStatusPanel section at the bottom.
 */

document.addEventListener('DOMContentLoaded', function() {

// ===========================================================================
// ImportStatus — drives the #importStatusPanel step timeline
// ===========================================================================

const ImportStatus = (function() {
    const panel      = document.getElementById('importStatusPanel');
    const stepsEl    = document.getElementById('importStatusSteps');
    const conflictEl = document.getElementById('importConflictArea');
    const finalEl    = document.getElementById('importFinalResult');
    const dismissBtn = document.getElementById('importStatusDismiss');

    if (dismissBtn) {
        dismissBtn.addEventListener('click', function() {
            panel.classList.add('d-none');
        });
    }

    function show() { panel.classList.remove('d-none'); }
    function hide() { panel.classList.add('d-none'); }

    /** Reset panel to empty state for a new import. */
    function reset(filename) {
        stepsEl.innerHTML = '';
        conflictEl.classList.add('d-none');
        finalEl.classList.add('d-none');
        finalEl.innerHTML = '';
        document.getElementById('importNoConflictMsg').classList.add('d-none');
        document.getElementById('importConflictTableWrap').classList.add('d-none');
        document.getElementById('importConflictTbody').innerHTML = '';
        _updateBadges(0, 0, 0);
        if (filename) _addStep('bi-file-earmark-arrow-up text-muted', 'File selected: <strong>' + _esc(filename) + '</strong>', 'done');
        show();
    }

    /**
     * Add a step row to the timeline.
     * state: "running" | "done" | "warn" | "error" | "info"
     * Returns the new <div> element so the caller can update it.
     */
    function addStep(iconClass, message, state) {
        return _addStep(iconClass, message, state);
    }

    /** Update the last step row in place. */
    function updateLastStep(iconClass, message, state) {
        const rows = stepsEl.querySelectorAll('.import-step');
        if (rows.length === 0) return addStep(iconClass, message, state);
        _applyStep(rows[rows.length - 1], iconClass, message, state);
        return rows[rows.length - 1];
    }

    /** Show conflict resolution area with the ConflictReport data. */
    function showConflicts(report, onConfirm, onCancel) {
        const s = report.summary || {};
        _updateBadges(s.new || 0, s.duplicates || 0, s.conflicts || 0);

        if (!report.has_conflicts) {
            document.getElementById('importNoConflictMsg').classList.remove('d-none');
        } else {
            const tableWrap = document.getElementById('importConflictTableWrap');
            tableWrap.classList.remove('d-none');
            _renderConflictRows(document.getElementById('importConflictTbody'), report.conflicts);
        }

        conflictEl.classList.remove('d-none');

        const confirmBtn = document.getElementById('importMergeConfirmBtn');
        const cancelBtn  = document.getElementById('importMergeCancelBtn');

        // Wire up once — remove previous listeners first by replacing nodes
        const newConfirm = confirmBtn.cloneNode(true);
        const newCancel  = cancelBtn.cloneNode(true);
        confirmBtn.replaceWith(newConfirm);
        cancelBtn.replaceWith(newCancel);

        newConfirm.addEventListener('click', function() {
            conflictEl.classList.add('d-none');
            onConfirm(_collectResolutions());
        });
        newCancel.addEventListener('click', function() {
            conflictEl.classList.add('d-none');
            onCancel();
        });
    }

    /** Show the final result banner (success or error). */
    function showResult(success, message) {
        finalEl.innerHTML = success
            ? '<div class="alert alert-success py-2 mb-0 small"><i class="bi bi-check-circle-fill me-2"></i>' + _esc(message) + '</div>'
            : '<div class="alert alert-danger py-2 mb-0 small"><i class="bi bi-x-circle-fill me-2"></i>' + _esc(message) + '</div>';
        finalEl.classList.remove('d-none');
    }

    // ---- private helpers ----

    function _esc(s) {
        return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function _addStep(iconClass, message, state) {
        const div = document.createElement('div');
        div.className = 'import-step d-flex align-items-start gap-2 mb-1 small';
        _applyStep(div, iconClass, message, state);
        stepsEl.appendChild(div);
        return div;
    }

    function _applyStep(div, iconClass, message, state) {
        const spinnerHtml = '<div class="spinner-border spinner-border-sm text-secondary flex-shrink-0" role="status" style="width:1rem;height:1rem;margin-top:2px;"></div>';
        const iconMap = {
            running : spinnerHtml,
            done    : '<i class="bi bi-check-circle-fill text-success flex-shrink-0 mt-1"></i>',
            warn    : '<i class="bi bi-exclamation-triangle-fill text-warning flex-shrink-0 mt-1"></i>',
            error   : '<i class="bi bi-x-circle-fill text-danger flex-shrink-0 mt-1"></i>',
            info    : '<i class="bi bi-info-circle text-secondary flex-shrink-0 mt-1"></i>',
        };
        const ic = (state && iconMap[state]) ? iconMap[state]
                   : '<i class="bi ' + iconClass + ' flex-shrink-0 mt-1"></i>';
        div.innerHTML = ic + '<span>' + message + '</span>';
    }

    function _updateBadges(n, d, c) {
        const bn = document.getElementById('importBadgeNew');
        const bd = document.getElementById('importBadgeDup');
        const bc = document.getElementById('importBadgeConflict');
        if (bn) { bn.textContent = n + ' new';        bn.classList.toggle('d-none', n === 0); }
        if (bd) { bd.textContent = d + ' duplicate';  bd.classList.toggle('d-none', d === 0); }
        if (bc) { bc.textContent = c + ' conflict';   bc.classList.toggle('d-none', c === 0); }
    }

    function _renderConflictRows(tbody, conflicts) {
        tbody.innerHTML = '';
        conflicts.forEach(function(item) {
            const tr = document.createElement('tr');
            const resKey = item.uri || item.name;
            tr.innerHTML =
                '<td class="text-capitalize">' + _esc(item.entity_type) + '</td>' +
                '<td><code>' + _esc(item.name || item.uri || '—') + '</code></td>' +
                '<td><span class="badge bg-warning text-dark">' + _esc(item.conflict_type.replace('_',' ')) + '</span><br>' +
                    '<small class="text-muted">' + _esc(_existingSummary(item)) + '</small></td>' +
                '<td></td>';
            tr.querySelector('td:last-child').appendChild(_buildResolutionSelect(item.conflict_type, resKey));
            tbody.appendChild(tr);
        });
    }

    function _existingSummary(item) {
        if (!item.existing) return '';
        const e = item.existing;
        const parts = [];
        if (e.parent_uri || e.parent) parts.push('parent: ' + (e.parent_uri || e.parent));
        if (e.type)                    parts.push('type: ' + e.type);
        if (e.range || e.rangeLabel)   parts.push('range: ' + (e.range || e.rangeLabel));
        return parts.length ? parts.join(', ') : (e.name || e.uri || '');
    }

    function _buildResolutionSelect(conflictType, resKey) {
        const sel = document.createElement('select');
        sel.className = 'form-select form-select-sm conflict-resolution-sel';
        sel.dataset.resKey = resKey;
        [
            { value: 'skip',      label: 'Skip (keep existing)' },
            { value: 'overwrite', label: 'Overwrite with incoming' },
        ].concat(conflictType === 'name_conflict' ? [{ value: 'rename:', label: 'Rename (type below)...' }] : [])
         .forEach(function(o) {
            const opt = document.createElement('option');
            opt.value = o.value; opt.text = o.label;
            sel.appendChild(opt);
         });
        sel.addEventListener('change', function() {
            const wrap = sel.closest('td');
            let inp = wrap.querySelector('.rename-input');
            if (sel.value === 'rename:') {
                if (!inp) {
                    inp = document.createElement('input');
                    inp.type = 'text';
                    inp.className = 'form-control form-control-sm mt-1 rename-input';
                    inp.placeholder = 'New name...';
                    wrap.appendChild(inp);
                }
            } else if (inp) { inp.remove(); }
        });
        return sel;
    }

    function _collectResolutions() {
        const res = {};
        document.querySelectorAll('#importConflictTbody .conflict-resolution-sel').forEach(function(sel) {
            const key = sel.dataset.resKey;
            let action = sel.value;
            if (action === 'rename:') {
                const inp = sel.closest('td').querySelector('.rename-input');
                action = 'rename:' + (inp ? inp.value.trim() : '');
            }
            res[key] = action;
        });
        return res;
    }

    return { reset, addStep, updateLastStep, showConflicts, showResult, hide };
})();

// ===========================================================================
// Generic API helpers
// ===========================================================================

async function _postJson(url, body) {
    const resp = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        credentials: 'same-origin',
    });
    return resp.json();
}

function _reloadOntology() {
    if (typeof loadOntologyFromSession === 'function') loadOntologyFromSession();
    if (typeof refreshOntologyStatus   === 'function') refreshOntologyStatus();
}

// ===========================================================================
// OWL Import
// ===========================================================================

document.getElementById('importOwlLocalBtn').addEventListener('click', function() {
    document.getElementById('importOwlFileInput').click();
});

document.getElementById('importOwlFileInput').addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    this.value = '';

    ImportStatus.reset(file.name);
    let content;
    try {
        content = await file.text();
    } catch (err) {
        ImportStatus.updateLastStep('', 'Error reading file: ' + err.message, 'error');
        ImportStatus.showResult(false, 'Could not read ' + file.name);
        return;
    }

    const mode = document.querySelector('input[name="owlImportMode"]:checked')?.value || 'replace';

    if (mode === 'append') {
        await _appendFlow('owl', content, file.name);
    } else {
        await _replaceOwlFlow(content, file.name);
    }
});

async function _replaceOwlFlow(content, filename) {
    ImportStatus.addStep('', 'Parsing and replacing ontology...', 'running');
    try {
        const result = await _postJson('/ontology/parse-owl', { content });
        if (result.success) {
            const s = result.stats || {};
            if (result.shacl) {
                ImportStatus.updateLastStep('', result.message, 'done');
                ImportStatus.showResult(true, result.message + ' — check the Data Quality tab');
            } else {
                ImportStatus.updateLastStep('', 'Replaced: ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' relationships loaded', 'done');
                ImportStatus.showResult(true, 'Replaced ontology from ' + filename + ' — ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' relationships');
                _reloadOntology();
            }
        } else {
            ImportStatus.updateLastStep('', 'Parse failed: ' + (result.message || 'unknown error'), 'error');
            ImportStatus.showResult(false, 'OWL parse failed: ' + (result.message || 'unknown error'));
        }
    } catch (err) {
        ImportStatus.updateLastStep('', 'Request error: ' + err.message, 'error');
        ImportStatus.showResult(false, 'Import error: ' + err.message);
    }
}

// ===========================================================================
// RDFS Import
// ===========================================================================

document.getElementById('importRdfsLocalBtn').addEventListener('click', function() {
    document.getElementById('importRdfsFileInput').click();
});

document.getElementById('importRdfsFileInput').addEventListener('change', async function(e) {
    const file = e.target.files[0];
    if (!file) return;
    this.value = '';

    ImportStatus.reset(file.name);
    let content;
    try {
        content = await file.text();
    } catch (err) {
        ImportStatus.updateLastStep('', 'Error reading file: ' + err.message, 'error');
        ImportStatus.showResult(false, 'Could not read ' + file.name);
        return;
    }

    const mode = document.querySelector('input[name="rdfsImportMode"]:checked')?.value || 'replace';

    if (mode === 'append') {
        await _appendFlow('rdfs', content, file.name);
    } else {
        await _replaceRdfsFlow(content, file.name);
    }
});

async function _replaceRdfsFlow(content, filename) {
    ImportStatus.addStep('', 'Parsing and replacing schema...', 'running');
    try {
        const result = await _postJson('/ontology/parse-rdfs', { content });
        if (result.success) {
            const s = result.stats || {};
            if (result.shacl) {
                // SHACL file — auto-routed to Data Quality
                ImportStatus.updateLastStep('', result.message, 'done');
                ImportStatus.showResult(true, result.message + ' — check the Data Quality tab');
            } else {
                ImportStatus.updateLastStep('', 'Replaced: ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' properties loaded', 'done');
                ImportStatus.showResult(true, 'Replaced ontology from ' + filename + ' — ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' properties');
                _reloadOntology();
            }
        } else {
            ImportStatus.updateLastStep('', 'Parse failed: ' + (result.message || 'unknown error'), 'error');
            ImportStatus.showResult(false, 'RDFS parse failed: ' + (result.message || 'unknown error'));
        }
    } catch (err) {
        ImportStatus.updateLastStep('', 'Request error: ' + err.message, 'error');
        ImportStatus.showResult(false, 'Import error: ' + err.message);
    }
}

// ===========================================================================
// Shared append-mode two-phase flow (OWL and RDFS)
// ===========================================================================

async function _appendFlow(format, content, filename) {
    // Phase 1: analyse
    ImportStatus.addStep('', 'Analysing ' + filename + ' for conflicts...', 'running');
    let report;
    try {
        const data = await _postJson('/ontology/analyze-import', { content, format });
        if (!data.success) {
            ImportStatus.updateLastStep('', 'Analysis failed: ' + (data.message || 'unknown error'), 'error');
            ImportStatus.showResult(false, 'Could not analyse ' + filename);
            return;
        }
        report = data.report;
    } catch (err) {
        ImportStatus.updateLastStep('', 'Analysis request error: ' + err.message, 'error');
        ImportStatus.showResult(false, 'Analysis error: ' + err.message);
        return;
    }

    const s = report.summary || {};
    const hasNew       = (s.new || 0) > 0;
    const hasConflicts = report.has_conflicts;
    const hasDups      = (s.duplicates || 0) > 0;

    // Build a descriptive step message
    const parts = [];
    if (hasNew)       parts.push(s.new + ' new');
    if (hasDups)      parts.push(s.duplicates + ' duplicate');
    if (hasConflicts) parts.push(s.conflicts + ' conflict(s) to resolve');
    const summary = parts.length ? parts.join(', ') : 'nothing new';

    ImportStatus.updateLastStep('', 'Analysis complete — ' + summary, hasConflicts ? 'warn' : 'done');

    if (!hasNew && !hasConflicts) {
        ImportStatus.showResult(true, 'Nothing to import — all ' + (s.duplicates||0) + ' item(s) already exist in the ontology.');
        return;
    }

    // Phase 2: show conflicts / wait for user
    if (hasConflicts || hasNew) {
        if (hasConflicts) {
            ImportStatus.addStep('', 'Resolve the ' + s.conflicts + ' conflict(s) below, then click "Confirm & Append".', 'warn');
        } else {
            ImportStatus.addStep('', (s.new||0) + ' new item(s) ready to append — click "Confirm & Append" to proceed.', 'info');
        }
    }

    ImportStatus.showConflicts(report,
        async function onConfirm(resolutions) {
            // Phase 3: merge
            ImportStatus.addStep('', 'Merging...', 'running');
            try {
                const mergeResult = await _postJson('/ontology/merge-import', { content, format, resolutions });
                if (mergeResult.success) {
                    const ms = mergeResult.stats || {};
                    const detail = (ms.new||0) + ' added, ' + (ms.duplicates_skipped||0) + ' skipped, ' + (ms.conflicts_resolved||0) + ' resolved';
                    ImportStatus.updateLastStep('', 'Merge complete — ' + detail, 'done');
                    ImportStatus.showResult(true, 'Appended from ' + filename + ': ' + detail);
                    _reloadOntology();
                } else {
                    ImportStatus.updateLastStep('', 'Merge failed: ' + (mergeResult.message || 'unknown error'), 'error');
                    ImportStatus.showResult(false, 'Merge failed: ' + (mergeResult.message || 'unknown error'));
                }
            } catch (err) {
                ImportStatus.updateLastStep('', 'Merge request error: ' + err.message, 'error');
                ImportStatus.showResult(false, 'Merge error: ' + err.message);
            }
        },
        function onCancel() {
            ImportStatus.addStep('', 'Import cancelled.', 'info');
        }
    );
}

// ===========================================================================
// FIBO Import
// ===========================================================================

document.getElementById('importFiboBtn').addEventListener('click', async function() {
    const domains = ['FND'];
    document.querySelectorAll('.fibo-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) domains.push(cb.value);
    });

    if (domains.length === 1) {
        const proceed = await showConfirmDialog({
            title: 'Import Foundations only?',
            message: 'Only <strong>Foundations (FND)</strong> is selected. This imports core concepts only.<br><br>Select additional domains (BE, FBC, etc.) for a richer ontology.',
            confirmText: 'Proceed with FND only',
            cancelText: 'Cancel',
            confirmClass: 'btn-primary',
            icon: 'info-circle'
        });
        if (!proceed) return;
    }

    const btn = document.getElementById('importFiboBtn');
    const progress = document.getElementById('fiboImportProgress');
    const statusEl = document.getElementById('fiboImportStatus');

    ImportStatus.reset('FIBO — ' + domains.join(', '));
    ImportStatus.addStep('', 'Fetching FIBO modules from spec.edmcouncil.org...', 'running');

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching FIBO modules from spec.edmcouncil.org... This may take 15-30 seconds.';

    try {
        const result = await _postJson('/ontology/import-fibo', { domains });
        if (result.success) {
            const s = result.stats || {};
            const msg = 'FIBO imported: ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' relationships' +
                        (s.modules_failed > 0 ? ' (' + s.modules_failed + ' modules unavailable)' : '');
            ImportStatus.updateLastStep('', msg, 'done');
            ImportStatus.showResult(true, msg);
            statusEl.textContent = 'Import complete!';
            if (result.failed && result.failed.length > 0) {
                ImportStatus.addStep('', result.failed.length + ' module(s) could not be fetched', 'warn');
            }
            _reloadOntology();
        } else {
            ImportStatus.updateLastStep('', 'FIBO import failed: ' + (result.message || ''), 'error');
            ImportStatus.showResult(false, 'FIBO import failed: ' + (result.message || 'unknown error'));
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        ImportStatus.updateLastStep('', 'FIBO import error: ' + error.message, 'error');
        ImportStatus.showResult(false, 'FIBO import error: ' + error.message);
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Domains';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

// ===========================================================================
// CDISC Import
// ===========================================================================

document.getElementById('importCdiscBtn').addEventListener('click', async function() {
    const domains = ['SCHEMAS'];
    document.querySelectorAll('.cdisc-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) domains.push(cb.value);
    });

    if (domains.length === 1) {
        const proceed = await showConfirmDialog({
            title: 'Import Schemas only?',
            message: 'Only <strong>Schemas</strong> is selected. This imports the meta-model only.<br><br>Select additional standards (SDTM, CDASH, etc.) for a richer ontology.',
            confirmText: 'Proceed with Schemas only',
            cancelText: 'Cancel',
            confirmClass: 'btn-primary',
            icon: 'info-circle'
        });
        if (!proceed) return;
    }

    const btn = document.getElementById('importCdiscBtn');
    const progress = document.getElementById('cdiscImportProgress');
    const statusEl = document.getElementById('cdiscImportStatus');

    ImportStatus.reset('CDISC — ' + domains.join(', '));
    ImportStatus.addStep('', 'Fetching CDISC modules from GitHub...', 'running');

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching CDISC modules from GitHub... This may take 15-30 seconds.';

    try {
        const result = await _postJson('/ontology/import-cdisc', { domains });
        if (result.success) {
            ImportStatus.updateLastStep('', result.message || 'CDISC import complete', 'done');
            ImportStatus.showResult(true, result.message || 'CDISC import complete');
            statusEl.textContent = 'Import complete!';
            if (result.failed && result.failed.length > 0) {
                ImportStatus.addStep('', result.failed.length + ' module(s) could not be fetched', 'warn');
            }
            _reloadOntology();
        } else {
            ImportStatus.updateLastStep('', 'CDISC import failed: ' + (result.message || ''), 'error');
            ImportStatus.showResult(false, 'CDISC import failed: ' + (result.message || 'unknown error'));
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        ImportStatus.updateLastStep('', 'CDISC import error: ' + error.message, 'error');
        ImportStatus.showResult(false, 'CDISC import error: ' + error.message);
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Standards';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

// ===========================================================================
// FHIR Import
// ===========================================================================

document.getElementById('importFhirBtn').addEventListener('click', async function() {
    const domains = ['FOUNDATION'];
    document.querySelectorAll('.fhir-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) domains.push(cb.value);
    });

    const version = (document.getElementById('fhirVersionSelect')?.value || 'R5').toUpperCase();

    if (domains.length === 1) {
        const proceed = await showConfirmDialog({
            title: 'Import Foundation only?',
            message: 'Only <strong>Foundation</strong> is selected. This imports base FHIR resource types only.<br><br>Select additional domains (Clinical, Diagnostics, etc.) for a richer ontology.',
            confirmText: 'Proceed with Foundation only',
            cancelText: 'Cancel',
            confirmClass: 'btn-primary',
            icon: 'info-circle'
        });
        if (!proceed) return;
    }

    const btn = document.getElementById('importFhirBtn');
    const progress = document.getElementById('fhirImportProgress');
    const statusEl = document.getElementById('fhirImportStatus');

    ImportStatus.reset('FHIR ' + version + ' — ' + domains.join(', '));
    ImportStatus.addStep('', 'Fetching FHIR ' + version + ' ontology from hl7.org...', 'running');

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching FHIR ' + version + ' ontology from hl7.org... This may take 20-40 seconds.';

    try {
        const result = await _postJson('/ontology/import-fhir', { domains, version });
        if (result.success) {
            const s = result.stats || {};
            const msg = 'FHIR ' + version + ' imported: ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' properties';
            ImportStatus.updateLastStep('', msg, 'done');
            ImportStatus.showResult(true, msg);
            statusEl.textContent = 'Import complete!';
            _reloadOntology();
        } else {
            ImportStatus.updateLastStep('', 'FHIR import failed: ' + (result.message || ''), 'error');
            ImportStatus.showResult(false, 'FHIR import failed: ' + (result.message || 'unknown error'));
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        ImportStatus.updateLastStep('', 'FHIR import error: ' + error.message, 'error');
        ImportStatus.showResult(false, 'FHIR import error: ' + error.message);
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Domains';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

// ===========================================================================
// IOF Import
// ===========================================================================

document.getElementById('importIofBtn').addEventListener('click', async function() {
    const domains = ['CORE'];
    document.querySelectorAll('.iof-domain-cb:checked').forEach(function(cb) {
        if (!domains.includes(cb.value)) domains.push(cb.value);
    });

    if (domains.length === 1) {
        const proceed = await showConfirmDialog({
            title: 'Import Core only?',
            message: 'Only <strong>Core</strong> is selected. This imports foundational manufacturing concepts only.<br><br>Select additional domains (Maintenance, Supply Chain) for a richer ontology.',
            confirmText: 'Proceed with Core only',
            cancelText: 'Cancel',
            confirmClass: 'btn-primary',
            icon: 'info-circle'
        });
        if (!proceed) return;
    }

    const btn = document.getElementById('importIofBtn');
    const progress = document.getElementById('iofImportProgress');
    const statusEl = document.getElementById('iofImportStatus');

    ImportStatus.reset('IOF — ' + domains.join(', '));
    ImportStatus.addStep('', 'Fetching IOF modules from GitHub...', 'running');

    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Importing...';
    progress.classList.remove('d-none');
    statusEl.textContent = 'Fetching IOF modules from GitHub... This may take 15-30 seconds.';

    try {
        const result = await _postJson('/ontology/import-iof', { domains });
        if (result.success) {
            const s = result.stats || {};
            const msg = 'IOF imported: ' + (s.classes||0) + ' classes, ' + (s.properties||0) + ' relationships' +
                        (s.modules_failed > 0 ? ' (' + s.modules_failed + ' modules unavailable)' : '');
            ImportStatus.updateLastStep('', msg, 'done');
            ImportStatus.showResult(true, msg);
            statusEl.textContent = 'Import complete!';
            if (result.failed && result.failed.length > 0) {
                ImportStatus.addStep('', result.failed.length + ' module(s) could not be fetched', 'warn');
            }
            _reloadOntology();
        } else {
            ImportStatus.updateLastStep('', 'IOF import failed: ' + (result.message || ''), 'error');
            ImportStatus.showResult(false, 'IOF import failed: ' + (result.message || 'unknown error'));
            statusEl.textContent = 'Import failed.';
        }
    } catch (error) {
        ImportStatus.updateLastStep('', 'IOF import error: ' + error.message, 'error');
        ImportStatus.showResult(false, 'IOF import error: ' + error.message);
        statusEl.textContent = 'Import error.';
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download"></i> Import Selected Domains';
        setTimeout(function() { progress.classList.add('d-none'); }, 3000);
    }
});

}); // DOMContentLoaded
