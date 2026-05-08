/**
 * OntoBricks - schedule.js
 * Unified Schedule tab: CRUD for both per-domain Digital Twin builds
 * and per-(domain, rule) cohort materialisations. The type pill in the
 * table and the "Type" radio group in the modal route each action to
 * the right backend endpoint:
 *   - build  → /settings/schedules
 *   - cohort → /settings/cohort-schedules
 */

document.addEventListener('DOMContentLoaded', function () {

    function formatFrequency(minutes) {
        if (minutes >= 1440 && minutes % 1440 === 0) {
            var d = minutes / 1440;
            return 'Every ' + d + (d === 1 ? ' day' : ' days');
        }
        if (minutes >= 60 && minutes % 60 === 0) {
            var h = minutes / 60;
            return 'Every ' + h + (h === 1 ? ' hour' : ' hours');
        }
        return 'Every ' + minutes + ' min';
    }

    function minutesToUnitValue(minutes) {
        if (minutes >= 1440 && minutes % 1440 === 0) return { value: minutes / 1440, unit: 'days' };
        if (minutes >= 60 && minutes % 60 === 0) return { value: minutes / 60, unit: 'hours' };
        return { value: minutes, unit: 'minutes' };
    }

    function unitValueToMinutes() {
        var val = parseInt(document.getElementById('scheduleIntervalValue').value, 10) || 1;
        var unit = document.getElementById('scheduleIntervalUnit').value;
        if (unit === 'days') return val * 1440;
        if (unit === 'hours') return val * 60;
        return val;
    }

    // editing: null when adding, else { kind, domain_name, rule_id? }
    let editing = null;

    // Cache of rules currently in the rule dropdown, indexed by id.
    // Lets us prefill the output checkboxes / target hint from the
    // selected rule without an extra API roundtrip.
    let rulesById = {};

    loadSchedules();

    document.getElementById('btnRefreshSchedules')?.addEventListener('click', () => loadSchedules());

    document.getElementById('btnAddSchedule')?.addEventListener('click', () => {
        editing = null;
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Add Schedule';
        setType('build');
        setTypeDisabled(false);

        const domSel = document.getElementById('scheduleDomain');
        domSel.disabled = false;
        const ruleSel = document.getElementById('scheduleCohortRule');
        ruleSel.disabled = true;
        ruleSel.innerHTML = '<option value="">Select a domain first</option>';
        rulesById = {};
        document.getElementById('scheduleIntervalValue').value = '1';
        document.getElementById('scheduleIntervalUnit').value = 'hours';
        document.getElementById('scheduleBuildMode').value = 'full';
        document.getElementById('scheduleEnabled').checked = true;
        resetOutputs();
        resetVersionSelect();
        applyTypeVisibility();
        loadDomainsForModal();
        new bootstrap.Modal(document.getElementById('scheduleModal')).show();
    });

    document.getElementById('scheduleCohortRule')?.addEventListener('change', function () {
        prefillOutputsFromRule(this.value);
    });

    document.querySelectorAll('input[name="scheduleType"]').forEach(input => {
        input.addEventListener('change', () => {
            applyTypeVisibility();
            const domain = document.getElementById('scheduleDomain').value;
            if (currentType() === 'cohort' && domain) {
                loadRulesForDomain(domain);
            }
        });
    });

    document.getElementById('scheduleDomain')?.addEventListener('change', function () {
        var domainName = this.value;
        if (domainName) {
            loadVersionsForDomain(domainName);
            if (currentType() === 'cohort') {
                loadRulesForDomain(domainName);
            }
        } else {
            resetVersionSelect();
            const ruleSel = document.getElementById('scheduleCohortRule');
            ruleSel.disabled = true;
            ruleSel.innerHTML = '<option value="">Select a domain first</option>';
        }
    });

    document.getElementById('btnApplySchedule')?.addEventListener('click', saveSchedule);

    function currentType() {
        const checked = document.querySelector('input[name="scheduleType"]:checked');
        return (checked && checked.value) || 'build';
    }

    function setType(kind) {
        const id = kind === 'cohort' ? 'scheduleTypeCohort' : 'scheduleTypeBuild';
        const input = document.getElementById(id);
        if (input) input.checked = true;
    }

    function setTypeDisabled(disabled) {
        document.querySelectorAll('input[name="scheduleType"]').forEach(input => {
            input.disabled = !!disabled;
        });
    }

    function applyTypeVisibility() {
        const isCohort = currentType() === 'cohort';
        document.querySelectorAll('.schedule-cohort-only').forEach(el => {
            el.style.display = isCohort ? '' : 'none';
        });
        document.querySelectorAll('.schedule-build-only').forEach(el => {
            el.style.display = isCohort ? 'none' : '';
        });
    }

    function resetOutputs() {
        const g = document.getElementById('scheduleOutputGraph');
        const u = document.getElementById('scheduleOutputUc');
        if (g) g.checked = true;
        if (u) {
            u.checked = true;
            u.disabled = false;
        }
        const hint = document.getElementById('scheduleOutputUcHint');
        if (hint) hint.textContent = " — uses the rule's saved target";
    }

    function prefillOutputsFromRule(ruleId) {
        const g = document.getElementById('scheduleOutputGraph');
        const u = document.getElementById('scheduleOutputUc');
        const hint = document.getElementById('scheduleOutputUcHint');
        const rule = rulesById[ruleId];
        if (!rule || !rule.output) {
            resetOutputs();
            return;
        }
        const out = rule.output;
        if (g) g.checked = out.graph !== false;

        const uc = out.uc_table;
        if (uc && uc.table_name) {
            if (u) {
                u.checked = true;
                u.disabled = false;
            }
            if (hint) {
                hint.textContent = ' — target: ' +
                    (uc.catalog || '') + '.' + (uc.schema || '') + '.' + uc.table_name;
            }
        } else {
            if (u) {
                u.checked = false;
                u.disabled = true;
            }
            if (hint) hint.textContent = ' — no UC target configured on this rule';
        }
    }

    async function loadSchedules() {
        const container = document.getElementById('schedulesTableContainer');
        if (!container) return;

        container.innerHTML = '<div class="text-center text-muted small py-3">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading schedules...</div>';

        try {
            const [buildsResp, cohortsResp] = await Promise.all([
                fetch('/settings/schedules', { credentials: 'same-origin' }).then(r => r.json()).catch(() => ({})),
                fetch('/settings/cohort-schedules', { credentials: 'same-origin' }).then(r => r.json()).catch(() => ({})),
            ]);

            const builds = (buildsResp && buildsResp.success && Array.isArray(buildsResp.schedules))
                ? buildsResp.schedules : [];
            const cohorts = (cohortsResp && cohortsResp.success && Array.isArray(cohortsResp.schedules))
                ? cohortsResp.schedules : [];

            const rows = [
                ...builds.map(s => ({ ...s, _kind: 'build' })),
                ...cohorts.map(s => ({ ...s, _kind: 'cohort' })),
            ];

            if (rows.length === 0) {
                if (buildsResp && buildsResp.success === false) {
                    container.innerHTML = '<div class="text-muted small py-3">' +
                        '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                        escapeHtml(buildsResp.message || 'Could not load schedules') + '</div>';
                    return;
                }
                container.innerHTML = '<div class="text-muted small py-4 text-center">' +
                    '<i class="bi bi-clock display-6 d-block mb-2 text-secondary"></i>' +
                    '<div class="mb-1">No schedules yet</div>' +
                    '<small>Click <strong>Add Schedule</strong> to create one.</small></div>';
                return;
            }

            // Sort: builds first, then cohorts; within each block by domain
            rows.sort((a, b) => {
                if (a._kind !== b._kind) return a._kind === 'build' ? -1 : 1;
                return (a.domain_name || '').localeCompare(b.domain_name || '');
            });

            let html = '<div class="table-responsive">' +
                '<table class="table table-sm table-hover align-middle mb-0">' +
                '<thead><tr>' +
                    '<th class="ps-3" style="width:22%;">Domain</th>' +
                    '<th style="width:18%;">Type</th>' +
                    '<th>Details</th>' +
                    '<th style="width:11rem;">Frequency</th>' +
                    '<th class="text-center" style="width:7rem;">Status</th>' +
                    '<th style="width:9rem;">Last Run</th>' +
                    '<th style="width:11rem;">Next Run</th>' +
                    '<th class="text-end pe-3" style="width:8rem;"></th>' +
                '</tr></thead><tbody>';

            rows.forEach(s => {
                const isCohort = s._kind === 'cohort';
                const domainName = s.domain_name || s.project_name || '';
                const ruleId = s.rule_id || '';

                const typeBadge = isCohort
                    ? '<span class="badge bg-info-subtle text-info border"><i class="bi bi-people-fill me-1"></i>Cohort</span>'
                    : '<span class="badge bg-primary-subtle text-primary border"><i class="bi bi-diagram-3 me-1"></i>Build</span>';

                const versionBadge = s.version && s.version !== 'latest'
                    ? '<span class="badge bg-secondary-subtle text-secondary border">v' + escapeHtml(s.version) + '</span>'
                    : '<span class="badge bg-secondary-subtle text-secondary border">Latest</span>';

                let detailsCell;
                if (isCohort) {
                    const outGraph = s.output_graph !== false;
                    const outUc = s.output_uc !== false;
                    let outBadges = '';
                    if (outGraph) {
                        outBadges += ' <span class="badge bg-primary-subtle text-primary border" title="Materialise to graph"><i class="bi bi-diagram-3 me-1"></i>Graph</span>';
                    }
                    if (outUc) {
                        outBadges += ' <span class="badge bg-primary-subtle text-primary border" title="Materialise to UC table"><i class="bi bi-table me-1"></i>UC</span>';
                    }
                    detailsCell =
                        '<span class="text-muted small me-1">Rule:</span>' +
                        '<code class="small">' + escapeHtml(ruleId) + '</code>' +
                        outBadges +
                        '<span class="ms-2">' + versionBadge + '</span>';
                } else {
                    const modeLabel = s.drop_existing
                        ? '<span class="badge bg-warning-subtle text-warning border"><i class="bi bi-arrow-repeat me-1"></i>Full</span>'
                        : '<span class="badge bg-info-subtle text-info border"><i class="bi bi-lightning me-1"></i>Incremental</span>';
                    detailsCell = modeLabel + ' ' + versionBadge;
                }

                const freqLabel = formatFrequency(s.interval_minutes);

                const enabledBadge = s.enabled
                    ? '<span class="badge bg-success-subtle text-success border">Active</span>'
                    : '<span class="badge bg-secondary-subtle text-secondary border">Paused</span>';

                let statusBadge = '';
                if (s.last_status === 'success') {
                    statusBadge = ' <span class="badge bg-success-subtle text-success border" title="' +
                        escapeHtml(s.last_message || '') + '"><i class="bi bi-check-circle"></i></span>';
                } else if (s.last_status === 'error') {
                    statusBadge = ' <span class="badge bg-danger-subtle text-danger border" title="' +
                        escapeHtml(s.last_message || '') + '"><i class="bi bi-x-circle"></i></span>';
                }

                const lastRun = s.last_run ? formatRelativeTime(s.last_run) : '<span class="text-muted">—</span>';
                const nextRun = s.next_run ? formatAbsoluteTime(s.next_run) : '<span class="text-muted">—</span>';

                const dataAttrs =
                    'data-kind="' + s._kind + '" ' +
                    'data-domain="' + escapeHtml(domainName) + '" ' +
                    'data-rule="' + escapeHtml(ruleId) + '" ';

                html += '<tr>' +
                    '<td class="ps-3 fw-semibold text-nowrap">' +
                        '<i class="bi bi-folder2 me-1 text-primary"></i>' + escapeHtml(domainName) +
                    '</td>' +
                    '<td>' + typeBadge + '</td>' +
                    '<td class="small">' + detailsCell + '</td>' +
                    '<td class="small text-muted text-nowrap">' + freqLabel + '</td>' +
                    '<td class="text-center text-nowrap">' + enabledBadge + statusBadge + '</td>' +
                    '<td class="small text-muted text-nowrap">' + lastRun + '</td>' +
                    '<td class="small text-muted text-nowrap">' + nextRun + '</td>' +
                    '<td class="text-end pe-3 text-nowrap">' +
                        '<div class="btn-group btn-group-sm" role="group">' +
                            '<button type="button" class="btn btn-sm btn-outline-success border-0 schedule-runnow-btn" ' +
                                dataAttrs + 'title="Run now (one-shot, recurring schedule untouched)">' +
                                '<i class="bi bi-play-fill"></i></button>' +
                            '<button type="button" class="btn btn-sm btn-outline-secondary border-0 schedule-history-btn" ' +
                                dataAttrs + 'title="Run history">' +
                                '<i class="bi bi-journal-text"></i></button>' +
                            '<button type="button" class="btn btn-sm btn-outline-secondary border-0 schedule-edit-btn" ' +
                                dataAttrs +
                                'data-interval="' + s.interval_minutes + '" ' +
                                'data-drop="' + (s.drop_existing ? '1' : '0') + '" ' +
                                'data-enabled="' + (s.enabled ? '1' : '0') + '" ' +
                                'data-version="' + escapeHtml(s.version || 'latest') + '" ' +
                                'data-output-graph="' + (s.output_graph === false ? '0' : '1') + '" ' +
                                'data-output-uc="' + (s.output_uc === false ? '0' : '1') + '" ' +
                                'title="Edit">' +
                                '<i class="bi bi-pencil"></i></button>' +
                            '<button type="button" class="btn btn-sm btn-outline-danger border-0 schedule-delete-btn" ' +
                                dataAttrs + 'title="Remove schedule">' +
                                '<i class="bi bi-trash"></i></button>' +
                        '</div>' +
                    '</td>' +
                '</tr>';
            });

            html += '</tbody></table></div>';
            container.innerHTML = html;

            container.querySelectorAll('.schedule-runnow-btn').forEach(btn => {
                btn.addEventListener('click', () => runScheduleNow(btn));
            });
            container.querySelectorAll('.schedule-history-btn').forEach(btn => {
                btn.addEventListener('click', () => openHistoryModal(btn.dataset));
            });
            container.querySelectorAll('.schedule-edit-btn').forEach(btn => {
                btn.addEventListener('click', () => openEditModal(btn));
            });
            container.querySelectorAll('.schedule-delete-btn').forEach(btn => {
                btn.addEventListener('click', () => deleteSchedule(btn.dataset));
            });

        } catch (e) {
            console.error('Error loading schedules:', e);
            container.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading schedules: ' +
                escapeHtml(e.message) + '</div>';
        }
    }

    function openEditModal(btn) {
        const kind = btn.dataset.kind || 'build';
        const domainName = btn.dataset.domain || '';
        const ruleId = btn.dataset.rule || '';

        editing = { kind, domain_name: domainName, rule_id: ruleId };
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Edit Schedule';

        setType(kind);
        setTypeDisabled(true);

        const domSel = document.getElementById('scheduleDomain');
        domSel.innerHTML = '<option value="' + escapeHtml(domainName) + '" selected>' +
            escapeHtml(domainName) + '</option>';
        domSel.disabled = true;

        const ruleSel = document.getElementById('scheduleCohortRule');
        if (kind === 'cohort') {
            ruleSel.innerHTML = '<option value="' + escapeHtml(ruleId) + '" selected>' +
                escapeHtml(ruleId) + '</option>';
            ruleSel.disabled = true;
            // Reload the full list (silently) so we can resolve the rule's
            // output config and refresh the UC target hint, then re-pin the
            // selection.
            loadRulesForDomain(domainName, ruleId).then(() => {
                ruleSel.disabled = true;
                const outGraphEl = document.getElementById('scheduleOutputGraph');
                const outUcEl = document.getElementById('scheduleOutputUc');
                if (outGraphEl) outGraphEl.checked = btn.dataset.outputGraph !== '0';
                if (outUcEl && !outUcEl.disabled) {
                    outUcEl.checked = btn.dataset.outputUc !== '0';
                }
            });
        } else {
            ruleSel.innerHTML = '<option value="">—</option>';
            ruleSel.disabled = true;
            resetOutputs();
        }

        var uv = minutesToUnitValue(parseInt(btn.dataset.interval, 10));
        document.getElementById('scheduleIntervalValue').value = uv.value;
        document.getElementById('scheduleIntervalUnit').value = uv.unit;
        document.getElementById('scheduleBuildMode').value = btn.dataset.drop === '1' ? 'full' : 'incremental';
        document.getElementById('scheduleEnabled').checked = btn.dataset.enabled === '1';
        var savedVersion = btn.dataset.version || 'latest';
        loadVersionsForDomain(domainName, savedVersion);
        applyTypeVisibility();
        new bootstrap.Modal(document.getElementById('scheduleModal')).show();
    }

    function resetVersionSelect(selectedValue) {
        var vSelect = document.getElementById('scheduleVersion');
        if (!vSelect) return;
        vSelect.innerHTML = '<option value="latest">Latest</option>';
        if (selectedValue && selectedValue !== 'latest') {
            vSelect.value = selectedValue;
        }
    }

    async function loadVersionsForDomain(domainName, selectedValue) {
        var vSelect = document.getElementById('scheduleVersion');
        if (!vSelect) return;
        vSelect.innerHTML = '<option value="latest">Loading...</option>';
        try {
            var resp = await fetch('/domain/list-versions?domain_name=' + encodeURIComponent(domainName),
                { credentials: 'same-origin' });
            var data = await resp.json();
            vSelect.innerHTML = '<option value="latest">Latest</option>';
            if (data.success && data.versions) {
                data.versions.forEach(function (v) {
                    var opt = document.createElement('option');
                    opt.value = v;
                    opt.textContent = 'v' + v;
                    vSelect.appendChild(opt);
                });
            }
            if (selectedValue) vSelect.value = selectedValue;
        } catch (e) {
            vSelect.innerHTML = '<option value="latest">Latest</option>';
        }
    }

    async function loadRulesForDomain(domainName, selectedValue) {
        const ruleSel = document.getElementById('scheduleCohortRule');
        if (!ruleSel) return;
        ruleSel.disabled = true;
        ruleSel.innerHTML = '<option value="">Loading rules...</option>';
        rulesById = {};
        try {
            const resp = await fetch('/settings/cohort-schedules/rules/' + encodeURIComponent(domainName),
                { credentials: 'same-origin' });
            const data = await resp.json();
            if (!data.success || !Array.isArray(data.rules) || data.rules.length === 0) {
                ruleSel.innerHTML = '<option value="">No saved cohort rules in this domain</option>';
                ruleSel.disabled = true;
                resetOutputs();
                return;
            }
            ruleSel.innerHTML = '<option value="">Select a rule</option>';
            data.rules.forEach(function (r) {
                rulesById[r.id] = r;
                const opt = document.createElement('option');
                opt.value = r.id;
                opt.textContent = (r.label || r.id) + ' (' + r.id + ')';
                ruleSel.appendChild(opt);
            });
            ruleSel.disabled = false;
            if (selectedValue && rulesById[selectedValue]) {
                ruleSel.value = selectedValue;
                prefillOutputsFromRule(selectedValue);
            } else {
                resetOutputs();
            }
        } catch (e) {
            ruleSel.innerHTML = '<option value="">Error loading rules</option>';
            ruleSel.disabled = true;
            resetOutputs();
        }
    }

    async function loadDomainsForModal() {
        const select = document.getElementById('scheduleDomain');
        select.innerHTML = '<option value="">Loading domains...</option>';
        try {
            const resp = await fetch('/settings/registry/domains', { credentials: 'same-origin' });
            const data = await resp.json();
            select.innerHTML = '<option value="">Select a domain</option>';
            const schedRows = data.domains || data.projects || [];
            if (data.success && schedRows.length) {
                schedRows.forEach(p => {
                    const opt = document.createElement('option');
                    opt.value = p.name;
                    opt.textContent = p.name;
                    select.appendChild(opt);
                });
            }
        } catch (e) {
            select.innerHTML = '<option value="">Error loading domains</option>';
        }
    }

    async function saveSchedule() {
        const kind = currentType();
        const domainName = document.getElementById('scheduleDomain').value;
        const ruleId = document.getElementById('scheduleCohortRule').value;
        const intervalMinutes = unitValueToMinutes();
        const dropExisting = document.getElementById('scheduleBuildMode').value === 'full';
        const enabled = document.getElementById('scheduleEnabled').checked;
        const version = (document.getElementById('scheduleVersion') || {}).value || 'latest';

        if (!domainName) {
            showNotification('Please select a domain', 'warning');
            return;
        }
        if (kind === 'cohort' && !ruleId) {
            showNotification('Please select a cohort rule', 'warning');
            return;
        }
        if (intervalMinutes < 2) {
            showNotification('Minimum interval is 2 minutes', 'warning');
            return;
        }

        const outputGraph = !!document.getElementById('scheduleOutputGraph')?.checked;
        const outputUc = !!document.getElementById('scheduleOutputUc')?.checked;
        if (kind === 'cohort' && !outputGraph && !outputUc) {
            showNotification('Pick at least one output target', 'warning');
            return;
        }

        const btn = document.getElementById('btnApplySchedule');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        try {
            const url = kind === 'cohort'
                ? '/settings/cohort-schedules'
                : '/settings/schedules';
            const body = kind === 'cohort'
                ? {
                    domain_name: domainName,
                    rule_id: ruleId,
                    interval_minutes: intervalMinutes,
                    enabled: enabled,
                    version: version,
                    output_graph: outputGraph,
                    output_uc: outputUc,
                }
                : {
                    domain_name: domainName,
                    interval_minutes: intervalMinutes,
                    drop_existing: dropExisting,
                    enabled: enabled,
                    version: version,
                };

            const resp = await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify(body),
            });
            if (!resp.ok) {
                var errText = '';
                try { var errData = await resp.json(); errText = errData.detail || errData.message || resp.statusText; }
                catch (_) { errText = resp.statusText; }
                showNotification('Error saving schedule (' + resp.status + '): ' + errText, 'error');
                return;
            }
            const data = await resp.json();
            if (data.success) {
                bootstrap.Modal.getInstance(document.getElementById('scheduleModal'))?.hide();
                showNotification(data.message || 'Schedule saved', 'success', 2000);
                await loadSchedules();
            } else {
                showNotification('Error: ' + (data.message || 'Unknown error'), 'error');
            }
        } catch (e) {
            showNotification('Error saving schedule: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-check-circle me-1"></i> Save';
        }
    }

    async function runScheduleNow(btn) {
        const ds = btn.dataset;
        const kind = ds.kind || 'build';
        const domainName = ds.domain || '';
        const ruleId = ds.rule || '';
        const label = kind === 'cohort'
            ? `${domainName} / ${ruleId}`
            : domainName;

        const original = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

        try {
            const url = kind === 'cohort'
                ? '/settings/cohort-schedules/' + encodeURIComponent(domainName) +
                    '/' + encodeURIComponent(ruleId) + '/run-now'
                : '/settings/schedules/' + encodeURIComponent(domainName) + '/run-now';
            const resp = await fetch(url, {
                method: 'POST',
                credentials: 'same-origin',
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.success) {
                showNotification(
                    'Could not trigger ' + label + ': ' + (data.detail || data.message || resp.statusText),
                    'error'
                );
                return;
            }
            showNotification('Triggered: ' + (data.message || label), 'success', 2500);
            // Materialise / build runs in APScheduler's worker thread; give it
            // a moment then refresh so the new "Last run" / status is visible.
            setTimeout(() => loadSchedules(), 1500);
        } catch (e) {
            showNotification('Error triggering ' + label + ': ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = original;
        }
    }

    async function deleteSchedule(ds) {
        const kind = ds.kind || 'build';
        const domainName = ds.domain || '';
        const ruleId = ds.rule || '';
        const label = kind === 'cohort'
            ? '"' + ruleId + '" in domain "' + domainName + '"'
            : '"' + domainName + '"';

        const confirmed = await showConfirmDialog({
            title: 'Remove Schedule',
            message: 'Remove the scheduled ' + (kind === 'cohort' ? 'cohort materialisation' : 'build') +
                ' for ' + label + '?',
            confirmText: 'Remove',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        try {
            const url = kind === 'cohort'
                ? '/settings/cohort-schedules/' + encodeURIComponent(domainName) + '/' + encodeURIComponent(ruleId)
                : '/settings/schedules/' + encodeURIComponent(domainName);
            const resp = await fetch(url, {
                method: 'DELETE',
                credentials: 'same-origin',
            });
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success', 2000);
                await loadSchedules();
            } else {
                showNotification('Error: ' + data.message, 'error');
            }
        } catch (e) {
            showNotification('Error removing schedule: ' + e.message, 'error');
        }
    }

    async function openHistoryModal(ds) {
        const kind = ds.kind || 'build';
        const domainName = ds.domain || '';
        const ruleId = ds.rule || '';

        const body = document.getElementById('scheduleHistoryBody');
        const label = document.getElementById('scheduleHistoryModalLabel');
        const titleSubject = kind === 'cohort'
            ? escapeHtml(domainName) + ' / ' + escapeHtml(ruleId)
            : escapeHtml(domainName);
        label.innerHTML = '<i class="bi bi-clock-history me-2"></i>Run History &mdash; ' + titleSubject;
        body.innerHTML = '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading history...</div>';

        new bootstrap.Modal(document.getElementById('scheduleHistoryModal')).show();

        try {
            const url = kind === 'cohort'
                ? '/settings/cohort-schedules/' + encodeURIComponent(domainName) +
                    '/' + encodeURIComponent(ruleId) + '/history'
                : '/settings/schedules/' + encodeURIComponent(domainName) + '/history';
            const resp = await fetch(url, { credentials: 'same-origin' });
            const data = await resp.json();

            if (!data.success) {
                body.innerHTML = '<div class="p-3 text-muted small">' +
                    '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtml(data.message || 'Could not load history') + '</div>';
                return;
            }

            if (!data.history || data.history.length === 0) {
                body.innerHTML = '<div class="p-3 text-muted small text-center">' +
                    '<i class="bi bi-clock"></i> No runs recorded yet.</div>';
                return;
            }

            const successCount = data.history.filter(h => h.status === 'success').length;
            const errorCount = data.history.filter(h => h.status === 'error').length;
            const totalTriples = data.history.reduce(
                (sum, h) => sum + ((kind === 'cohort'
                    ? (h.materialized_triples || 0)
                    : (h.triple_count || 0))), 0);
            const totalUcRows = data.history.reduce((sum, h) => sum + (h.uc_rows_written || 0), 0);
            const avgDuration = data.history.length > 0
                ? (data.history.reduce((sum, h) => sum + (h.duration_s || 0), 0) / data.history.length).toFixed(1)
                : '0';

            let html = '<div class="schedule-history-summary d-flex gap-2 px-3 py-2 bg-light border-bottom flex-wrap">' +
                '<span class="badge bg-secondary-subtle text-secondary border"><i class="bi bi-list-ol me-1"></i>' +
                    data.history.length + ' runs</span>' +
                '<span class="badge bg-success-subtle text-success border"><i class="bi bi-check-circle me-1"></i>' +
                    successCount + ' ok</span>' +
                '<span class="badge bg-danger-subtle text-danger border"><i class="bi bi-x-circle me-1"></i>' +
                    errorCount + ' failed</span>' +
                '<span class="badge bg-info-subtle text-info border"><i class="bi bi-diagram-3 me-1"></i>' +
                    totalTriples.toLocaleString() + ' triples</span>';
            if (kind === 'cohort') {
                html += '<span class="badge bg-info-subtle text-info border"><i class="bi bi-table me-1"></i>' +
                    totalUcRows.toLocaleString() + ' UC rows</span>';
            }
            html += '<span class="badge bg-primary-subtle text-primary border"><i class="bi bi-speedometer me-1"></i>' +
                    avgDuration + 's avg</span>' +
                '</div>';

            html += '<div class="schedule-history-table-wrapper">' +
                '<table class="table table-sm table-hover align-middle mb-0 schedule-history-table">' +
                '<thead><tr>' +
                    '<th class="ps-3">Time</th>' +
                    '<th class="text-center">Status</th>' +
                    '<th class="text-end">Duration</th>' +
                    '<th class="text-end">Triples</th>' +
                    (kind === 'cohort' ? '<th class="text-end">UC Rows</th>' : '') +
                    '<th class="ps-3">Message</th>' +
                '</tr></thead><tbody>';

            data.history.forEach(h => {
                let statusBadge;
                if (h.status === 'success') {
                    statusBadge = '<span class="badge bg-success-subtle text-success border"><i class="bi bi-check-circle me-1"></i>OK</span>';
                } else if (h.status === 'error') {
                    statusBadge = '<span class="badge bg-danger-subtle text-danger border"><i class="bi bi-x-circle me-1"></i>Error</span>';
                } else {
                    statusBadge = '<span class="badge bg-secondary-subtle text-secondary border">' + escapeHtml(h.status || '--') + '</span>';
                }

                const timeStr = h.timestamp ? formatAbsoluteTime(h.timestamp) : '--';
                const durationStr = h.duration_s != null ? h.duration_s + 's' : '--';
                const tripleVal = kind === 'cohort'
                    ? (h.materialized_triples != null ? h.materialized_triples : null)
                    : (h.triple_count != null ? h.triple_count : null);
                const tripleStr = tripleVal != null ? tripleVal.toLocaleString() : '--';
                const ucStr = h.uc_rows_written != null ? h.uc_rows_written.toLocaleString() : '--';
                const msgStr = h.message || '';

                html += '<tr>' +
                    '<td class="ps-3 small text-nowrap">' + timeStr + '</td>' +
                    '<td class="text-center">' + statusBadge + '</td>' +
                    '<td class="text-end small font-monospace">' + durationStr + '</td>' +
                    '<td class="text-end small font-monospace">' + tripleStr + '</td>' +
                    (kind === 'cohort'
                        ? '<td class="text-end small font-monospace">' + ucStr + '</td>'
                        : '') +
                    '<td class="small text-muted schedule-history-msg" title="' + escapeHtml(msgStr) + '">' +
                        escapeHtml(msgStr.length > 80 ? msgStr.substring(0, 80) + '...' : msgStr) + '</td>' +
                '</tr>';
            });

            html += '</tbody></table></div>';
            body.innerHTML = html;

        } catch (e) {
            body.innerHTML = '<div class="p-3 text-danger small">' +
                '<i class="bi bi-x-circle me-1"></i> Error: ' + escapeHtml(e.message) + '</div>';
        }
    }

    function formatAbsoluteTime(isoStr) {
        try {
            const d = new Date(isoStr);
            const pad = n => String(n).padStart(2, '0');
            const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'local';
            return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' +
                   pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()) +
                   ' (' + tz + ')';
        } catch {
            return isoStr;
        }
    }

    function formatRelativeTime(isoStr) {
        try {
            const d = new Date(isoStr);
            const now = new Date();
            const diffMs = d - now;
            const absDiffMs = Math.abs(diffMs);

            if (absDiffMs < 60000) return 'just now';

            const mins = Math.round(absDiffMs / 60000);
            const hours = Math.round(absDiffMs / 3600000);
            const days = Math.round(absDiffMs / 86400000);

            if (diffMs > 0) {
                if (mins < 60) return 'in ' + mins + ' min';
                if (hours < 24) return 'in ' + hours + 'h';
                return 'in ' + days + 'd';
            } else {
                if (mins < 60) return mins + ' min ago';
                if (hours < 24) return hours + 'h ago';
                return days + 'd ago';
            }
        } catch {
            return isoStr;
        }
    }
});
