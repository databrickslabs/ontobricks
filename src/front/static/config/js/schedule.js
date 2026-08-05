/**
 * OntoBricks - schedule.js
 * Settings → Automation → Scheduler.
 *
 * One generic surface over /settings/schedules for every task type. Each
 * type contributes a descriptor in TYPES holding its badge, its details
 * cell, any extra history columns, and the two functions that move its
 * options in and out of the modal (readConfig / applyConfig). Adding a
 * type is one entry here, not a new branch in six functions.
 */

document.addEventListener('DOMContentLoaded', function () {

    const API = '/settings/schedules';

    function badge(cls, icon, text) {
        return '<span class="badge bg-' + cls + '-subtle text-' + cls + ' border">' +
            '<i class="bi bi-' + icon + ' me-1"></i>' + text + '</span>';
    }

    function versionBadge(version) {
        return '<span class="badge bg-secondary-subtle text-secondary border">' +
            (version && version !== 'latest' ? 'v' + escapeHtml(version) : 'Latest') + '</span>';
    }

    function checked(id) {
        const el = document.getElementById(id);
        return !!(el && el.checked);
    }

    function setChecked(id, value) {
        const el = document.getElementById(id);
        if (el) el.checked = !!value;
    }

    const TYPES = {
        build: {
            label: 'Knowledge Graph build',
            badge: () => badge('primary', 'diagram-3', 'Build'),
            details: (s) => versionBadge(s.version),
            historyColumns: [],
            readConfig: () => ({ drop_existing: true }),
            applyConfig: () => {},
        },
        cohort: {
            label: 'cohort materialisation',
            badge: () => badge('info', 'people-fill', 'Cohort'),
            details: (s) => {
                const cfg = s.config || {};
                let out = '';
                if (cfg.output_graph !== false) out += ' ' + badge('primary', 'diagram-3', 'Graph');
                if (cfg.output_uc !== false) out += ' ' + badge('primary', 'table', 'UC');
                return '<span class="text-muted small me-1">Rule:</span><code class="small">' +
                    escapeHtml(s.target_key || '') + '</code>' + out +
                    '<span class="ms-2">' + versionBadge(s.version) + '</span>';
            },
            historyColumns: [
                { header: 'UC Rows', value: (h) => (h.detail || {}).uc_rows_written },
            ],
            readConfig: () => ({
                output_graph: checked('scheduleOutputGraph'),
                output_uc: checked('scheduleOutputUc'),
            }),
            applyConfig: (cfg) => {
                setChecked('scheduleOutputGraph', cfg.output_graph !== false);
                const u = document.getElementById('scheduleOutputUc');
                if (u && !u.disabled) u.checked = cfg.output_uc !== false;
            },
            validate: (cfg) => (cfg.output_graph || cfg.output_uc)
                ? '' : 'Pick at least one output target',
        },
        analytics: {
            label: 'graph analytics run',
            badge: () => badge('warning', 'graph-up', 'Analytics'),
            details: (s) => versionBadge(s.version),
            historyColumns: [],
            readConfig: () => ({}),
            applyConfig: () => {},
        },
        reasoning: {
            label: 'inference run',
            badge: () => badge('success', 'lightbulb', 'Inference'),
            details: (s) => {
                const cfg = s.config || {};
                const phases = cfg.phases || {};
                const on = Object.keys(phases).filter(k => phases[k]);
                let out = '';
                if (cfg.materialize_graph) out += ' ' + badge('primary', 'diagram-3', 'Graph');
                if (cfg.materialize_delta) out += ' ' + badge('primary', 'table', 'Delta');
                return '<span class="text-muted small me-1">Phases:</span>' +
                    '<code class="small">' + escapeHtml(on.join(', ') || 'none') + '</code>' +
                    out + '<span class="ms-2">' + versionBadge(s.version) + '</span>';
            },
            historyColumns: [
                { header: 'To Graph', value: (h) => (h.detail || {}).append_graph_count },
                { header: 'To Delta', value: (h) => (h.detail || {}).materialize_count },
            ],
            readConfig: () => {
                const phases = {};
                document.querySelectorAll('.schedule-phase').forEach(el => {
                    phases[el.dataset.phase] = el.checked;
                });
                return {
                    phases: phases,
                    materialize_graph: checked('scheduleMaterializeGraph'),
                    materialize_delta: checked('scheduleMaterializeDelta'),
                    materialize_table: (document.getElementById('scheduleMaterializeTable') || {}).value || '',
                };
            },
            applyConfig: (cfg) => {
                const phases = cfg.phases || {};
                document.querySelectorAll('.schedule-phase').forEach(el => {
                    el.checked = !!phases[el.dataset.phase];
                });
                setChecked('scheduleMaterializeGraph', cfg.materialize_graph !== false);
                setChecked('scheduleMaterializeDelta', !!cfg.materialize_delta);
                const table = document.getElementById('scheduleMaterializeTable');
                if (table) {
                    table.value = cfg.materialize_table || '';
                    table.disabled = !cfg.materialize_delta;
                }
            },
            validate: (cfg) => {
                if (!Object.keys(cfg.phases || {}).some(k => cfg.phases[k])) {
                    return 'Enable at least one inference phase';
                }
                if (!cfg.materialize_graph && !cfg.materialize_delta) {
                    return 'Pick at least one materialisation target';
                }
                if (cfg.materialize_delta && (cfg.materialize_table || '').split('.').length !== 3) {
                    return 'The Delta target must be a fully qualified catalog.schema.table';
                }
                return '';
            },
        },
    };

    const DEFAULT_TYPE = {
        label: 'task',
        badge: (s) => badge('secondary', 'clock', escapeHtml(s.task_type || '?')),
        details: (s) => versionBadge(s.version),
        historyColumns: [],
        readConfig: () => ({}),
        applyConfig: () => {},
    };

    function typeOf(key) {
        return TYPES[key] || DEFAULT_TYPE;
    }

    function needsTarget(key) {
        return key === 'cohort';
    }

    function scheduleUrl(taskType, domainName, suffix, targetKey) {
        let url = API + '/' + encodeURIComponent(taskType) + '/' + encodeURIComponent(domainName);
        if (suffix) url += '/' + suffix;
        if (targetKey) url += '?target=' + encodeURIComponent(targetKey);
        return url;
    }

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

    // null when adding, else { task_type, domain_name, target_key }
    let editing = null;

    // Cache of rules currently in the rule dropdown, indexed by id. Lets
    // us prefill the output checkboxes / target hint from the selected
    // rule without an extra API roundtrip.
    let rulesById = {};

    // The schedules behind the rendered table, indexed by row. Row
    // buttons carry only the index: the ``config`` blob is arbitrary
    // JSON and has no business round-tripping through a DOM attribute.
    let schedulesByRow = [];

    loadSchedules();

    document.getElementById('btnRefreshSchedules')?.addEventListener('click', () => loadSchedules());

    document.getElementById('btnAddSchedule')?.addEventListener('click', () => {
        editing = null;
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Add Schedule';
        setType('build');
        setTypeDisabled(false);

        document.getElementById('scheduleDomain').disabled = false;
        const ruleSel = document.getElementById('scheduleCohortRule');
        ruleSel.disabled = true;
        ruleSel.innerHTML = '<option value="">Select a domain first</option>';
        rulesById = {};
        document.getElementById('scheduleIntervalValue').value = '1';
        document.getElementById('scheduleIntervalUnit').value = 'hours';

        document.getElementById('scheduleEnabled').checked = true;
        resetOutputs();
        resetReasoningFields();
        resetVersionSelect();
        applyTypeVisibility();
        loadDomainsForModal();
        new bootstrap.Modal(document.getElementById('scheduleModal')).show();
    });

    document.getElementById('scheduleCohortRule')?.addEventListener('change', function () {
        prefillOutputsFromRule(this.value);
    });

    document.getElementById('scheduleMaterializeDelta')?.addEventListener('change', function () {
        const table = document.getElementById('scheduleMaterializeTable');
        if (table) table.disabled = !this.checked;
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
        const checkedInput = document.querySelector('input[name="scheduleType"]:checked');
        return (checkedInput && checkedInput.value) || 'build';
    }

    function setType(taskType) {
        const input = document.querySelector('input[name="scheduleType"][value="' + taskType + '"]');
        if (input) input.checked = true;
    }

    function setTypeDisabled(disabled) {
        document.querySelectorAll('input[name="scheduleType"]').forEach(input => {
            input.disabled = !!disabled;
        });
    }

    function applyTypeVisibility() {
        const active = currentType();
        Object.keys(TYPES).forEach(key => {
            document.querySelectorAll('.schedule-type-' + key).forEach(el => {
                el.style.display = key === active ? '' : 'none';
            });
        });
    }

    function resetOutputs() {
        setChecked('scheduleOutputGraph', true);
        const u = document.getElementById('scheduleOutputUc');
        if (u) {
            u.checked = true;
            u.disabled = false;
        }
        const hint = document.getElementById('scheduleOutputUcHint');
        if (hint) hint.textContent = " — uses the rule's saved target";
    }

    function resetReasoningFields() {
        TYPES.reasoning.applyConfig({
            phases: { tbox: true, swrl: true, graph: true },
            materialize_graph: true,
            materialize_delta: false,
            materialize_table: '',
        });
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
            const resp = await fetch(API, { credentials: 'same-origin' });
            const data = await resp.json().catch(() => ({}));

            if (data && data.success === false) {
                container.innerHTML = '<div class="text-muted small py-3">' +
                    '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtml(data.message || 'Could not load schedules') + '</div>';
                return;
            }

            const rows = Array.isArray(data.schedules) ? data.schedules.slice() : [];
            if (rows.length === 0) {
                container.innerHTML = '<div class="text-muted small py-4 text-center">' +
                    '<i class="bi bi-clock display-6 d-block mb-2 text-secondary"></i>' +
                    '<div class="mb-1">No schedules yet</div>' +
                    '<small>Click <strong>Add Schedule</strong> to create one.</small></div>';
                return;
            }

            const order = Object.keys(TYPES);
            rows.sort((a, b) => {
                const ta = order.indexOf(a.task_type);
                const tb = order.indexOf(b.task_type);
                if (ta !== tb) return ta - tb;
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

            schedulesByRow = rows;

            rows.forEach((s, index) => {
                const spec = typeOf(s.task_type);
                const domainName = s.domain_name || '';

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

                const dataAttrs = 'data-index="' + index + '" ';

                html += '<tr>' +
                    '<td class="ps-3 fw-semibold text-nowrap">' +
                        '<i class="bi bi-box me-1 text-primary"></i>' + escapeHtml(domainName) +
                    '</td>' +
                    '<td>' + spec.badge(s) + '</td>' +
                    '<td class="small">' + spec.details(s) + '</td>' +
                    '<td class="small text-muted text-nowrap">' + formatFrequency(s.interval_minutes) + '</td>' +
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
                                dataAttrs + 'title="Edit">' +
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
                btn.addEventListener('click', () => runScheduleNow(btn, scheduleOf(btn)));
            });
            container.querySelectorAll('.schedule-history-btn').forEach(btn => {
                btn.addEventListener('click', () => openHistoryModal(scheduleOf(btn)));
            });
            container.querySelectorAll('.schedule-edit-btn').forEach(btn => {
                btn.addEventListener('click', () => openEditModal(scheduleOf(btn)));
            });
            container.querySelectorAll('.schedule-delete-btn').forEach(btn => {
                btn.addEventListener('click', () => deleteSchedule(scheduleOf(btn)));
            });

        } catch (e) {
            console.error('Error loading schedules:', e);
            container.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading schedules: ' +
                escapeHtml(e.message) + '</div>';
        }
    }

    function scheduleOf(btn) {
        return schedulesByRow[parseInt(btn.dataset.index, 10)] || {};
    }

    function openEditModal(s) {
        const taskType = s.task_type || 'build';
        const domainName = s.domain_name || '';
        const targetKey = s.target_key || '';
        const config = s.config || {};

        editing = { task_type: taskType, domain_name: domainName, target_key: targetKey };
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Edit Schedule';

        setType(taskType);
        setTypeDisabled(true);

        const domSel = document.getElementById('scheduleDomain');
        domSel.innerHTML = '<option value="' + escapeHtml(domainName) + '" selected>' +
            escapeHtml(domainName) + '</option>';
        domSel.disabled = true;

        const ruleSel = document.getElementById('scheduleCohortRule');
        resetOutputs();
        resetReasoningFields();
        if (taskType === 'cohort') {
            ruleSel.innerHTML = '<option value="' + escapeHtml(targetKey) + '" selected>' +
                escapeHtml(targetKey) + '</option>';
            ruleSel.disabled = true;
            // Reload the full list (silently) so we can resolve the rule's
            // output config and refresh the UC target hint, then re-pin the
            // selection and the saved overrides.
            loadRulesForDomain(domainName, targetKey).then(() => {
                ruleSel.disabled = true;
                TYPES.cohort.applyConfig(config);
            });
        } else {
            ruleSel.innerHTML = '<option value="">—</option>';
            ruleSel.disabled = true;
            typeOf(taskType).applyConfig(config);
        }

        var uv = minutesToUnitValue(s.interval_minutes || 60);
        document.getElementById('scheduleIntervalValue').value = uv.value;
        document.getElementById('scheduleIntervalUnit').value = uv.unit;

        document.getElementById('scheduleEnabled').checked = !!s.enabled;
        loadVersionsForDomain(domainName, s.version || 'latest');
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
            const resp = await fetch(API + '/rules/' + encodeURIComponent(domainName),
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
        const taskType = currentType();
        const spec = typeOf(taskType);
        const domainName = document.getElementById('scheduleDomain').value;
        const targetKey = needsTarget(taskType)
            ? document.getElementById('scheduleCohortRule').value
            : '';
        const intervalMinutes = unitValueToMinutes();

        if (!domainName) {
            showNotification('Please select a domain', 'warning');
            return;
        }
        if (needsTarget(taskType) && !targetKey) {
            showNotification('Please select a cohort rule', 'warning');
            return;
        }
        if (intervalMinutes < 2) {
            showNotification('Minimum interval is 2 minutes', 'warning');
            return;
        }

        const config = spec.readConfig();
        const problem = spec.validate ? spec.validate(config) : '';
        if (problem) {
            showNotification(problem, 'warning');
            return;
        }

        const btn = document.getElementById('btnApplySchedule');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        try {
            const resp = await fetch(API, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({
                    task_type: taskType,
                    domain_name: domainName,
                    target_key: targetKey,
                    interval_minutes: intervalMinutes,
                    enabled: document.getElementById('scheduleEnabled').checked,
                    version: (document.getElementById('scheduleVersion') || {}).value || 'latest',
                    config: config,
                }),
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

    function scheduleLabel(s) {
        const domainName = s.domain_name || '';
        return s.target_key ? domainName + ' / ' + s.target_key : domainName;
    }

    async function runScheduleNow(btn, s) {
        const label = scheduleLabel(s);

        const original = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';

        try {
            const resp = await fetch(
                scheduleUrl(s.task_type || 'build', s.domain_name || '', 'run-now', s.target_key),
                { method: 'POST', credentials: 'same-origin' }
            );
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || !data.success) {
                showNotification(
                    'Could not trigger ' + label + ': ' + (data.detail || data.message || resp.statusText),
                    'error'
                );
                return;
            }
            showNotification('Triggered: ' + (data.message || label), 'success', 2500);
            // The run happens in APScheduler's worker thread; give it a
            // moment then refresh so the new "Last run" / status is visible.
            setTimeout(() => loadSchedules(), 1500);
        } catch (e) {
            showNotification('Error triggering ' + label + ': ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = original;
        }
    }

    async function deleteSchedule(s) {
        const taskType = s.task_type || 'build';
        const confirmed = await showConfirmDialog({
            title: 'Remove Schedule',
            message: 'Remove the scheduled ' + typeOf(taskType).label +
                ' for "' + scheduleLabel(s) + '"?',
            confirmText: 'Remove',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        try {
            const resp = await fetch(
                scheduleUrl(taskType, s.domain_name || '', '', s.target_key),
                { method: 'DELETE', credentials: 'same-origin' }
            );
            const data = await resp.json();
            if (data.success) {
                showNotification(data.message, 'success', 2000);
                await loadSchedules();
            } else {
                showNotification('Error: ' + (data.detail || data.message), 'error');
            }
        } catch (e) {
            showNotification('Error removing schedule: ' + e.message, 'error');
        }
    }

    async function openHistoryModal(s) {
        const taskType = s.task_type || 'build';
        const spec = typeOf(taskType);
        const extraColumns = spec.historyColumns || [];

        const body = document.getElementById('scheduleHistoryBody');
        const label = document.getElementById('scheduleHistoryModalLabel');
        label.innerHTML = '<i class="bi bi-clock-history me-2"></i>Run History &mdash; ' +
            escapeHtml(scheduleLabel(s));
        body.innerHTML = '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading history...</div>';

        new bootstrap.Modal(document.getElementById('scheduleHistoryModal')).show();

        try {
            const resp = await fetch(
                scheduleUrl(taskType, s.domain_name || '', 'history', s.target_key),
                { credentials: 'same-origin' }
            );
            const data = await resp.json();

            if (!data.success) {
                body.innerHTML = '<div class="p-3 text-muted small">' +
                    '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtml(data.detail || data.message || 'Could not load history') + '</div>';
                return;
            }

            const history = data.history || [];
            if (history.length === 0) {
                body.innerHTML = '<div class="p-3 text-muted small text-center">' +
                    '<i class="bi bi-clock"></i> No runs recorded yet.</div>';
                return;
            }

            const successCount = history.filter(h => h.status === 'success').length;
            const errorCount = history.filter(h => h.status === 'error').length;
            const totalCount = history.reduce((sum, h) => sum + (h.triple_count || 0), 0);
            const avgDuration = (history.reduce((sum, h) => sum + (h.duration_s || 0), 0) / history.length).toFixed(1);

            let html = '<div class="schedule-history-summary d-flex gap-2 px-3 py-2 bg-light border-bottom flex-wrap">' +
                '<span class="badge bg-secondary-subtle text-secondary border"><i class="bi bi-list-ol me-1"></i>' +
                    history.length + ' runs</span>' +
                '<span class="badge bg-success-subtle text-success border"><i class="bi bi-check-circle me-1"></i>' +
                    successCount + ' ok</span>' +
                '<span class="badge bg-danger-subtle text-danger border"><i class="bi bi-x-circle me-1"></i>' +
                    errorCount + ' failed</span>' +
                '<span class="badge bg-info-subtle text-info border"><i class="bi bi-diagram-3 me-1"></i>' +
                    totalCount.toLocaleString() + ' written</span>' +
                '<span class="badge bg-primary-subtle text-primary border"><i class="bi bi-speedometer me-1"></i>' +
                    avgDuration + 's avg</span>' +
                '</div>';

            html += '<div class="schedule-history-table-wrapper">' +
                '<table class="table table-sm table-hover align-middle mb-0 schedule-history-table">' +
                '<thead><tr>' +
                    '<th class="ps-3">Time</th>' +
                    '<th class="text-center">Status</th>' +
                    '<th class="text-end">Duration</th>' +
                    '<th class="text-end">Count</th>' +
                    extraColumns.map(c => '<th class="text-end">' + escapeHtml(c.header) + '</th>').join('') +
                    '<th class="ps-3">Message</th>' +
                '</tr></thead><tbody>';

            history.forEach(h => {
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
                const countStr = h.triple_count != null ? h.triple_count.toLocaleString() : '--';
                const msgStr = h.message || '';

                html += '<tr>' +
                    '<td class="ps-3 small text-nowrap">' + timeStr + '</td>' +
                    '<td class="text-center">' + statusBadge + '</td>' +
                    '<td class="text-end small font-monospace">' + durationStr + '</td>' +
                    '<td class="text-end small font-monospace">' + countStr + '</td>' +
                    extraColumns.map(c => {
                        const v = c.value(h);
                        return '<td class="text-end small font-monospace">' +
                            (v != null ? Number(v).toLocaleString() : '--') + '</td>';
                    }).join('') +
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
