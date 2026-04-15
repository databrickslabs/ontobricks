/**
 * OntoBricks - schedule.js
 * Schedule tab: CRUD for per-domain scheduled Digital Twin builds
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

    let schedulesLoaded = false;
    let editingDomain = null;

    loadSchedules();

    document.getElementById('btnRefreshSchedules')?.addEventListener('click', () => loadSchedules());

    document.getElementById('btnAddSchedule')?.addEventListener('click', () => {
        editingDomain = null;
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Add Schedule';
        const projSelect = document.getElementById('scheduleDomain');
        projSelect.disabled = false;
        document.getElementById('scheduleIntervalValue').value = '1';
        document.getElementById('scheduleIntervalUnit').value = 'hours';
        document.getElementById('scheduleBuildMode').value = 'full';
        document.getElementById('scheduleEnabled').checked = true;
        resetVersionSelect();
        loadDomainsForModal();
        new bootstrap.Modal(document.getElementById('scheduleModal')).show();
    });

    document.getElementById('scheduleDomain')?.addEventListener('change', function () {
        var domainName = this.value;
        if (domainName) {
            loadVersionsForDomain(domainName);
        } else {
            resetVersionSelect();
        }
    });

    document.getElementById('btnApplySchedule')?.addEventListener('click', saveSchedule);

    async function loadSchedules() {
        const container = document.getElementById('schedulesTableContainer');
        if (!container) return;

        container.innerHTML = '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading schedules...</div>';

        try {
            const resp = await fetch('/settings/schedules', { credentials: 'same-origin' });
            const data = await resp.json();

            if (!data.success) {
                container.innerHTML = '<div class="text-muted small py-3">' +
                    '<i class="bi bi-exclamation-triangle text-warning me-1"></i> ' +
                    escapeHtml(data.message || 'Could not load schedules') + '</div>';
                return;
            }

            schedulesLoaded = true;

            if (!data.schedules || data.schedules.length === 0) {
                container.innerHTML = '<div class="text-muted small py-3 text-center">' +
                    '<i class="bi bi-clock"></i> No scheduled builds yet. Click <strong>Add Schedule</strong> to create one.</div>';
                return;
            }

            let html = '<div class="table-responsive">' +
                '<table class="table table-sm table-hover align-middle mb-0">' +
                '<thead><tr>' +
                    '<th class="ps-3">Domain</th>' +
                    '<th>Version</th>' +
                    '<th>Mode</th>' +
                    '<th>Frequency</th>' +
                    '<th class="text-center">Status</th>' +
                    '<th>Last Run</th>' +
                    '<th>Next Run</th>' +
                    '<th class="text-end pe-3" style="width:8rem;"></th>' +
                '</tr></thead><tbody>';

            data.schedules.forEach(s => {
                const freqLabel = formatFrequency(s.interval_minutes);
                const versionLabel = s.version && s.version !== 'latest'
                    ? '<span class="badge bg-primary-subtle text-primary border">v' + escapeHtml(s.version) + '</span>'
                    : '<span class="badge bg-secondary-subtle text-secondary border">Latest</span>';
                const modeLabel = s.drop_existing
                    ? '<span class="badge bg-warning-subtle text-warning border"><i class="bi bi-arrow-repeat me-1"></i>Full</span>'
                    : '<span class="badge bg-info-subtle text-info border"><i class="bi bi-lightning me-1"></i>Incremental</span>';
                const enabledBadge = s.enabled
                    ? '<span class="badge bg-success-subtle text-success border">Active</span>'
                    : '<span class="badge bg-secondary-subtle text-secondary border">Paused</span>';

                let statusBadge = '<span class="text-muted small">--</span>';
                if (s.last_status === 'success') {
                    statusBadge = '<span class="badge bg-success-subtle text-success border"><i class="bi bi-check-circle me-1"></i>OK</span>';
                } else if (s.last_status === 'error') {
                    statusBadge = '<span class="badge bg-danger-subtle text-danger border" title="' +
                        escapeHtml(s.last_message || '') + '"><i class="bi bi-x-circle me-1"></i>Error</span>';
                }

                const lastRun = s.last_run ? formatRelativeTime(s.last_run) : '--';
                const nextRun = s.next_run ? formatAbsoluteTime(s.next_run) : '--';

                const schedDomain = s.domain_name || s.project_name || '';
                html += '<tr>' +
                    '<td class="ps-3 fw-semibold"><i class="bi bi-folder2 me-1 text-primary"></i>' + escapeHtml(schedDomain) + '</td>' +
                    '<td>' + versionLabel + '</td>' +
                    '<td>' + modeLabel + '</td>' +
                    '<td class="small">' + freqLabel + '</td>' +
                    '<td class="text-center">' + enabledBadge + ' ' + statusBadge + '</td>' +
                    '<td class="small text-muted">' + lastRun + '</td>' +
                    '<td class="small text-muted">' + nextRun + '</td>' +
                    '<td class="text-end pe-3">' +
                        '<button type="button" class="btn btn-sm btn-outline-info border-0 schedule-history-btn" ' +
                            'data-domain="' + escapeHtml(schedDomain) + '" ' +
                            'title="Run history"><i class="bi bi-journal-text"></i></button>' +
                        '<button type="button" class="btn btn-sm btn-outline-secondary border-0 schedule-edit-btn" ' +
                            'data-domain="' + escapeHtml(schedDomain) + '" ' +
                            'data-interval="' + s.interval_minutes + '" ' +
                            'data-drop="' + (s.drop_existing ? '1' : '0') + '" ' +
                            'data-enabled="' + (s.enabled ? '1' : '0') + '" ' +
                            'data-version="' + escapeHtml(s.version || 'latest') + '" ' +
                            'title="Edit"><i class="bi bi-pencil"></i></button>' +
                        '<button type="button" class="btn btn-sm btn-outline-danger border-0 schedule-delete-btn" ' +
                            'data-domain="' + escapeHtml(schedDomain) + '" ' +
                            'title="Remove schedule"><i class="bi bi-trash"></i></button>' +
                    '</td>' +
                '</tr>';
            });

            html += '</tbody></table></div>';
            container.innerHTML = html;

            container.querySelectorAll('.schedule-history-btn').forEach(btn => {
                btn.addEventListener('click', () => openHistoryModal(btn.dataset.domain));
            });
            container.querySelectorAll('.schedule-edit-btn').forEach(btn => {
                btn.addEventListener('click', () => openEditModal(btn));
            });
            container.querySelectorAll('.schedule-delete-btn').forEach(btn => {
                btn.addEventListener('click', () => deleteSchedule(btn.dataset.domain));
            });

        } catch (e) {
            console.error('Error loading schedules:', e);
            container.innerHTML = '<div class="text-danger small py-3">' +
                '<i class="bi bi-x-circle me-1"></i> Error loading schedules: ' +
                escapeHtml(e.message) + '</div>';
        }
    }

    function openEditModal(btn) {
        editingDomain = btn.dataset.domain;
        document.getElementById('scheduleModalLabel').innerHTML =
            '<i class="bi bi-clock-history me-2"></i>Edit Schedule';
        const projSelect = document.getElementById('scheduleDomain');
        projSelect.innerHTML = '<option value="' + escapeHtml(editingDomain) + '" selected>' +
            escapeHtml(editingDomain) + '</option>';
        projSelect.disabled = true;
        var uv = minutesToUnitValue(parseInt(btn.dataset.interval, 10));
        document.getElementById('scheduleIntervalValue').value = uv.value;
        document.getElementById('scheduleIntervalUnit').value = uv.unit;
        document.getElementById('scheduleBuildMode').value = btn.dataset.drop === '1' ? 'full' : 'incremental';
        document.getElementById('scheduleEnabled').checked = btn.dataset.enabled === '1';
        var savedVersion = btn.dataset.version || 'latest';
        loadVersionsForDomain(editingDomain, savedVersion);
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

    async function loadDomainsForModal() {
        const select = document.getElementById('scheduleDomain');
        select.innerHTML = '<option value="">Loading domains...</option>';
        try {
            const resp = await fetch('/settings/registry/domains', { credentials: 'same-origin' });
            const data = await resp.json();
            select.innerHTML = '<option value="">-- Select a domain --</option>';
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
        const domainName = document.getElementById('scheduleDomain').value;
        const intervalMinutes = unitValueToMinutes();
        const dropExisting = document.getElementById('scheduleBuildMode').value === 'full';
        const enabled = document.getElementById('scheduleEnabled').checked;
        const version = (document.getElementById('scheduleVersion') || {}).value || 'latest';

        if (!domainName) {
            showNotification('Please select a domain', 'warning');
            return;
        }

        if (intervalMinutes < 2) {
            showNotification('Minimum interval is 2 minutes', 'warning');
            return;
        }

        const btn = document.getElementById('btnApplySchedule');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        try {
            const resp = await fetch('/settings/schedules', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({
                    domain_name: domainName,
                    interval_minutes: intervalMinutes,
                    drop_existing: dropExisting,
                    enabled: enabled,
                    version: version,
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

    async function deleteSchedule(domainName) {
        const confirmed = await showConfirmDialog({
            title: 'Remove Schedule',
            message: 'Remove the scheduled build for "' + domainName + '"?',
            confirmText: 'Remove',
            confirmClass: 'btn-danger',
            icon: 'trash',
        });
        if (!confirmed) return;

        try {
            const resp = await fetch('/settings/schedules/' + encodeURIComponent(domainName), {
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

    async function openHistoryModal(domainName) {
        const body = document.getElementById('scheduleHistoryBody');
        const label = document.getElementById('scheduleHistoryModalLabel');
        label.innerHTML = '<i class="bi bi-clock-history me-2"></i>Run History &mdash; ' + escapeHtml(domainName);
        body.innerHTML = '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading history...</div>';

        new bootstrap.Modal(document.getElementById('scheduleHistoryModal')).show();

        try {
            const resp = await fetch('/settings/schedules/' + encodeURIComponent(domainName) + '/history',
                { credentials: 'same-origin' });
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
            const totalTriples = data.history.reduce((sum, h) => sum + (h.triple_count || 0), 0);
            const avgDuration = data.history.length > 0
                ? (data.history.reduce((sum, h) => sum + (h.duration_s || 0), 0) / data.history.length).toFixed(1)
                : '0';

            let html = '<div class="schedule-history-summary d-flex gap-3 px-3 py-2 bg-light border-bottom">' +
                '<span class="badge bg-secondary-subtle text-secondary border"><i class="bi bi-list-ol me-1"></i>' +
                    data.history.length + ' runs</span>' +
                '<span class="badge bg-success-subtle text-success border"><i class="bi bi-check-circle me-1"></i>' +
                    successCount + ' success</span>' +
                '<span class="badge bg-danger-subtle text-danger border"><i class="bi bi-x-circle me-1"></i>' +
                    errorCount + ' failed</span>' +
                '<span class="badge bg-info-subtle text-info border"><i class="bi bi-diagram-3 me-1"></i>' +
                    totalTriples.toLocaleString() + ' triples total</span>' +
                '<span class="badge bg-primary-subtle text-primary border"><i class="bi bi-speedometer me-1"></i>' +
                    avgDuration + 's avg</span>' +
                '</div>';

            html += '<div class="schedule-history-table-wrapper">' +
                '<table class="table table-sm table-hover align-middle mb-0 schedule-history-table">' +
                '<thead><tr>' +
                    '<th class="ps-3">Time</th>' +
                    '<th class="text-center">Status</th>' +
                    '<th class="text-end">Duration</th>' +
                    '<th class="text-end">Triples</th>' +
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
                const tripleStr = h.triple_count != null ? h.triple_count.toLocaleString() : '--';
                const msgStr = h.message || '';

                html += '<tr>' +
                    '<td class="ps-3 small text-nowrap">' + timeStr + '</td>' +
                    '<td class="text-center">' + statusBadge + '</td>' +
                    '<td class="text-end small font-monospace">' + durationStr + '</td>' +
                    '<td class="text-end small font-monospace">' + tripleStr + '</td>' +
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
