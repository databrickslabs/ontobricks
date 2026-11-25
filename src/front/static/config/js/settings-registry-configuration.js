/**
 * OntoBricks - settings-registry-configuration.js
 *
 * Settings → Registry tab: Unity Catalog schema/volume access checks and
 * Lakebase permission-check / grant-result rendering. Extracted from an
 * inline <script> in
 * front/templates/partials/registry/_registry_configuration.html so the
 * partial stays markup-only.
 *
 * ``_obRenderRegistryGrants`` is exposed on ``window`` because
 * ``registry.js`` (loaded globally in base.html) calls it after a
 * successful Settings → Registry → Initialize.
 */
(function () {
    function _esc(s) {
        return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    // ── UC badge for schema/volume item ──────────────────────────────
    function _ucBadgeHtml(result) {
        if (!result) return '';
        if (result.exists === null || result.exists === undefined) {
            return '<span class="badge bg-secondary-subtle text-secondary border border-secondary" style="font-size:0.65rem">'
                + '<i class="bi bi-question-circle me-1"></i>Unknown</span>';
        }
        if (!result.exists) {
            return '<span class="badge bg-danger-subtle text-danger-emphasis border border-danger" style="font-size:0.65rem">'
                + '<i class="bi bi-x-circle me-1"></i>Not found</span>';
        }
        if (!result.accessible) {
            return '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning" style="font-size:0.65rem">'
                + '<i class="bi bi-shield-exclamation me-1"></i>Access denied</span>';
        }
        return '<span class="badge bg-success-subtle text-success-emphasis border border-success" style="font-size:0.65rem">'
            + '<i class="bi bi-shield-check me-1"></i>OK</span>';
    }

    function _ucDetailHtml(result) {
        if (!result || result.accessible) return '';
        var cls = result.exists === false ? 'text-danger' : 'text-warning-emphasis';
        return '<span class="' + cls + '"><i class="bi bi-info-circle me-1"></i>'
            + _esc(result.error || 'Unknown issue') + '</span>';
    }

    // ── Lakebase per-check badge ──────────────────────────────────────
    var _LB_BADGE = {
        ok:      '<span class="badge bg-success-subtle text-success-emphasis border border-success" style="font-size:0.65rem"><i class="bi bi-check2-circle me-1"></i>OK</span>',
        warning: '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning"  style="font-size:0.65rem"><i class="bi bi-exclamation-triangle me-1"></i>Warning</span>',
        error:   '<span class="badge bg-danger-subtle  text-danger-emphasis  border border-danger"   style="font-size:0.65rem"><i class="bi bi-x-circle me-1"></i>Error</span>',
        missing: '<span class="badge bg-secondary-subtle text-secondary border border-secondary"     style="font-size:0.65rem"><i class="bi bi-dash-circle me-1"></i>Not created</span>',
    };

    function _renderLbChecks(lb) {
        var panel   = document.getElementById('lbChecksPanel');
        var body    = document.getElementById('lbChecksBody');
        var ctx     = document.getElementById('lbChecksContext');
        var summary = document.getElementById('lbChecksSummaryBadge');
        if (!panel || !body) return;

        if (!lb || lb.success === false) {
            var errMsg = (lb && lb.error)
                ? lb.error
                : (lb && lb.checks && lb.checks.length
                    ? lb.checks.map(function(c){ return (c.label || c.id) + ': ' + (c.detail || c.status); }).join(' | ')
                    : 'Lakebase check unavailable — is psycopg installed?');
            var errHtml = '<div class="alert alert-warning small py-2 mb-0"><i class="bi bi-exclamation-triangle me-1"></i>'
                + _esc(errMsg)
                + '</div>';
            // If checks are available render them below the error banner so the
            // user can see exactly which step failed without opening the logs.
            if (lb && lb.checks && lb.checks.length) {
                if (ctx) ctx.innerHTML = lb.database
                    ? '<i class="bi bi-server me-1"></i>Database: <code>' + _esc(lb.database) + '</code>'
                      + ' &nbsp;·&nbsp; <i class="bi bi-grid me-1"></i>Schema: <code>' + _esc(lb.schema || '—') + '</code>'
                    : '';
                body.innerHTML = errHtml + _buildChecksTable(lb.checks);
            } else {
                body.innerHTML = errHtml;
            }
            if (summary) summary.innerHTML = '<span class="badge bg-secondary-subtle text-secondary border" style="font-size:0.65rem">N/A</span>';
            if (ctx) ctx.textContent = '';
            panel.style.display = '';
            return;
        }

        if (ctx) {
            ctx.innerHTML = '<i class="bi bi-server me-1"></i>Database: <code>' + _esc(lb.database || '—') + '</code>'
                + ' &nbsp;·&nbsp; <i class="bi bi-person me-1"></i>User: <code>' + _esc(lb.user || '—') + '</code>'
                + ' &nbsp;·&nbsp; <i class="bi bi-grid me-1"></i>Schema: <code>' + _esc(lb.schema || '—') + '</code>';
        }

        var checks = lb.checks || [];
        var errCount  = checks.filter(function(c){ return c.status === 'error';   }).length;
        var warnCount = checks.filter(function(c){ return c.status === 'warning'; }).length;

        if (summary) {
            if (errCount > 0) {
                summary.innerHTML = '<span class="badge bg-danger-subtle text-danger-emphasis border border-danger" style="font-size:0.65rem">'
                    + '<i class="bi bi-x-circle me-1"></i>' + errCount + ' error' + (errCount > 1 ? 's' : '') + '</span>';
            } else if (warnCount > 0) {
                summary.innerHTML = '<span class="badge bg-warning-subtle text-warning-emphasis border border-warning" style="font-size:0.65rem">'
                    + '<i class="bi bi-exclamation-triangle me-1"></i>' + warnCount + ' warning' + (warnCount > 1 ? 's' : '') + '</span>';
            } else {
                summary.innerHTML = '<span class="badge bg-success-subtle text-success-emphasis border border-success" style="font-size:0.65rem">'
                    + '<i class="bi bi-shield-check me-1"></i>All checks passed</span>';
            }
        }

        body.innerHTML = _buildChecksTable(checks);
        panel.style.display = '';
    }

    function _buildChecksTable(checks) {
        var structural = (checks || []).filter(function(c){ return !c.id.startsWith('tbl_'); });
        var tables     = (checks || []).filter(function(c){ return c.id.startsWith('tbl_'); });
        var html = '';

        html += '<div class="mb-2">';
        structural.forEach(function(c) {
            var badge = _LB_BADGE[c.status] || _LB_BADGE.missing;
            html += '<div class="d-flex align-items-start gap-2 py-1 border-bottom">'
                + '<div class="flex-shrink-0 mt-1">' + badge + '</div>'
                + '<div class="flex-grow-1 small">'
                +   '<span class="fw-semibold">' + _esc(c.label) + '</span>'
                + (c.detail ? '<div class="text-muted mt-1" style="font-size:0.72rem">'
                    + '<i class="bi bi-terminal me-1"></i><code>' + _esc(c.detail) + '</code></div>' : '')
                + '</div></div>';
        });
        html += '</div>';

        if (tables.length > 0) {
            var tblErrCount = tables.filter(function(c){ return c.status === 'error'; }).length;
            var tblOkCount  = tables.filter(function(c){ return c.status === 'ok'; }).length;
            var tblMissing  = tables.filter(function(c){ return c.status === 'missing'; }).length;
            var collapseId  = 'lbTblCollapse';
            var summaryText = tblOkCount + ' ok';
            if (tblErrCount) summaryText += ', ' + tblErrCount + ' error' + (tblErrCount > 1 ? 's' : '');
            if (tblMissing)  summaryText += ', ' + tblMissing + ' not created';

            html += '<div>';
            html += '<button class="btn btn-link btn-sm p-0 text-decoration-none" '
                + 'data-bs-toggle="collapse" data-bs-target="#' + collapseId + '" '
                + 'aria-expanded="' + (tblErrCount > 0 ? 'true' : 'false') + '">'
                + '<i class="bi bi-table me-1"></i>Registry tables ('
                + summaryText + ')'
                + ' <i class="bi bi-chevron-down" style="font-size:0.7rem"></i></button>';

            html += '<div class="collapse' + (tblErrCount > 0 ? ' show' : '') + '" id="' + collapseId + '">';
            html += '<table class="table table-sm small mt-2 mb-0">';
            html += '<thead class="table-light"><tr><th class="ps-2">Table</th><th>Status</th><th>Detail</th></tr></thead><tbody>';
            tables.forEach(function(c) {
                var badge = _LB_BADGE[c.status] || _LB_BADGE.missing;
                var rowCls = c.status === 'error' ? 'table-danger' : (c.status === 'warning' ? 'table-warning' : '');
                html += '<tr class="' + rowCls + '">'
                    + '<td class="ps-2 font-monospace">' + _esc(c.label.replace('Table: ', '')) + '</td>'
                    + '<td>' + badge + '</td>'
                    + '<td class="text-muted" style="font-size:0.7rem;max-width:340px">'
                    + (c.detail ? '<code>' + _esc(c.detail) + '</code>' : '') + '</td>'
                    + '</tr>';
            });
            html += '</tbody></table></div></div>';
        }
        return html;
    }

    // ── Main click handler ────────────────────────────────────────────
    document.getElementById('btnCheckRegistryAccess')?.addEventListener('click', async function () {
        var btn = this;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Checking…';

        // Reset UC badge
        var schemaBadgeEl = document.getElementById('registrySchemaCheckBadge');
        if (schemaBadgeEl) schemaBadgeEl.innerHTML = '<span class="spinner-border spinner-border-sm text-secondary" style="width:0.75rem;height:0.75rem"></span>';
        var schemaDetailEl = document.getElementById('registrySchemaCheckDetail');
        if (schemaDetailEl) { schemaDetailEl.innerHTML = ''; schemaDetailEl.style.display = 'none'; }
        var lbPanel = document.getElementById('lbChecksPanel');
        if (lbPanel) lbPanel.style.display = 'none';

        try {
            var resp = await fetch('/settings/registry/check', { credentials: 'same-origin' });
            var data = await resp.json();

            var uc = data.uc || data;   // back-compat if uc wrapper missing
            var lb = data.lakebase;

            // UC schema
            var schemaBadge  = document.getElementById('registrySchemaCheckBadge');
            var schemaDetail = document.getElementById('registrySchemaCheckDetail');
            if (schemaBadge) schemaBadge.innerHTML = _ucBadgeHtml(uc.schema);
            var sd = _ucDetailHtml(uc.schema);
            if (sd && schemaDetail) { schemaDetail.innerHTML = sd; schemaDetail.style.display = ''; }

            // Lakebase
            _renderLbChecks(lb);

            // Summary toast
            var ucOk = !!uc.schema?.accessible;
            var lbOk = lb && lb.success && lb.checks && !lb.checks.some(function(c){ return c.status === 'error'; });
            var allOk = ucOk && lbOk;
            if (typeof showNotification === 'function') {
                showNotification(
                    allOk
                        ? 'All checks passed — UC and Lakebase permissions are in order.'
                        : 'Check completed with issues — review the details below.',
                    allOk ? 'success' : 'warning'
                );
            }
        } catch (e) {
            if (typeof showNotification === 'function')
                showNotification('Check failed: ' + e.message, 'danger');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-shield-check me-1"></i>Check Access';
        }
    });

    // ── Permission grant results ──────────────────────────────────────
    // Renders the {granted: [...], warnings: [...]} summary returned by
    // POST /settings/registry/grant-permissions (Repair button) and by
    // POST /settings/registry/initialize (the ``permissions`` key).
    function _renderGrantResult(perm) {
        var panel   = document.getElementById('lbGrantPanel');
        var body    = document.getElementById('lbGrantBody');
        var summary = document.getElementById('lbGrantSummaryBadge');
        if (!panel || !body || !perm) return;

        var granted  = perm.granted || [];
        var warnings = perm.warnings || [];
        var ok = perm.success !== false && warnings.length === 0;

        if (summary) {
            if (perm.success === false) {
                summary.innerHTML = '<span class="badge bg-danger">failed</span>';
            } else if (warnings.length) {
                summary.innerHTML = '<span class="badge bg-warning text-dark">' +
                    granted.length + ' applied · ' + warnings.length + ' warning' +
                    (warnings.length === 1 ? '' : 's') + '</span>';
            } else {
                summary.innerHTML = '<span class="badge bg-success">' +
                    granted.length + ' applied</span>';
            }
        }

        var html = '';
        if (perm.error) {
            html += '<div class="text-danger small mb-2"><i class="bi bi-x-circle me-1"></i>' +
                _esc(perm.error) + '</div>';
        }
        if (granted.length) {
            html += '<ul class="list-unstyled small mb-2">';
            granted.forEach(function (g) {
                html += '<li class="text-success"><i class="bi bi-check-circle me-1"></i>' +
                    _esc(g) + '</li>';
            });
            html += '</ul>';
        }
        if (warnings.length) {
            html += '<ul class="list-unstyled small mb-0">';
            warnings.forEach(function (w) {
                html += '<li class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>' +
                    _esc(w) + '</li>';
            });
            html += '</ul>';
        }
        if (!html) {
            html = '<div class="text-muted small">No grants reported.</div>';
        }
        body.innerHTML = html;
        panel.style.display = '';
        return ok;
    }

    // Expose so registry.js can surface the post-Initialize ``permissions``.
    window._obRenderRegistryGrants = _renderGrantResult;

    // ── Setup prerequisites (read-only) ───────────────────────────────
    // Two identities: POST /settings/registry/setup/check runs as the signed-in
    // admin; /settings/diagnostics/readiness runs as the app's service principal.
    var _SETUP_BADGE = {
        ok:      '<span class="badge bg-success"><i class="bi bi-check-circle me-1"></i>ok</span>',
        warning: '<span class="badge bg-warning text-dark"><i class="bi bi-exclamation-triangle me-1"></i>warning</span>',
        error:   '<span class="badge bg-danger"><i class="bi bi-x-circle me-1"></i>error</span>',
        skipped: '<span class="badge bg-secondary"><i class="bi bi-dash-circle me-1"></i>skipped</span>'
    };
    var _lastSetupDiagnostics = null;

    function _setupCheckItem(c) {
        return '<li class="list-group-item px-0 d-flex gap-2 align-items-start">'
            + '<div class="flex-shrink-0">' + (_SETUP_BADGE[c.status] || _SETUP_BADGE.skipped) + '</div>'
            + '<div class="flex-grow-1 text-break">'
            +   '<div class="fw-semibold">' + _esc(c.label || c.name) + '</div>'
            +   (c.detail ? '<div class="text-muted">' + _esc(c.detail) + '</div>' : '')
            +   (c.fix ? '<div><i class="bi bi-wrench me-1"></i><strong>Fix:</strong> ' + _esc(c.fix) + '</div>' : '')
            + '</div></li>';
    }

    function _renderSetupChecks(you, app) {
        var panel = document.getElementById('registrySetupChecks');
        if (!panel) return;
        // Service-principal side: problems first; collapse the passing ones into one line.
        var appProblems = app.filter(function (c) { return c.status !== 'ok'; });
        var appOk = app.length - appProblems.length;
        document.getElementById('registrySetupChecksYou').innerHTML = you.map(_setupCheckItem).join('');
        document.getElementById('registrySetupChecksApp').innerHTML = appProblems.map(_setupCheckItem).join('')
            + '<li class="list-group-item px-0 text-muted"><i class="bi bi-check-all me-1"></i>'
            + appOk + ' other check' + (appOk === 1 ? '' : 's') + ' passed</li>';
        var errors = you.concat(app).filter(function (c) { return c.status === 'error'; }).length;
        document.getElementById('registrySetupChecksSummary').textContent = errors
            ? errors + ' problem' + (errors === 1 ? '' : 's') + ' to fix before setup'
            : 'Ready to set up';
        panel.classList.remove('d-none');
        return errors;
    }

    async function _runSetupChecks(form) {
        var body = JSON.stringify({
            catalog: form.elements.catalog.value.trim(),
            schema: form.elements.schema.value.trim()
        });
        var youResp = fetch('/settings/registry/setup/check', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            credentials: 'same-origin', body: body
        }).then(function (r) { return r.json().then(function (d) { return r.ok ? d.checks : [{
            name: 'you.check', label: 'Checks as you', status: 'error',
            detail: d.message || d.error || ('HTTP ' + r.status)
        }]; }); });
        var appResp = fetch('/settings/diagnostics/readiness', { credentials: 'same-origin' })
            .then(function (r) { return r.json(); })
            .then(function (d) { return d.checks || []; });
        var results = await Promise.all([youResp, appResp]);
        _lastSetupDiagnostics = {
            collected_at: new Date().toISOString(),
            page: window.location.pathname,
            as_admin: results[0],
            as_service_principal: results[1]
        };
        return _renderSetupChecks(results[0], results[1]);
    }

    document.getElementById('btnRegistrySetupCheck')?.addEventListener('click', async function () {
        var form = document.getElementById('registrySetupForm');
        if (!form.checkValidity()) { form.reportValidity(); return; }
        var btn = this;
        var label = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Checking…';
        try {
            var errors = await _runSetupChecks(form);
            showNotification(errors ? 'Some prerequisites are missing — see the fixes below.' : 'All prerequisites are in place.',
                errors ? 'warning' : 'success');
        } catch (e) {
            showNotification('Check failed: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = label;
        }
    });

    document.getElementById('btnCopySetupDiagnostics')?.addEventListener('click', async function () {
        if (!_lastSetupDiagnostics) return;
        try {
            await navigator.clipboard.writeText(JSON.stringify(_lastSetupDiagnostics, null, 2));
            showNotification('Diagnostics copied to the clipboard.', 'success');
        } catch (e) {
            showNotification('Could not copy: ' + e.message, 'error');
        }
    });

    // ── First-run setup (no registry location yet, e.g. Marketplace) ──
    // POST /settings/registry/setup creates the schema and grants the app's
    // service principal as the signed-in admin, then runs Initialize.
    document.getElementById('registrySetupForm')?.addEventListener('submit', async function (ev) {
        ev.preventDefault();
        var form = this;
        if (!form.checkValidity()) {
            form.reportValidity();
            return;
        }
        var btn = document.getElementById('btnRegistrySetup');
        var label = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Setting up…';
        try {
            var resp = await fetch('/settings/registry/setup', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({
                    catalog: form.elements.catalog.value.trim(),
                    schema: form.elements.schema.value.trim()
                })
            });
            var data = await resp.json();
            if (!resp.ok || data.success === false) {
                showNotification('Registry setup failed: ' + (data.message || data.error || ('HTTP ' + resp.status)), 'error');
                // Show which prerequisite is missing, without making the admin ask.
                try { await _runSetupChecks(form); } catch (_) { /* the notification already reported the failure */ }
                return;
            }
            var setup = data.setup || {};
            _renderGrantResult({
                granted: (setup.granted || []).concat((data.permissions || {}).granted || []),
                warnings: (setup.warnings || []).concat((data.permissions || {}).warnings || [])
            });
            showNotification(
                (setup.warnings || []).length
                    ? 'Registry set up, with warnings — review Permission Grants below.'
                    : 'Registry set up and initialized.',
                (setup.warnings || []).length ? 'warning' : 'success'
            );
            if (typeof window.reloadRegistryConfig === 'function') window.reloadRegistryConfig();
        } catch (e) {
            showNotification('Registry setup failed: ' + e.message, 'error');
        } finally {
            btn.disabled = false;
            btn.innerHTML = label;
        }
    });

    document.getElementById('btnRepairRegistryPerms')?.addEventListener('click', async function () {
        var btn = this;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Repairing…';
        try {
            var resp = await fetch('/settings/registry/grant-permissions', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin'
            });
            var data = await resp.json();
            if (!resp.ok) {
                var errMsg = data.message || data.error || ('HTTP ' + resp.status);
                if (typeof showNotification === 'function') {
                    showNotification('Permission repair failed: ' + errMsg, 'danger');
                }
                return;
            }
            _renderGrantResult(data);
            if (typeof showNotification === 'function') {
                if (data.success === false) {
                    showNotification('Permission repair failed: ' + (data.error || data.message || 'unknown error'), 'danger');
                } else if ((data.warnings || []).length) {
                    showNotification('Permissions applied with warnings — review the details below.', 'warning');
                } else {
                    showNotification('Lakebase permissions re-applied to the app service principals.', 'success');
                }
            }
        } catch (e) {
            if (typeof showNotification === 'function')
                showNotification('Permission repair failed: ' + e.message, 'danger');
        } finally {
            btn.disabled = false;
            btn.innerHTML = '<i class="bi bi-key me-1"></i>Repair permissions';
        }
    });
})();
