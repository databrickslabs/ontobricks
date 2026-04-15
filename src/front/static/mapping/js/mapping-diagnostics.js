/**
 * Mapping Diagnostics – calls GET /mapping/diagnostics and renders results.
 */
(function () {
    'use strict';

    var _STATUS_ICON = {
        ok:      '<i class="bi bi-check-circle-fill text-success"></i>',
        warning: '<i class="bi bi-exclamation-triangle-fill text-warning"></i>',
        error:   '<i class="bi bi-x-circle-fill text-danger"></i>'
    };

    function _icon(status) {
        return _STATUS_ICON[status] || _STATUS_ICON.ok;
    }

    function _escapeHtml(str) {
        if (!str) return '';
        var div = document.createElement('div');
        div.appendChild(document.createTextNode(str));
        return div.innerHTML;
    }

    function _safeId(prefix, value) {
        return prefix + (value || '' + Math.random()).replace(/[^a-zA-Z0-9_-]/g, '_');
    }

    function _renderChecks(checks) {
        if (!checks || !checks.length) return '';
        var rows = checks.map(function (c) {
            return '<tr class="diag-check-row diag-check-' + c.status + '">' +
                '<td class="diag-check-icon">' + _icon(c.status) + '</td>' +
                '<td class="diag-check-name">' + _escapeHtml(c.check) + '</td>' +
                '<td class="diag-check-detail">' + _escapeHtml(c.detail) + '</td>' +
                '</tr>';
        });
        return '<table class="diag-checks-table">' + rows.join('') + '</table>';
    }

    function _renderEntityRow(ent) {
        var id = _safeId('diag-ent-', ent.ontology_class);
        var colsStr = ent.available_columns ? ent.available_columns.join(', ') : '—';
        var sourceDisplay = ent.source || '—';
        if (sourceDisplay.length > 120) sourceDisplay = sourceDisplay.substring(0, 120) + '…';

        return '<div class="diag-item diag-item-' + ent.status + '">' +
            '<div class="diag-item-header" data-bs-toggle="collapse" data-bs-target="#' + id + '">' +
                '<span class="diag-item-status">' + _icon(ent.status) + '</span>' +
                '<span class="diag-item-label">' + _escapeHtml(ent.label) + '</span>' +
                '<span class="diag-item-meta text-muted small ms-3">' + _escapeHtml(sourceDisplay) + '</span>' +
                '<i class="bi bi-chevron-down diag-chevron ms-auto"></i>' +
            '</div>' +
            '<div id="' + id + '" class="collapse diag-item-body">' +
                '<div class="diag-detail-row"><strong>Class URI:</strong> ' + _escapeHtml(ent.ontology_class) + '</div>' +
                '<div class="diag-detail-row"><strong>Source:</strong> <code>' + _escapeHtml(ent.source) + '</code></div>' +
                '<div class="diag-detail-row"><strong>Available columns:</strong> ' + _escapeHtml(colsStr) + '</div>' +
                _renderChecks(ent.checks) +
            '</div>' +
        '</div>';
    }

    function _renderRelRow(rel) {
        var id = _safeId('diag-rel-', rel.property);

        return '<div class="diag-item diag-item-' + rel.status + '">' +
            '<div class="diag-item-header" data-bs-toggle="collapse" data-bs-target="#' + id + '">' +
                '<span class="diag-item-status">' + _icon(rel.status) + '</span>' +
                '<span class="diag-item-label">' + _escapeHtml(rel.label) + '</span>' +
                '<span class="diag-item-meta text-muted small ms-3">' +
                    _escapeHtml(rel.source_class || '') + ' → ' + _escapeHtml(rel.target_class || '') +
                '</span>' +
                '<i class="bi bi-chevron-down diag-chevron ms-auto"></i>' +
            '</div>' +
            '<div id="' + id + '" class="collapse diag-item-body">' +
                '<div class="diag-detail-row"><strong>Property URI:</strong> ' + _escapeHtml(rel.property) + '</div>' +
                '<div class="diag-detail-row"><strong>Source:</strong> ' + _escapeHtml(rel.source_class) +
                    ' &rarr; <strong>Target:</strong> ' + _escapeHtml(rel.target_class) + '</div>' +
                _renderChecks(rel.checks) +
            '</div>' +
        '</div>';
    }

    function _showSection(id, visible) {
        var el = document.getElementById(id);
        if (el) el.classList.toggle('d-none', !visible);
    }

    async function runDiagnostics() {
        _showSection('diagEmptyState', false);
        _showSection('diagResults', false);
        _showSection('diagSummaryBar', false);
        _showSection('diagLoading', true);

        try {
            var resp = await fetch('/mapping/diagnostics');
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            var data = await resp.json();

            if (!data.success) throw new Error(data.message || 'Diagnostics failed');

            var summary = data.summary || {};
            document.getElementById('diagTotalCount').textContent = summary.total || 0;
            document.getElementById('diagOkCount').textContent = summary.ok || 0;
            document.getElementById('diagWarningCount').textContent = summary.warnings || 0;
            document.getElementById('diagErrorCount').textContent = summary.errors || 0;

            var entHtml = (data.entities || []).map(_renderEntityRow).join('');
            document.getElementById('diagEntitiesBody').innerHTML = entHtml || '<p class="text-muted p-3">No entity mappings found.</p>';
            document.getElementById('diagEntityBadge').textContent = (data.entities || []).length;

            var relHtml = (data.relationships || []).map(_renderRelRow).join('');
            document.getElementById('diagRelationshipsBody').innerHTML = relHtml || '<p class="text-muted p-3">No relationship mappings found.</p>';
            document.getElementById('diagRelBadge').textContent = (data.relationships || []).length;

            _showSection('diagSummaryBar', true);
            _showSection('diagResults', true);

        } catch (err) {
            console.error('Diagnostics error:', err);
            document.getElementById('diagEntitiesBody').innerHTML =
                '<div class="alert alert-danger m-3">' + _escapeHtml(err.message) + '</div>';
            _showSection('diagResults', true);
        } finally {
            _showSection('diagLoading', false);
        }
    }

    document.addEventListener('DOMContentLoaded', function () {
        var btn = document.getElementById('runDiagnosticsBtn');
        if (btn) btn.addEventListener('click', runDiagnostics);
    });
})();
