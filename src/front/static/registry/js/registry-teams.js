/**
 * Registry → Teams
 *
 * Matrix UI that assigns per-domain roles to Databricks App principals.
 * Rows = users/groups, columns = domains, cells = role dropdown.
 * A single Save button commits every dirty cell in one POST.
 */
(function () {
    'use strict';

    let matrixLoaded = false;
    let state = {
        domains: [],
        principals: [],
        original: new Map(),
    };
    const dirty = new Map();

    document.addEventListener('DOMContentLoaded', init);

    function init() {
        document.addEventListener('sidebarSectionChanged', (e) => {
            if (e.detail?.section === 'teams' && !matrixLoaded) {
                loadMatrix();
            }
        });

        const urlSection = new URLSearchParams(window.location.search).get('section');
        if (urlSection === 'teams') {
            loadMatrix();
        }

        document.getElementById('btnRefreshTeams')?.addEventListener('click', () => {
            if (dirty.size > 0 && !confirm(
                'You have unsaved changes. Reload and discard them?'
            )) {
                return;
            }
            matrixLoaded = false;
            loadMatrix();
        });

        document.getElementById('btnSaveTeams')?.addEventListener('click', saveChanges);

        document.getElementById('teamsPrincipalSearch')?.addEventListener(
            'input',
            (e) => applyPrincipalFilter(e.target.value || '')
        );
        document.getElementById('teamsDomainSearch')?.addEventListener(
            'input',
            (e) => applyDomainFilter(e.target.value || '')
        );

        window.addEventListener('beforeunload', (e) => {
            if (dirty.size === 0) return;
            e.preventDefault();
            e.returnValue = '';
        });
    }

    async function loadMatrix() {
        const container = document.getElementById('teamsMatrixContainer');
        if (!container) return;

        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span>' +
            ' Loading teams matrix...</div>';

        dirty.clear();
        updateDirtyBadge();

        try {
            const resp = await fetch('/settings/teams', {
                credentials: 'same-origin',
            });
            const data = await resp.json();

            if (!resp.ok || !data.success) {
                container.innerHTML =
                    '<div class="alert alert-danger small mb-0">' +
                    escapeHtml(data.error || 'Failed to load teams matrix') +
                    '</div>';
                return;
            }

            state.domains = Array.isArray(data.domains) ? data.domains : [];
            state.principals = Array.isArray(data.principals) ? data.principals : [];

            state.original = new Map();
            const assignments = data.assignments || {};
            for (const domain of Object.keys(assignments)) {
                for (const principal of Object.keys(assignments[domain])) {
                    state.original.set(
                        cellKey(principal, domain),
                        assignments[domain][principal] || ''
                    );
                }
            }

            renderMatrix();
            matrixLoaded = true;
        } catch (err) {
            console.error('loadMatrix error:', err);
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">Network error: ' +
                escapeHtml(String(err)) + '</div>';
        }
    }

    function renderMatrix() {
        const container = document.getElementById('teamsMatrixContainer');
        if (!container) return;

        if (state.domains.length === 0) {
            container.innerHTML =
                '<div class="empty-state">' +
                '<i class="bi bi-folder-x me-1"></i>' +
                ' No domains found in the registry.' +
                '</div>';
            return;
        }
        if (state.principals.length === 0) {
            container.innerHTML =
                '<div class="empty-state">' +
                '<i class="bi bi-person-x me-1"></i>' +
                ' No Databricks App principals found.' +
                ' Grant access in the Databricks App permissions page.' +
                '</div>';
            return;
        }

        const thead = ['<thead><tr>',
            '<th class="principal-col">Principal</th>'];
        for (const domain of state.domains) {
            thead.push(
                '<th data-domain="' + escapeAttr(domain) + '" ' +
                'title="' + escapeAttr(domain) + '">' +
                escapeHtml(domain) +
                '</th>'
            );
        }
        thead.push('</tr></thead>');

        const tbody = ['<tbody>'];
        for (const p of state.principals) {
            const principal = p.principal || '';
            const display = p.display_name || principal;
            const type = p.principal_type || 'user';
            const icon = type === 'group' ? 'bi-people' : 'bi-person';
            const typeClass = type === 'group'
                ? 'bg-info-subtle text-info-emphasis'
                : 'bg-secondary-subtle text-secondary-emphasis';

            tbody.push(
                '<tr data-principal="' + escapeAttr(principal) + '" ' +
                'data-type="' + escapeAttr(type) + '" ' +
                'data-display="' + escapeAttr(display) + '">' +
                '<th class="principal-cell">' +
                '<div class="principal-meta">' +
                '<span class="principal-display">' +
                '<i class="bi ' + icon + ' me-1"></i>' +
                escapeHtml(display) +
                ' <span class="badge principal-type-badge ' + typeClass + '">' +
                type + '</span>' +
                '</span>' +
                (principal !== display
                    ? '<span class="principal-id">' + escapeHtml(principal) + '</span>'
                    : '') +
                '</div></th>'
            );
            for (const domain of state.domains) {
                const key = cellKey(principal, domain);
                const role = state.original.get(key) || '';
                tbody.push(renderCell(principal, type, display, domain, role));
            }
            tbody.push('</tr>');
        }
        tbody.push('</tbody>');

        container.innerHTML =
            '<table class="teams-matrix">' +
            thead.join('') + tbody.join('') +
            '</table>';

        container.querySelectorAll('select.cell-role').forEach((sel) => {
            sel.addEventListener('change', onCellChange);
        });
    }

    function renderCell(principal, type, display, domain, role) {
        const opts = [
            '<option value=""' + (role === '' ? ' selected' : '') + '>&mdash;</option>',
            '<option value="viewer"' + (role === 'viewer' ? ' selected' : '') + '>V</option>',
            '<option value="editor"' + (role === 'editor' ? ' selected' : '') + '>E</option>',
            '<option value="builder"' + (role === 'builder' ? ' selected' : '') + '>B</option>',
        ];
        const title = roleTitle(role);
        return '<td data-domain="' + escapeAttr(domain) + '" ' +
            'data-principal="' + escapeAttr(principal) + '">' +
            '<select class="cell-role" ' +
            'data-principal="' + escapeAttr(principal) + '" ' +
            'data-type="' + escapeAttr(type) + '" ' +
            'data-display="' + escapeAttr(display) + '" ' +
            'data-domain="' + escapeAttr(domain) + '" ' +
            'data-role="' + escapeAttr(role) + '" ' +
            'title="' + escapeAttr(title) + '">' +
            opts.join('') +
            '</select></td>';
    }

    function roleTitle(role) {
        switch (role) {
            case 'viewer': return 'Viewer – read-only';
            case 'editor': return 'Editor – can edit';
            case 'builder': return 'Builder – can build';
            default: return 'No access';
        }
    }

    function onCellChange(e) {
        const sel = e.target;
        const principal = sel.dataset.principal;
        const type = sel.dataset.type;
        const display = sel.dataset.display || principal;
        const domain = sel.dataset.domain;
        const newRole = sel.value || '';
        const key = cellKey(principal, domain);
        const originalRole = state.original.get(key) || '';

        sel.dataset.role = newRole;
        sel.title = roleTitle(newRole);

        const td = sel.closest('td');

        if (newRole === originalRole) {
            dirty.delete(key);
            td?.classList.remove('is-dirty');
        } else {
            dirty.set(key, {
                domain_folder: domain,
                principal,
                principal_type: type,
                display_name: display,
                role: newRole === '' ? null : newRole,
            });
            td?.classList.add('is-dirty');
        }

        updateDirtyBadge();
    }

    function updateDirtyBadge() {
        const btn = document.getElementById('btnSaveTeams');
        const badge = document.getElementById('teamsDirtyBadge');
        if (!btn || !badge) return;
        if (dirty.size === 0) {
            btn.disabled = true;
            badge.textContent = '0';
            badge.classList.add('d-none');
        } else {
            btn.disabled = false;
            badge.textContent = String(dirty.size);
            badge.classList.remove('d-none');
        }
    }

    async function saveChanges() {
        if (dirty.size === 0) return;
        const btn = document.getElementById('btnSaveTeams');
        const status = document.getElementById('teamsStatus');
        if (!btn) return;

        const changes = Array.from(dirty.values());
        btn.disabled = true;
        const originalInner = btn.innerHTML;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span> Saving...';

        try {
            const resp = await fetch('/settings/teams', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ changes }),
            });
            const data = await resp.json();

            if (!resp.ok) {
                showStatus('danger',
                    'Save failed: ' + escapeHtml(data.error || resp.statusText));
                btn.innerHTML = originalInner;
                btn.disabled = false;
                return;
            }

            const savedDomains = new Set((data.saved || []).map((s) => s.domain));
            const failedDomains = new Set((data.failed || []).map((f) => f.domain));

            for (const [key, change] of Array.from(dirty.entries())) {
                if (savedDomains.has(change.domain_folder) &&
                    !failedDomains.has(change.domain_folder)) {
                    const roleStr = change.role === null ? '' : change.role;
                    if (roleStr === '') {
                        state.original.delete(key);
                    } else {
                        state.original.set(key, roleStr);
                    }
                    dirty.delete(key);
                    const td = document.querySelector(
                        'td[data-principal="' + cssEscape(change.principal) + '"][data-domain="' + cssEscape(change.domain_folder) + '"]'
                    );
                    td?.classList.remove('is-dirty');
                }
            }

            updateDirtyBadge();

            if (data.success && (!data.failed || data.failed.length === 0)) {
                showStatus('success',
                    '<i class="bi bi-check-circle me-1"></i>Saved ' +
                    data.total_changes + ' change(s).');
            } else {
                const msg = (data.failed || []).map((f) =>
                    escapeHtml(f.domain) + ': ' + escapeHtml(f.message)
                ).join('<br>');
                showStatus('warning',
                    '<i class="bi bi-exclamation-triangle me-1"></i>' +
                    'Partial save. Failures:<br>' + msg);
            }
        } catch (err) {
            console.error('saveChanges error:', err);
            showStatus('danger', 'Network error: ' + escapeHtml(String(err)));
        } finally {
            btn.innerHTML = originalInner;
            updateDirtyBadge();
        }
    }

    function showStatus(kind, html) {
        const el = document.getElementById('teamsStatus');
        if (!el) return;
        el.style.display = '';
        el.className = 'alert alert-' + kind + ' small mb-3';
        el.innerHTML = html;
        if (kind === 'success') {
            setTimeout(() => { el.style.display = 'none'; }, 4000);
        }
    }

    function applyPrincipalFilter(q) {
        const term = q.trim().toLowerCase();
        document.querySelectorAll('.teams-matrix tbody tr').forEach((tr) => {
            if (!term) {
                tr.classList.remove('row-hidden');
                return;
            }
            const p = (tr.dataset.principal || '').toLowerCase();
            const d = (tr.dataset.display || '').toLowerCase();
            tr.classList.toggle('row-hidden',
                !p.includes(term) && !d.includes(term));
        });
    }

    function applyDomainFilter(q) {
        const term = q.trim().toLowerCase();
        const table = document.querySelector('.teams-matrix');
        if (!table) return;
        table.querySelectorAll('thead th[data-domain]').forEach((th) => {
            const d = (th.dataset.domain || '').toLowerCase();
            th.classList.toggle('col-hidden', term && !d.includes(term));
        });
        table.querySelectorAll('tbody td[data-domain]').forEach((td) => {
            const d = (td.dataset.domain || '').toLowerCase();
            td.classList.toggle('col-hidden', term && !d.includes(term));
        });
    }

    function cellKey(principal, domain) {
        return principal + '|' + domain;
    }

    function escapeHtml(text) {
        if (text == null) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    function escapeAttr(text) {
        return escapeHtml(text).replace(/"/g, '&quot;');
    }

    function cssEscape(text) {
        if (window.CSS && typeof CSS.escape === 'function') {
            return CSS.escape(String(text));
        }
        return String(text).replace(/([\0-\x1f\x7f]|^-?\d|[\s"'\\^])/g, '\\$1');
    }
})();
