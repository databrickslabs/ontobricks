/**
 * OntoBricks - settings-locks.js
 * Locks tab of the Settings page (admin only).
 *
 * Lists every active domain edit-lock across the registry (GET
 * /settings/locks) and lets an admin force-unlock a stuck one
 * (POST /settings/locks/release). Lazy-loaded on first visit to the
 * ``locks`` section (the registry query does not run on page load unless
 * Locks is the active section) and re-fetched every time the section is
 * (re-)opened, so the table always reflects the current locks.
 */
document.addEventListener('DOMContentLoaded', function () {
    const container = document.getElementById('locksTableContainer');
    if (!container) return; // panel not in the DOM (non-admin user)

    let pending = null; // {folder, version, holder} awaiting confirmation

    const modalEl = document.getElementById('lockReleaseModal');
    const modal = modalEl && window.bootstrap ? new bootstrap.Modal(modalEl) : null;

    // Reload every time the Locks section is shown so the list is never
    // a stale snapshot from an earlier visit.
    document.addEventListener('sidebarSectionChanged', (e) => {
        if (e.detail && e.detail.section === 'locks') {
            loadLocks();
        }
    });
    // Also load immediately if Locks is the active section on page load.
    if (document.getElementById('locks-section')?.classList.contains('active')) {
        loadLocks();
    }

    document.getElementById('btnRefreshLocks')?.addEventListener('click', loadLocks);

    // Force-unlock button (event delegation — no JSON in onclick).
    container.addEventListener('click', (ev) => {
        const btn = ev.target.closest('[data-action="unlock"]');
        if (!btn) return;
        pending = {
            folder: btn.getAttribute('data-folder') || '',
            version: btn.getAttribute('data-version') || '',
            holder: btn.getAttribute('data-holder') || '',
        };
        setText('lockReleaseName', pending.folder);
        setText('lockReleaseVersion', pending.version);
        setText('lockReleaseHolder', pending.holder || 'unknown');
        if (modal) modal.show();
    });

    document.getElementById('btnConfirmLockRelease')?.addEventListener('click', async () => {
        if (!pending) return;
        const { folder, version } = pending;
        try {
            const resp = await fetch('/settings/locks/release', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ folder, version }),
            });
            const data = await resp.json();
            if (data && data.success) {
                notify(
                    data.released
                        ? 'Lock released for "' + folder + '" v' + version + '.'
                        : 'No active lock found for "' + folder + '" v' + version + '.',
                    'success'
                );
            } else {
                notify('Could not release the lock.', 'error');
            }
        } catch (err) {
            notify('Could not release the lock: ' + err.message, 'error');
        } finally {
            if (modal) modal.hide();
            pending = null;
            loadLocks();
        }
    });

    async function loadLocks() {
        container.innerHTML =
            '<div class="text-center text-muted small py-4">' +
            '<span class="spinner-border spinner-border-sm me-1"></span> Loading locks…</div>';
        try {
            const resp = await fetch('/settings/locks', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!data || !data.success) {
                container.innerHTML =
                    '<div class="alert alert-danger small mb-0">' +
                    escapeHtml((data && data.error) || 'Failed to load locks') +
                    '</div>';
                return;
            }
            renderLocks(Array.isArray(data.locks) ? data.locks : []);
        } catch (err) {
            container.innerHTML =
                '<div class="alert alert-danger small mb-0">Network error: ' +
                escapeHtml(String(err)) + '</div>';
        }
    }

    function renderLocks(locks) {
        if (locks.length === 0) {
            container.innerHTML =
                '<div class="alert alert-success small mb-0">' +
                '<i class="bi bi-unlock me-1"></i> No active edit-locks — every DRAFT version is free to edit.' +
                '</div>';
            return;
        }
        const rows = locks.map((l) => {
            const folder = escapeHtml(l.folder || '');
            const version = escapeHtml(l.version || '');
            const status = escapeHtml(l.status || 'DRAFT');
            const holderName = l.holder_name || l.holder_email || 'unknown';
            const holderEmail = l.holder_email || '';
            const acquired = fmtDate(l.acquired_at);
            const holderCell =
                '<div>' + escapeHtml(holderName) + '</div>' +
                (holderEmail && holderEmail !== holderName
                    ? '<div class="text-muted small">' + escapeHtml(holderEmail) + '</div>'
                    : '');
            return (
                '<tr>' +
                '<td><i class="bi bi-folder me-1 text-secondary"></i>' + folder + '</td>' +
                '<td><code>' + version + '</code></td>' +
                '<td><span class="badge bg-secondary-subtle text-secondary-emphasis">' + status + '</span></td>' +
                '<td>' + holderCell + '</td>' +
                '<td class="text-muted small">' + escapeHtml(acquired) + '</td>' +
                '<td class="text-end">' +
                '<button type="button" class="btn btn-sm btn-outline-warning" data-action="unlock"' +
                ' data-folder="' + escapeAttr(l.folder || '') + '"' +
                ' data-version="' + escapeAttr(l.version || '') + '"' +
                ' data-holder="' + escapeAttr(holderName) + '" title="Force-unlock">' +
                '<i class="bi bi-unlock"></i></button>' +
                '</td>' +
                '</tr>'
            );
        });
        container.innerHTML =
            '<div class="table-responsive">' +
            '<table class="table table-sm table-hover align-middle mb-0">' +
            '<thead class="table-light"><tr>' +
            '<th style="width: 26%;">Domain</th>' +
            '<th style="width: 10%;">Version</th>' +
            '<th style="width: 12%;">Status</th>' +
            '<th style="width: 26%;">Held by</th>' +
            '<th style="width: 18%;">Acquired</th>' +
            '<th style="width: 8%;" class="text-end">Action</th>' +
            '</tr></thead>' +
            '<tbody>' + rows.join('') + '</tbody>' +
            '</table></div>';
    }

    function fmtDate(iso) {
        if (!iso) return '';
        const d = new Date(iso);
        return isNaN(d.getTime()) ? String(iso) : d.toLocaleString();
    }

    function setText(id, val) {
        const el = document.getElementById(id);
        if (el) el.textContent = val == null ? '' : String(val);
    }

    function notify(msg, kind) {
        if (typeof window.showNotification === 'function') {
            window.showNotification(msg, kind || 'info');
        }
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
});
