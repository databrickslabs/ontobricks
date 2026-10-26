/**
 * OntoBricks - settings-github.js
 * GitHub section of the Settings page (admin only).
 *
 * Loads/saves the instance GitHub mirror settings (GET/POST /settings/github),
 * tests and claims the repository (POST /settings/github/test) and rewrites
 * every domain into it (POST /settings/github/sync-all). The token secret
 * scope/key dropdowns reuse the Databricks secrets listing endpoints.
 */
document.addEventListener('DOMContentLoaded', function () {
    const form = document.getElementById('githubConfigForm');
    if (!form) return; // panel not in the DOM (non-admin user)

    const $ = (id) => document.getElementById(id);
    let loaded = false;

    document.addEventListener('sidebarSectionChanged', (e) => {
        if (e.detail && e.detail.section === 'github') load();
    });
    if ($('github-section')?.classList.contains('active')) load();

    $('btnSaveGithub')?.addEventListener('click', save);
    $('btnTestGithub')?.addEventListener('click', testConnection);
    $('btnSyncAllGithub')?.addEventListener('click', syncAll);
    $('btnRefreshGithubScopes')?.addEventListener('click', () => loadScopes($('githubSecretScope').value));
    $('githubSecretScope')?.addEventListener('change', () => loadKeys($('githubSecretScope').value, ''));
    $('githubEnabled')?.addEventListener('change', syncDetailsVisibility);

    async function load() {
        try {
            const data = await fetchJson('/settings/github');
            const g = data.github || {};
            $('githubEnabled').checked = !!g.enabled;
            $('githubPushOnSave').checked = g.push_on_save !== false;
            $('githubOwner').value = g.owner || '';
            $('githubRepo').value = g.repo || '';
            $('githubBranch').value = g.branch || 'main';
            $('githubBasePath').value = g.base_path || '';
            $('githubAuthorName').value = g.author_name || '';
            $('githubAuthorEmail').value = g.author_email || '';
            renderStatus(data.status || {}, g);
            if (!loaded) {
                await loadScopes(g.token_secret_scope || '');
                await loadKeys(g.token_secret_scope || '', g.token_secret_key || '');
                loaded = true;
            }
            $('btnSaveGithub').disabled = false;
            syncDetailsVisibility();
        } catch (err) {
            showResult('danger', 'Could not load GitHub settings: ' + err.message);
        } finally {
            $('githubLoading')?.classList.add('d-none');
            $('githubLoaded')?.classList.remove('d-none');
        }
    }

    function syncDetailsVisibility() {
        $('githubDetails')?.classList.toggle('d-none', !$('githubEnabled').checked);
    }

    function payload() {
        return {
            enabled: $('githubEnabled').checked,
            push_on_save: $('githubPushOnSave').checked,
            owner: $('githubOwner').value.trim(),
            repo: $('githubRepo').value.trim(),
            branch: $('githubBranch').value.trim() || 'main',
            base_path: $('githubBasePath').value.trim(),
            token_secret_scope: $('githubSecretScope').value,
            token_secret_key: $('githubSecretKey').value,
            author_name: $('githubAuthorName').value.trim(),
            author_email: $('githubAuthorEmail').value.trim(),
        };
    }

    async function save() {
        await withBusy('btnSaveGithub', async () => {
            await fetchJson('/settings/github', { method: 'POST', body: payload() });
            notify('GitHub settings saved', 'success');
            hideResult();
            load();
        });
    }

    async function testConnection() {
        await withBusy('btnTestGithub', async () => {
            await fetchJson('/settings/github', { method: 'POST', body: payload() });
            const data = await fetchJson('/settings/github/test', {
                method: 'POST',
                body: { take_over: $('githubTakeOver').checked },
            });
            showResult('success',
                '<i class="bi bi-check-circle me-1"></i>Connected to <strong>' + esc(data.repository) +
                '</strong> (' + (data.private ? 'private' : 'public') + '), branch <code>' +
                esc(data.branch) + '</code>. Repository claimed by this instance.' +
                (mirrorOffReason() ? '<div class="text-warning mt-1"><i class="bi bi-pause-circle me-1"></i>' +
                    esc(mirrorOffReason()) + '</div>' : ''));
            $('githubTakeOver').checked = false;
            load();
        });
    }

    async function syncAll() {
        await withBusy('btnSyncAllGithub', async () => {
            const data = await fetchJson('/settings/github/sync-all', { method: 'POST', body: {} });
            const sha = data.commit_sha ? '<code>' + esc(data.commit_sha.slice(0, 7)) + '</code>' : 'no changes';
            const warn = (data.errors || []).length
                ? '<div class="text-warning mt-1">' + data.errors.map(esc).join('<br>') + '</div>'
                : '';
            showResult('success',
                '<i class="bi bi-cloud-check me-1"></i>Synced ' + data.domains + ' domain(s), ' +
                data.versions + ' version(s) — ' + sha + warn);
            load();
        });
    }

    async function loadScopes(selected) {
        const sel = $('githubSecretScope');
        try {
            const data = await fetchJson('/settings/graph-engine/neo4j-secret-scopes');
            fillSelect(sel, data.scopes || [], selected, '— Select a scope —');
        } catch (err) {
            fillSelect(sel, selected ? [selected] : [], selected, '— Select a scope —');
        }
    }

    async function loadKeys(scope, selected) {
        const sel = $('githubSecretKey');
        if (!scope) { fillSelect(sel, [], '', '— Select a key —'); return; }
        try {
            const data = await fetchJson('/settings/graph-engine/neo4j-secret-keys?scope=' + encodeURIComponent(scope));
            fillSelect(sel, data.keys || [], selected, '— Select a key —');
        } catch (err) {
            fillSelect(sel, selected ? [selected] : [], selected, '— Select a key —');
        }
    }

    function fillSelect(sel, values, selected, placeholder) {
        const opts = values.slice();
        if (selected && !opts.includes(selected)) opts.unshift(selected);
        sel.innerHTML = '<option value="">' + esc(placeholder) + '</option>' +
            opts.map(v => '<option value="' + esc(v) + '"' + (v === selected ? ' selected' : '') + '>' + esc(v) + '</option>').join('');
    }

    function mirrorOffReason() {
        if (!$('githubEnabled').checked) return 'Mirror disabled: saves are not pushed. Tick "Enable GitHub mirror" and save.';
        if (!$('githubPushOnSave').checked) return 'Push on save is off: use "Sync all" to push manually.';
        return '';
    }

    function renderStatus(s, g) {
        const el = $('githubStatus');
        if (g && (!g.enabled || g.push_on_save === false) && s.state !== 'error') {
            el.className = 'small text-warning';
            el.innerHTML = '<i class="bi bi-pause-circle me-1"></i>' + esc(mirrorOffReason());
            return;
        }
        if (!s.state || s.state === 'idle') {
            el.className = 'small text-muted';
            el.textContent = 'No push since the app started' + (s.pending ? ' (' + s.pending + ' pending)' : '');
            return;
        }
        const when = s.at ? new Date(s.at).toLocaleString() : '';
        if (s.state === 'ok') {
            el.className = 'small text-success';
            el.innerHTML = '<i class="bi bi-check-circle me-1"></i>' + esc(s.target) + ' · ' + esc(when) +
                (s.commit_sha ? ' · <code>' + esc(s.commit_sha.slice(0, 7)) + '</code>' : ' · no changes') +
                ((s.errors || []).length ? '<div class="text-warning">' + s.errors.map(esc).join('<br>') + '</div>' : '');
        } else {
            el.className = 'small text-danger';
            el.innerHTML = '<i class="bi bi-exclamation-triangle me-1"></i>' + esc(s.target) + ' · ' +
                esc(when) + ' · ' + esc(s.message);
        }
    }

    async function fetchJson(url, opts) {
        const init = { credentials: 'same-origin', method: (opts && opts.method) || 'GET' };
        if (opts && opts.body !== undefined) {
            init.headers = { 'Content-Type': 'application/json' };
            init.body = JSON.stringify(opts.body);
        }
        const resp = await fetch(url, init);
        let data = {};
        try { data = await resp.json(); } catch (_) { /* empty body */ }
        if (!resp.ok || data.success === false) {
            throw new Error(data.message || data.error || ('HTTP ' + resp.status));
        }
        return data;
    }

    async function withBusy(btnId, fn) {
        const btn = $(btnId);
        const html = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>' + btn.textContent.trim();
        try {
            await fn();
        } catch (err) {
            showResult('danger', '<i class="bi bi-x-circle me-1"></i>' + esc(err.message));
        } finally {
            btn.disabled = false;
            btn.innerHTML = html;
        }
    }

    function showResult(kind, html) {
        const el = $('githubResult');
        el.className = 'small alert alert-' + kind + ' py-2';
        el.innerHTML = html;
    }

    function hideResult() {
        $('githubResult').className = 'small d-none';
    }

    function notify(msg, kind) {
        if (typeof window.showNotification === 'function') window.showNotification(msg, kind || 'info');
    }

    function esc(text) {
        const div = document.createElement('div');
        div.textContent = text == null ? '' : String(text);
        return div.innerHTML.replace(/"/g, '&quot;');
    }
});
