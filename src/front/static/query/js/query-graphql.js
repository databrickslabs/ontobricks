/**
 * OntoBricks – query-graphql.js
 *
 * Manages the embedded GraphiQL playground inside the Digital Twin
 * "GraphQL" sidebar section.  Relies on the same readiness gating
 * (sync-requires-ready) as the Triples and Knowledge Graph sections.
 */

/* global React, ReactDOM, GraphiQL */

const GraphQLPlayground = (() => {
    let _mounted = false;
    let _projectName = null;
    let _root = null;
    let _resizeHandler = null;
    let _libsRequested = false;
    let _depthDefault = 2;
    let _depthMax = 5;

    function _loadScript(src) {
        return new Promise(function (resolve, reject) {
            var s = document.createElement('script');
            s.src = src;
            s.crossOrigin = 'anonymous';
            s.onload = resolve;
            s.onerror = function () { reject(new Error('Failed to load ' + src)); };
            document.head.appendChild(s);
        });
    }

    async function _loadLibs() {
        if (_libsRequested) return;
        _libsRequested = true;

        var css = document.createElement('link');
        css.rel = 'stylesheet';
        css.href = 'https://unpkg.com/graphiql@3/graphiql.min.css';
        document.head.appendChild(css);

        await _loadScript('https://unpkg.com/react@18/umd/react.production.min.js');
        await _loadScript('https://unpkg.com/react-dom@18/umd/react-dom.production.min.js');
        await _loadScript('https://unpkg.com/graphiql@3/graphiql.min.js');
    }

    async function _resolveProjectName() {
        try {
            const resp = await fetch('/project/info', { credentials: 'same-origin' });
            const data = await resp.json();
            if (!data.success) return '';
            if (data.project_folder) return data.project_folder;
            const name = data?.info?.name || '';
            return name ? name.toLowerCase().replace(/\s+/g, '_') : '';
        } catch {
            return '';
        }
    }

    function _el(id) { return document.getElementById(id); }

    function _show(id) { const e = _el(id); if (e) e.style.display = ''; }
    function _hide(id) { const e = _el(id); if (e) e.style.display = 'none'; }

    function _hideAll() {
        ['graphqlLoading', 'graphqlError', 'graphiql-container'].forEach(_hide);
    }

    function _sizeContainer() {
        // Height is handled by CSS flex (flex:1 + min-height:0) — no JS needed.
    }

    async function init() {
        if (_mounted) return;

        const container = _el('graphiql-container');
        if (!container) return;

        _hideAll();
        _show('graphqlLoading');

        try {
            await _loadLibs();
        } catch (loadErr) {
            _hideAll();
            const msg = _el('graphqlErrorMsg');
            if (msg) msg.textContent =
                'GraphiQL library failed to load: ' + loadErr.message;
            _show('graphqlError');
            return;
        }
        if (typeof GraphiQL === 'undefined' || typeof ReactDOM.createRoot !== 'function') {
            _hideAll();
            const msg = _el('graphqlErrorMsg');
            if (msg) msg.textContent =
                'GraphiQL library failed to load. Check your network connection and reload.';
            _show('graphqlError');
            return;
        }

        _projectName = await _resolveProjectName();
        if (!_projectName) {
            _hideAll();
            const msg = _el('graphqlErrorMsg');
            if (msg) msg.textContent =
                'Could not resolve the project name. Make sure a project is loaded.';
            _show('graphqlError');
            return;
        }

        _el('graphqlProjectBadge').textContent = _projectName;
        _show('graphqlProjectBadge');
        const openBtn = _el('graphqlOpenNewTab');
        _show('graphqlOpenNewTab');
        if (openBtn) {
            openBtn.onclick = () => {
                window.open('/graphql/' + encodeURIComponent(_projectName), '_blank');
            };
        }

        try {
            const schemaResp = await fetch(
                '/graphql/' + encodeURIComponent(_projectName) + '/schema',
                { credentials: 'same-origin' }
            );
            if (!schemaResp.ok) {
                const errData = await schemaResp.json().catch(() => ({}));
                throw new Error(errData.detail || `HTTP ${schemaResp.status}`);
            }
        } catch (err) {
            _hideAll();
            const msg = _el('graphqlErrorMsg');
            if (msg) msg.textContent =
                'Could not load GraphQL schema: ' + err.message;
            _show('graphqlError');
            return;
        }

        _hideAll();
        _show('graphiql-container');
        _sizeContainer();

        try {
            const depthResp = await fetch('/graphql/settings/depth', { credentials: 'same-origin' });
            if (depthResp.ok) {
                const depthData = await depthResp.json();
                _depthDefault = depthData.default || 2;
                _depthMax = depthData.max || 5;
            }
        } catch { /* keep defaults */ }

        var depthSelect = _el('graphqlDepthSelect');
        if (depthSelect) {
            depthSelect.innerHTML = '';
            for (var d = 1; d <= _depthMax; d++) {
                var opt = document.createElement('option');
                opt.value = d;
                opt.textContent = d;
                if (d === _depthDefault) opt.selected = true;
                depthSelect.appendChild(opt);
            }
            _show('graphqlDepthControl');
        }

        const endpointUrl = window.location.origin +
            '/graphql/' + encodeURIComponent(_projectName);

        function depthFetcher(graphQLParams, fetcherOpts) {
            var depth = parseInt((_el('graphqlDepthSelect') || {}).value || _depthDefault, 10);
            var body = Object.assign({}, graphQLParams, { depth: depth });
            return fetch(endpointUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify(body),
            }).then(function (r) { return r.json(); });
        }

        _root = ReactDOM.createRoot(container);
        _root.render(
            React.createElement(GraphiQL, {
                fetcher: depthFetcher,
                defaultEditorToolsVisibility: true,
            })
        );

        _resizeHandler = () => _sizeContainer();
        window.addEventListener('resize', _resizeHandler);

        _mounted = true;
    }

    function reset() {
        if (_root) {
            _root.unmount();
            _root = null;
        }
        if (_resizeHandler) {
            window.removeEventListener('resize', _resizeHandler);
            _resizeHandler = null;
        }
        _mounted = false;
        _projectName = null;
    }

    return { init, reset };
})();
