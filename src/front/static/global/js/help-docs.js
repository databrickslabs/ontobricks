/**
 * OntoBricks - Help Center Documentation controller
 *
 * Renders /docs/*.md inside the Help Center modal. Fetches the index once,
 * then fetches and renders each document on demand using marked.js.
 *
 * Exposes window.HelpDocs = { activate(slug?) } which help-modal.js calls
 * when the "Documentation" section is activated (or via deep-link hash
 * #help-docs/{slug}).
 */
(function () {
    'use strict';

    var API_LIST = '/api/help/docs';
    var API_DOC = '/api/help/docs/';
    var IMG_PREFIX = '/api/help/docs/images/';
    var GITHUB_BASE =
        'https://github.com/databrickslabs/ontobricks/blob/main/docs/';

    var state = {
        loadedIndex: false,
        loadingIndex: false,
        docs: {},           // slug -> {slug, title, category}
        categories: [],      // [{id,label,docs:[{slug,title}]}]
        currentSlug: null,
        pendingSlug: null,  // slug to activate once the index is ready
    };

    // ── DOM helpers ──────────────────────────────────────────────────────────

    function $(id) {
        return document.getElementById(id);
    }

    function _setBody(html) {
        var body = $('helpDocsBody');
        if (body) body.innerHTML = html;
    }

    function _setTitle(title, file) {
        var t = $('helpDocsTitle');
        if (t) t.textContent = title || 'Documentation';
        var meta = $('helpDocsMeta');
        if (meta) meta.textContent = file ? 'docs/' + file : '';
        var gh = $('helpDocsGithub');
        if (gh) {
            if (file) {
                gh.href = GITHUB_BASE + file;
                gh.classList.remove('d-none');
            } else {
                gh.classList.add('d-none');
            }
        }
    }

    function _showPlaceholder() {
        _setTitle('Select a document', null);
        _setBody(
            '<div class="help-docs-placeholder text-center text-muted py-5">' +
                '<i class="bi bi-journal-text fs-1 d-block mb-3"></i>' +
                '<h5>Browse the full documentation</h5>' +
                '<p class="mb-0">Pick a document on the left to start reading.</p>' +
            '</div>'
        );
    }

    function _showLoading(title) {
        _setTitle(title || 'Loading...', null);
        _setBody(
            '<div class="help-docs-placeholder text-center text-muted py-5">' +
                '<span class="spinner-border me-2" role="status" aria-hidden="true"></span>' +
                'Loading...' +
            '</div>'
        );
    }

    function _showError(msg) {
        _setBody(
            '<div class="alert alert-warning m-3">' +
                '<i class="bi bi-exclamation-triangle me-2"></i>' +
                (msg || 'Could not load this document.') +
            '</div>'
        );
    }

    // ── Sidebar index ────────────────────────────────────────────────────────

    function _renderIndex() {
        var nav = $('helpDocsNav');
        if (!nav) return;
        if (!state.categories.length) {
            nav.innerHTML =
                '<div class="text-muted small p-2">No documentation available.</div>';
            return;
        }
        var html = '';
        state.categories.forEach(function (cat) {
            html += '<div class="help-docs-cat">';
            html += '<div class="help-docs-cat-label">' + _esc(cat.label) + '</div>';
            html += '<ul class="help-docs-cat-list">';
            cat.docs.forEach(function (doc) {
                html +=
                    '<li><a href="#help-docs/' +
                    encodeURIComponent(doc.slug) +
                    '" class="help-docs-link" data-doc-slug="' +
                    _esc(doc.slug) +
                    '">' +
                    _esc(doc.title) +
                    '</a></li>';
            });
            html += '</ul></div>';
        });
        nav.innerHTML = html;
    }

    function _markActiveLink(slug) {
        var links = document.querySelectorAll('#helpDocsNav .help-docs-link');
        links.forEach(function (a) {
            a.classList.toggle(
                'active',
                a.getAttribute('data-doc-slug') === slug
            );
        });
    }

    // ── Data fetching ────────────────────────────────────────────────────────

    function _loadIndex() {
        if (state.loadedIndex || state.loadingIndex) return;
        state.loadingIndex = true;
        fetch(API_LIST, { credentials: 'same-origin' })
            .then(function (r) {
                if (!r.ok) throw new Error('HTTP ' + r.status);
                return r.json();
            })
            .then(function (data) {
                state.categories = (data && data.categories) || [];
                state.docs = {};
                state.categories.forEach(function (cat) {
                    cat.docs.forEach(function (d) {
                        state.docs[d.slug] = {
                            slug: d.slug,
                            title: d.title,
                            category: cat.id,
                        };
                    });
                });
                state.loadedIndex = true;
                state.loadingIndex = false;
                _renderIndex();
                if (state.pendingSlug) {
                    var s = state.pendingSlug;
                    state.pendingSlug = null;
                    _loadDoc(s);
                }
            })
            .catch(function (err) {
                state.loadingIndex = false;
                var nav = $('helpDocsNav');
                if (nav) {
                    nav.innerHTML =
                        '<div class="alert alert-warning small m-2">' +
                        'Could not load documentation index: ' +
                        _esc(err.message || String(err)) +
                        '</div>';
                }
            });
    }

    function _loadDoc(slug) {
        if (!slug) return;
        if (!state.loadedIndex) {
            state.pendingSlug = slug;
            _loadIndex();
            return;
        }
        if (!state.docs[slug]) {
            _showError('Unknown document: ' + slug);
            return;
        }
        var doc = state.docs[slug];
        state.currentSlug = slug;
        _markActiveLink(slug);
        _showLoading(doc.title);

        fetch(API_DOC + encodeURIComponent(slug), { credentials: 'same-origin' })
            .then(function (r) {
                if (!r.ok) throw new Error('HTTP ' + r.status);
                return r.json();
            })
            .then(function (payload) {
                _renderMarkdown(payload);
                // Update the hash so the doc is deep-linkable
                try {
                    var url =
                        window.location.pathname +
                        window.location.search +
                        '#help-docs/' +
                        encodeURIComponent(slug);
                    window.history.replaceState(null, '', url);
                } catch (e) {
                    /* ignore */
                }
            })
            .catch(function (err) {
                _showError(
                    'Failed to load "' + doc.title + '" (' + err.message + ')'
                );
            });
    }

    // ── Rendering ────────────────────────────────────────────────────────────

    function _renderMarkdown(payload) {
        var body = $('helpDocsBody');
        if (!body) return;
        var md = (payload && payload.markdown) || '';
        var title = (payload && payload.title) || state.docs[payload.slug]?.title || '';
        var file = (payload && payload.file) || '';
        _setTitle(title, file);

        if (!window.marked || typeof window.marked.parse !== 'function') {
            _setBody(
                '<pre class="help-docs-raw">' + _esc(md) + '</pre>'
            );
            return;
        }
        try {
            window.marked.setOptions({
                gfm: true,
                breaks: false,
                headerIds: true,
                mangle: false,
            });
        } catch (e) {
            /* older marked versions */
        }
        var html = window.marked.parse(md);
        body.innerHTML = html;
        _postProcess(body);
        body.scrollTop = 0;
    }

    /**
     * Rewrite <img src="images/..."> to the /api/help/docs/images/ endpoint
     * and <a href="*.md"> to the in-app viewer. External links get
     * target=_blank.
     */
    function _postProcess(root) {
        var imgs = root.querySelectorAll('img');
        imgs.forEach(function (img) {
            var src = img.getAttribute('src') || '';
            if (/^https?:\/\//i.test(src) || src.startsWith('data:')) return;
            // Strip a leading "./" or "docs/"
            src = src.replace(/^\.\//, '').replace(/^docs\//, '');
            if (src.startsWith('images/')) {
                img.setAttribute(
                    'src',
                    IMG_PREFIX + src.slice('images/'.length)
                );
            }
            img.setAttribute('loading', 'lazy');
        });

        var links = root.querySelectorAll('a[href]');
        links.forEach(function (a) {
            var href = a.getAttribute('href') || '';
            if (!href) return;
            if (href.startsWith('#')) {
                // Intra-document anchor (marked generates header ids) — no-op
                return;
            }
            if (/^https?:\/\//i.test(href) || href.startsWith('mailto:')) {
                a.setAttribute('target', '_blank');
                a.setAttribute('rel', 'noopener');
                return;
            }
            // Relative links to other docs (*.md or ./*.md#anchor)
            var cleaned = href.replace(/^\.\//, '').replace(/^docs\//, '');
            var mdMatch = cleaned.match(/^([A-Za-z0-9_.-]+)\.md(#.+)?$/);
            if (mdMatch) {
                var file = mdMatch[1] + '.md';
                var slug = _slugForFile(file);
                if (slug) {
                    a.setAttribute('href', '#help-docs/' + slug + (mdMatch[2] || ''));
                    a.setAttribute('data-doc-slug', slug);
                    a.classList.add('help-docs-internal');
                    return;
                }
            }
            // Fallback: open in new tab
            a.setAttribute('target', '_blank');
            a.setAttribute('rel', 'noopener');
        });

        // Intercept clicks on internal doc links to activate the viewer
        // without a full navigation.
        root.addEventListener(
            'click',
            function (e) {
                var a = e.target.closest('a.help-docs-internal');
                if (!a) return;
                var slug = a.getAttribute('data-doc-slug');
                if (!slug) return;
                e.preventDefault();
                _loadDoc(slug);
            },
            { once: false }
        );
    }

    function _slugForFile(filename) {
        for (var slug in state.docs) {
            if (!Object.prototype.hasOwnProperty.call(state.docs, slug)) continue;
            // The index only stores title/category — check against categories
            // which still hold the original file info embedded in the slug.
            // We instead compare via a simple heuristic: slugs are derived
            // from filenames, so "README.md" → "readme", "get-started.md"
            // → "get-started", etc.
            var norm = filename.toLowerCase().replace(/\.md$/, '');
            if (norm === slug) return slug;
        }
        return null;
    }

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Activate the Documentation viewer. If called without a slug, loads
     * the index and clears the viewer back to the placeholder.
     */
    function activate(slug) {
        _loadIndex();
        if (slug) {
            _loadDoc(slug);
        } else if (!state.currentSlug) {
            _showPlaceholder();
        }
    }

    // ── Sidebar click delegation ─────────────────────────────────────────────

    function _initNavDelegation() {
        var nav = $('helpDocsNav');
        if (!nav) return;
        nav.addEventListener('click', function (e) {
            var a = e.target.closest('.help-docs-link');
            if (!a) return;
            e.preventDefault();
            var slug = a.getAttribute('data-doc-slug');
            if (slug) _loadDoc(slug);
        });
    }

    // ── Hash deep-linking ────────────────────────────────────────────────────
    // Supports URLs like "#help-docs/user-guide" — works in concert with
    // the existing help-modal state machine.

    function _slugFromHash() {
        var h = (window.location.hash || '').replace(/^#/, '');
        var m = h.match(/^help-docs\/([A-Za-z0-9_.-]+)/);
        return m ? decodeURIComponent(m[1]) : null;
    }

    function _initHashHandling() {
        window.addEventListener('hashchange', function () {
            var slug = _slugFromHash();
            if (slug) {
                if (
                    window.HelpCenter &&
                    typeof window.HelpCenter.open === 'function'
                ) {
                    window.HelpCenter.open('docs');
                }
                activate(slug);
            }
        });
        // On initial load, if the URL already contains a help-docs hash
        // and the modal happens to open (e.g. user pasted the link and
        // clicks Help), the hash listener + activate() will pick it up.
    }

    function _init() {
        _initNavDelegation();
        _initHashHandling();
        // Kick off a preload of the index as soon as the modal is first
        // shown — avoids a visible flash when the user clicks Documentation.
        var modalEl = document.getElementById('helpModal');
        if (modalEl) {
            modalEl.addEventListener(
                'shown.bs.modal',
                function () {
                    _loadIndex();
                    // If the URL hash points to a doc, honor it.
                    var slug = _slugFromHash();
                    if (slug) {
                        if (
                            window.HelpCenter &&
                            typeof window.HelpCenter.activate ===
                                'function'
                        ) {
                            window.HelpCenter.activate('docs');
                        }
                        activate(slug);
                    }
                },
                { once: false }
            );
        }
    }

    // ── util ─────────────────────────────────────────────────────────────────

    function _esc(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
            return {
                '&': '&amp;',
                '<': '&lt;',
                '>': '&gt;',
                '"': '&quot;',
                "'": '&#39;',
            }[c];
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', _init);
    } else {
        _init();
    }

    window.HelpDocs = { activate: activate };
})();
