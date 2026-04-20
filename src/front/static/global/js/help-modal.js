/**
 * OntoBricks - Help Center controller
 *
 * Handles the in-app Help Center modal:
 *  - Opens on click of the navbar "Help" icon (#helpCenterToggle)
 *  - Section switching from the left rail
 *  - Jump-links inside the welcome cards
 *  - Live search across all sections
 */
(function () {
    'use strict';

    var MODAL_ID = 'helpModal';
    var TOGGLE_ID = 'helpCenterToggle';

    /** Lazily obtain a Bootstrap modal instance. */
    function _getBsModal() {
        var el = document.getElementById(MODAL_ID);
        if (!el || !window.bootstrap) return null;
        return window.bootstrap.Modal.getOrCreateInstance(el);
    }

    /** Show the Help Center, optionally jumping to a given section id. */
    function openHelp(sectionId) {
        var inst = _getBsModal();
        if (!inst) return;
        inst.show();
        if (sectionId) {
            setTimeout(function () { _activateSection(sectionId); }, 30);
        }
    }

    function _activateSection(sectionId) {
        var nav = document.querySelectorAll('.help-nav .nav-link');
        var sections = document.querySelectorAll('.help-section');

        nav.forEach(function (a) {
            a.classList.toggle('active', a.getAttribute('data-help-section') === sectionId);
        });
        sections.forEach(function (s) {
            s.classList.toggle('active', s.getAttribute('data-help-section-panel') === sectionId);
        });

        var content = document.querySelector('.help-content');
        if (content) content.scrollTop = 0;
    }

    function _initSidebarNav() {
        var nav = document.querySelector('.help-nav');
        if (!nav) return;
        nav.addEventListener('click', function (e) {
            var link = e.target.closest('[data-help-section]');
            if (!link) return;
            e.preventDefault();
            var id = link.getAttribute('data-help-section');
            if (id) _activateSection(id);
        });
    }

    function _initJumpLinks() {
        var body = document.querySelector('.help-modal-body');
        if (!body) return;
        body.addEventListener('click', function (e) {
            var jump = e.target.closest('[data-help-jump]');
            if (!jump) return;
            e.preventDefault();
            var id = jump.getAttribute('data-help-jump');
            if (id) _activateSection(id);
        });
    }

    // ── Search ───────────────────────────────────────────────────────────────

    function _escapeHtml(str) {
        return str.replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
        });
    }

    function _escapeRegex(str) {
        return str.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    }

    /**
     * Rebuild the sidebar to only show sections with matches, and scroll
     * the content column through all matching sections stacked vertically.
     */
    function _applySearch(query) {
        var q = (query || '').trim();
        var sections = document.querySelectorAll('.help-section[data-help-section-panel]');
        var navLinks = document.querySelectorAll('.help-nav .nav-link[data-help-section]');
        var content = document.querySelector('.help-content');
        var empty = document.querySelector('.help-search-empty');

        // Reset previous highlights (strip any <mark class="help-search-match">)
        sections.forEach(function (s) {
            s.querySelectorAll('mark.help-search-match').forEach(function (m) {
                var parent = m.parentNode;
                parent.replaceChild(document.createTextNode(m.textContent), m);
                parent.normalize();
            });
        });

        if (!q) {
            if (content) content.removeAttribute('data-help-searching');
            sections.forEach(function (s) {
                s.classList.remove('active');
                s.style.display = '';
            });
            if (empty) empty.classList.add('d-none');
            navLinks.forEach(function (a) { a.classList.remove('help-hidden'); a.style.display = ''; });
            var firstActive = document.querySelector('.help-nav .nav-link.active');
            var firstId = firstActive ? firstActive.getAttribute('data-help-section') : 'welcome';
            _activateSection(firstId || 'welcome');
            return;
        }

        if (content) content.setAttribute('data-help-searching', '1');
        var re = new RegExp('(' + _escapeRegex(q) + ')', 'gi');
        var matchedSections = new Set();

        sections.forEach(function (section) {
            if (section.getAttribute('data-help-section-panel') === '__empty') return;
            var text = section.textContent.toLowerCase();
            if (text.indexOf(q.toLowerCase()) === -1) {
                section.style.display = 'none';
                section.classList.remove('active');
                return;
            }
            matchedSections.add(section.getAttribute('data-help-section-panel'));
            section.style.display = 'block';
            section.classList.add('active');
            // Highlight text matches within text-only nodes
            _highlightIn(section, re);
        });

        navLinks.forEach(function (a) {
            var id = a.getAttribute('data-help-section');
            var keep = matchedSections.has(id);
            a.style.display = keep ? '' : 'none';
        });

        if (empty) {
            empty.classList.toggle('d-none', matchedSections.size > 0);
            if (matchedSections.size === 0) {
                empty.style.display = 'block';
                empty.classList.add('active');
            } else {
                empty.style.display = 'none';
                empty.classList.remove('active');
            }
        }

        if (content) content.scrollTop = 0;
    }

    function _highlightIn(root, re) {
        var SKIP = { SCRIPT: 1, STYLE: 1, MARK: 1, CODE: 1, KBD: 1, PRE: 1 };
        var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
            acceptNode: function (node) {
                if (!node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
                var p = node.parentElement;
                while (p && p !== root) {
                    if (SKIP[p.tagName]) return NodeFilter.FILTER_REJECT;
                    p = p.parentElement;
                }
                return re.test(node.nodeValue) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
            }
        });
        re.lastIndex = 0;
        var nodes = [];
        var n;
        while ((n = walker.nextNode())) nodes.push(n);
        nodes.forEach(function (textNode) {
            var frag = document.createDocumentFragment();
            var parts = textNode.nodeValue.split(re);
            parts.forEach(function (part) {
                if (!part) return;
                if (re.test(part)) {
                    var mark = document.createElement('mark');
                    mark.className = 'help-search-match';
                    mark.textContent = part;
                    frag.appendChild(mark);
                } else {
                    frag.appendChild(document.createTextNode(part));
                }
                re.lastIndex = 0;
            });
            textNode.parentNode.replaceChild(frag, textNode);
        });
    }

    function _initSearch() {
        var input = document.getElementById('helpSearchInput');
        if (!input) return;
        var timer = null;
        input.addEventListener('input', function () {
            if (timer) clearTimeout(timer);
            timer = setTimeout(function () { _applySearch(input.value); }, 120);
        });
        input.addEventListener('keydown', function (e) {
            if (e.key === 'Escape') {
                input.value = '';
                _applySearch('');
            }
        });
        var modalEl = document.getElementById(MODAL_ID);
        if (modalEl) {
            modalEl.addEventListener('shown.bs.modal', function () {
                input.focus();
            });
            modalEl.addEventListener('hidden.bs.modal', function () {
                input.value = '';
                _applySearch('');
            });
        }
    }

    function _initToggle() {
        var btn = document.getElementById(TOGGLE_ID);
        if (!btn) return;
        btn.addEventListener('click', function (e) {
            e.preventDefault();
            openHelp();
        });
    }

    function init() {
        _initToggle();
        _initSidebarNav();
        _initJumpLinks();
        _initSearch();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    window.HelpCenter = { open: openHelp, activate: _activateSection };
})();
