/**
 * OntoBricks UI internationalisation (i18n).
 *
 * Translates the English UI into the selected language on the client:
 *   - server-rendered text nodes and attributes (placeholder, title, aria-label)
 *   - text injected later by page scripts (MutationObserver), which also
 *     covers showNotification / showConfirmDialog output
 *
 * Dictionaries register themselves in ``window.OB_I18N_DICTS[<lang>]`` as a
 * flat ``{ "English source": "Translation" }`` map (see i18n-ko.js). Keys are
 * whitespace-normalised; digit runs in a key are written as ``{n}``.
 *
 * The language is stored in the ``ob_lang`` cookie so the server can render
 * ``<html lang>`` on first paint and only ship the dictionary to non-English
 * pages. English is the identity language: no dictionary, no DOM work.
 *
 * Content inside code, editors, SVG, form values, and any element marked
 * ``translate="no"`` / ``.notranslate`` / ``[data-i18n-skip]`` is never touched,
 * so user data (entity names, SQL, SPARQL, ...) stays verbatim. Templates and
 * scripts mark the containers that render user-authored content with
 * ``data-i18n-skip``.
 */
(function () {
    'use strict';

    // Keep COOKIE / SUPPORTED / DEFAULT_LANG in sync with UI_LANG_COOKIE /
    // SUPPORTED_UI_LANGS / FALLBACK_UI_LANG in src/front/fastapi/dependencies.py
    // (a server-side ONTOBRICKS_UI_LANG override reaches us via <html lang>).
    var COOKIE = 'ob_lang';
    var SUPPORTED = ['en', 'ko'];
    var DEFAULT_LANG = 'en';
    var ONE_YEAR_SECONDS = 365 * 24 * 60 * 60; // language cookie max-age
    var ATTRS = ['placeholder', 'title', 'aria-label', 'data-bs-original-title', 'data-bs-title'];
    var SKIP_SELECTOR = [
        'script', 'style', 'code', 'pre', 'textarea', 'svg', 'kbd', 'samp',
        '[contenteditable=""]', '[contenteditable="true"]',
        '[translate="no"]', '.notranslate', '[data-i18n-skip]',
        '.CodeMirror', '.cm-editor', '.monaco-editor', '.ace_editor'
    ].join(',');
    var TRAILING_PUNCT = /^(.*?)(\s*(?:\.\.\.|…|[:.!?])+)$/;

    function normalize(text) {
        return String(text).replace(/\s+/g, ' ').trim();
    }

    function readCookie(name) {
        var m = document.cookie.match(new RegExp('(?:^|; )' + name + '=([^;]*)'));
        return m ? decodeURIComponent(m[1]) : null;
    }

    function resolveLang() {
        var fromCookie = readCookie(COOKIE);
        if (SUPPORTED.indexOf(fromCookie) >= 0) return fromCookie;
        var fromHtml = (document.documentElement.getAttribute('lang') || '').slice(0, 2);
        return SUPPORTED.indexOf(fromHtml) >= 0 ? fromHtml : DEFAULT_LANG;
    }

    /** Exact or ``{n}``-template lookup. Returns null when unknown. */
    function lookup(dict, key) {
        if (Object.prototype.hasOwnProperty.call(dict, key)) return dict[key];
        if (!/\d/.test(key)) return null;
        var nums = key.match(/\d+/g);
        var tpl = key.replace(/\d+/g, '{n}');
        if (!Object.prototype.hasOwnProperty.call(dict, tpl)) return null;
        var i = 0;
        return dict[tpl].replace(/\{n\}/g, function () { return nums[i++] || ''; });
    }

    /**
     * Translate one string with ``dict``. Tries, in order: exact match,
     * number template, "Known prefix: dynamic detail", and the core text
     * without trailing punctuation. Returns null when nothing matches.
     *
     * Examples, with dict = { "Save": "<Save>", "{n} entities": "<{n} entities>",
     * "Error deleting relationship:": "<Error deleting relationship>:",
     * "Loading": "<Loading>" }:
     *   exact:           "Save"                                 -> "<Save>"
     *   number template: "12 entities"                          -> "<12 entities>"
     *   known prefix:    "Error deleting relationship: timeout" -> "<Error deleting relationship>: timeout"
     *   trailing punct:  "Loading..."                           -> "<Loading>..."
     *   no match:        "Unknown text"                         -> null
     */
    function translateWith(dict, text) {
        var key = normalize(text);
        if (!key || !/[A-Za-z]/.test(key)) return null;
        var hit = lookup(dict, key);
        if (hit !== null) return hit;

        var colon = key.indexOf(': ');
        if (colon > 0) {
            var head = lookup(dict, key.slice(0, colon + 1)) || lookup(dict, key.slice(0, colon));
            if (head !== null) {
                return head.replace(/[:：]\s*$/, '') + ': ' + key.slice(colon + 2);
            }
        }

        var m = key.match(TRAILING_PUNCT);
        if (m && m[1]) {
            var core = lookup(dict, m[1]);
            if (core !== null) return core + m[2].trim();
        }
        return null;
    }

    /** Re-attach the original leading/trailing whitespace to a translation. */
    function keepPadding(original, translated) {
        var lead = original.match(/^\s*/)[0];
        var trail = original.match(/\s*$/)[0];
        return lead + translated + trail;
    }

    function createTranslator(dict) {
        function t(text) {
            if (text === null || text === undefined) return text;
            var out = translateWith(dict, text);
            return out === null ? text : keepPadding(String(text), out);
        }

        function isSkipped(el) {
            return !el || (el.closest && el.closest(SKIP_SELECTOR) !== null);
        }

        function translateTextNode(node) {
            var value = node.nodeValue;
            if (!value || !/[A-Za-z]/.test(value) || isSkipped(node.parentElement)) return;
            var next = t(value);
            if (next !== value) node.nodeValue = next;
        }

        function translateAttrs(el) {
            if (isSkipped(el)) return;
            for (var i = 0; i < ATTRS.length; i++) {
                var name = ATTRS[i];
                var value = el.getAttribute(name);
                if (!value) continue;
                var next = t(value);
                if (next !== value) el.setAttribute(name, next);
            }
            if (el.tagName === 'INPUT' && (el.type === 'button' || el.type === 'submit') && el.value) {
                var v = t(el.value);
                if (v !== el.value) el.value = v;
            }
        }

        function translateTree(root) {
            if (!root) return;
            if (root.nodeType === Node.TEXT_NODE) { translateTextNode(root); return; }
            if (root.nodeType !== Node.ELEMENT_NODE &&
                root.nodeType !== Node.DOCUMENT_NODE &&
                root.nodeType !== Node.DOCUMENT_FRAGMENT_NODE) return;
            if (root.nodeType === Node.ELEMENT_NODE) {
                if (isSkipped(root)) return;
                translateAttrs(root);
            }
            var walker = document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT, {
                acceptNode: function (n) {
                    if (n.nodeType === Node.ELEMENT_NODE && n.matches(SKIP_SELECTOR)) return NodeFilter.FILTER_REJECT;
                    return NodeFilter.FILTER_ACCEPT;
                }
            });
            var n;
            while ((n = walker.nextNode())) {
                if (n.nodeType === Node.TEXT_NODE) translateTextNode(n);
                else translateAttrs(n);
            }
        }

        return { t: t, translateTree: translateTree };
    }

    function observe(translator) {
        var pending = [];
        var scheduled = false;
        function flush() {
            scheduled = false;
            var batch = pending;
            pending = [];
            for (var i = 0; i < batch.length; i++) translator.translateTree(batch[i]);
        }
        var mo = new MutationObserver(function (records) {
            for (var i = 0; i < records.length; i++) {
                var r = records[i];
                if (r.type === 'childList') {
                    for (var j = 0; j < r.addedNodes.length; j++) pending.push(r.addedNodes[j]);
                } else if (r.type === 'characterData' || r.type === 'attributes') {
                    // Edited text node, or element whose ATTRS value changed.
                    pending.push(r.target);
                }
            }
            if (!scheduled && pending.length) {
                scheduled = true;
                Promise.resolve().then(flush);
            }
        });
        mo.observe(document.body, {
            childList: true, subtree: true, characterData: true,
            attributes: true, attributeFilter: ATTRS
        });
        return mo;
    }

    function setLang(lang) {
        if (SUPPORTED.indexOf(lang) < 0) return;
        var secure = window.location.protocol === 'https:' ? '; Secure' : '';
        document.cookie = COOKIE + '=' + lang + '; path=/; max-age=' + ONE_YEAR_SECONDS + '; SameSite=Lax' + secure;
        window.location.reload();
    }

    function bindLanguageSwitch(current) {
        document.addEventListener('click', function (e) {
            var el = e.target.closest && e.target.closest('[data-ob-lang]');
            if (!el) return;
            e.preventDefault();
            var lang = el.getAttribute('data-ob-lang');
            if (lang !== current) setLang(lang);
        });
        var items = document.querySelectorAll('[data-ob-lang]');
        for (var i = 0; i < items.length; i++) {
            var active = items[i].getAttribute('data-ob-lang') === current;
            items[i].classList.toggle('active', active);
            items[i].setAttribute('aria-current', active ? 'true' : 'false');
        }
        // <select data-ob-lang-select> (Settings > UI): reflect and switch.
        var selects = document.querySelectorAll('[data-ob-lang-select]');
        for (var s = 0; s < selects.length; s++) {
            selects[s].value = current;
            selects[s].addEventListener('change', function (e) {
                if (e.target.value !== current) setLang(e.target.value);
            });
        }
    }

    function init() {
        var lang = resolveLang();
        var dicts = window.OB_I18N_DICTS || {};
        var dict = lang === 'en' ? null : dicts[lang];
        var translator = createTranslator(dict || {});

        window.OB = window.OB || {};
        window.OB.i18n = {
            lang: lang,
            t: translator.t,
            translate: translator.translateTree,
            setLang: setLang,
            supported: SUPPORTED.slice()
        };

        bindLanguageSwitch(lang);
        if (dict) {
            translator.translateTree(document.body);
            observe(translator);
        }
        document.documentElement.classList.add('ob-i18n-ready');
    }

    // Exposed for unit tests (no DOM required).
    window.OB_I18N_CORE = {
        normalize: normalize,
        translateWith: translateWith,
        keepPadding: keepPadding,
        createTranslator: createTranslator
    };

    if (typeof document !== 'undefined' && document.body) {
        init();
    } else if (typeof document !== 'undefined') {
        document.addEventListener('DOMContentLoaded', init);
    }
})();
