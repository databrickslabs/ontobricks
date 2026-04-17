/**
 * Parse triplestore config JSON embedded in the page (see domain.html).
 * Must load after the #triplestore-config element and before query-sync.js.
 */
(function () {
    var el = document.getElementById('triplestore-config');
    if (el) {
        try {
            window.__TRIPLESTORE_CONFIG = JSON.parse(el.textContent);
        } catch (e) {
            window.__TRIPLESTORE_CONFIG = {};
        }
    }
})();
