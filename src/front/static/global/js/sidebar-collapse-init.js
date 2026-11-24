/**
 * Pre-paint sidebar collapse preference.
 *
 * Runs synchronously in <head> (no defer) so the default-collapsed rail
 * never flashes expanded→collapsed on first paint. Mirrors the preference
 * stored by SidebarNav under ``ontobricks-sidebar-collapsed``.
 */
(function () {
    var KEY = 'ontobricks-sidebar-collapsed';
    var collapsed = true;
    try {
        collapsed = localStorage.getItem(KEY) !== '0';
    } catch (_) {
        collapsed = true;
    }
    document.documentElement.classList.toggle('ob-sidebar-collapsed', collapsed);
})();
