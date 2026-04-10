/**
 * Pre-render section switcher: reads ?section= from the URL and injects
 * a temporary <style> so the correct sidebar section is visible on first
 * paint, avoiding a flash of the default section.
 */
(function() {
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');
    if (initialSection) {
        const style = document.createElement('style');
        style.id = 'initial-section-style';
        style.textContent =
            '.sidebar-section { display: none !important; }' +
            ' #' + initialSection + '-section { display: flex !important; }' +
            ' .nav-link.active { background: transparent !important; border-left-color: transparent !important; }' +
            ' .nav-link[data-section="' + initialSection + '"] { background: rgba(0, 0, 0, 0.15) !important; border-left-color: #333 !important; }';
        document.head.appendChild(style);
    }
})();
