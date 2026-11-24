/**
 * Pre-render section switcher: reads ?section= from the URL and injects
 * a temporary <style> so the correct sidebar section is visible on first
 * paint, avoiding a flash of the default section.
 *
 * Legacy ``?section=information`` (Mapping → General, removed) and
 * ``?section=import`` land on Studio → Informations; the R2RML import button
 * lives in the Studio toolbar.
 */
(function() {
    const urlParams = new URLSearchParams(window.location.search);
    let initialSection = urlParams.get('section');
    if (initialSection === 'information' || initialSection === 'import') {
        initialSection = 'design';
        window.__OB_MAPPING_STUDIO_TAB = 'informations';
    }
    // Legacy Auto-Map page → stay on default/current section; modal opens via JS.
    if (initialSection === 'autoassign') {
        initialSection = urlParams.get('fallback') || 'design';
        window.__OB_OPEN_MAPPING_AUTOMAP = true;
    }
    // Legacy Diagnostics page → stay on default/current section; modal opens via JS.
    if (initialSection === 'diagnostics') {
        initialSection = urlParams.get('fallback') || 'design';
        window.__OB_OPEN_MAPPING_DIAGNOSTICS = true;
    }
    // Legacy Manual sidebar → Studio → List tab.
    if (initialSection === 'manual') {
        initialSection = 'design';
        window.__OB_MAPPING_STUDIO_TAB = 'list';
    }
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
