/**
 * OntoBricks - ontology-init.js
 * Ontology page initialization - extracted from ontology.html per code_instructions.txt
 */

// =====================================================
// ONTOLOGY PAGE - Sidebar Navigation & Initialization
// =====================================================

// Enable full-width layout for this page
document.body.classList.add('full-width-layout');

// OntoViz instance for the Design section
let ontologyDesigner = null;

var _MAP_RETRY_MAX = 20;
var _MAP_RETRY_INTERVAL = 100;

function _tryInitMap(retries) {
    if (retries === undefined) retries = 0;
    var container = document.getElementById('ontology-map-container');
    var isVisible = container && container.offsetParent !== null;

    if (isVisible && OntologyState.loaded) {
        if (typeof initOntologyMap === 'function') initOntologyMap();
    } else if (retries < _MAP_RETRY_MAX) {
        setTimeout(function () { _tryInitMap(retries + 1); }, _MAP_RETRY_INTERVAL);
    } else {
        console.warn('Ontology Model: Force initializing after timeout');
        if (typeof initOntologyMap === 'function') initOntologyMap();
    }
}

function _initSectionByName(section) {
    if (section === 'wizard' && typeof initOntologyWizard === 'function') {
        initOntologyWizard();
    } else if (section === 'design' && typeof initOntologyDesigner === 'function') {
        initOntologyDesigner();
    } else if (section === 'map') {
        if (typeof showOntologyMapLoading === 'function') {
            showOntologyMapLoading(true);
        }
        setTimeout(function () { _tryInitMap(0); }, 50);
    } else if (section === 'entities' && typeof updateClassesList === 'function') {
        updateClassesList();
    } else if (section === 'relationships' && typeof updatePropertiesList === 'function') {
        updatePropertiesList();
    } else if (section === 'swrl' && typeof BusinessRulesModule !== 'undefined') {
        BusinessRulesModule.init();
    } else if (section === 'dataquality' && typeof DataQualityModule !== 'undefined') {
        DataQualityModule.init();
    } else if (section === 'axioms' && typeof AxiomsModule !== 'undefined') {
        AxiomsModule.init();
    } else if (section === 'owl' && typeof autoGenerateOwl === 'function') {
        autoGenerateOwl();
    } else if (section === 'groups' && typeof OntologyGroups !== 'undefined') {
        OntologyGroups.init();
    }
}

// Configure sidebar navigation
window.SIDEBAR_NAV_MANUAL_INIT = true;
document.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');

    SidebarNav.init({
        onBeforeSectionChange: async function(section) {
            if (typeof checkDirtyBeforeSwitch === 'function') {
                await checkDirtyBeforeSwitch();
            }
            const currentSection = SidebarNav.getActiveSection();
            if (currentSection === 'design' && typeof flushDesignLayout === 'function') {
                await flushDesignLayout();
            }
            return true;
        },
        onSectionChange: function(section) {
            _initSectionByName(section);
        }
    });
    
    initializeDefaultSection();

    if (initialSection) {
        const link = document.querySelector(`[data-section="${initialSection}"]`);
        if (link) {
            setTimeout(() => link.click(), 200);
        }
    }

    const selectItem = urlParams.get('select');
    if (selectItem) {
        const waitForReady = (retries = 0) => {
            if (!OntologyState.loaded && retries < 40) {
                setTimeout(() => waitForReady(retries + 1), 150);
                return;
            }
            if (initialSection === 'entities' && typeof editClassByName === 'function') {
                editClassByName(selectItem);
            } else if (initialSection === 'relationships' && typeof editPropertyByName === 'function') {
                editPropertyByName(selectItem);
            }
        };
        setTimeout(() => waitForReady(0), 400);
    }
});

/**
 * Initialize the default active section after ensuring data is loaded.
 */
async function initializeDefaultSection() {
    if (typeof window.waitForOntologyLoaded === 'function') {
        await window.waitForOntologyLoaded();
    }
    
    const activeSection = SidebarNav.getActiveSection();

    if (activeSection === 'wizard' || activeSection === 'design') {
        setTimeout(function () { _initSectionByName(activeSection); }, 150);
    } else {
        _initSectionByName(activeSection);
    }
}

