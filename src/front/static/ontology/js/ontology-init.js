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

// Configure sidebar navigation
window.SIDEBAR_NAV_MANUAL_INIT = true;
document.addEventListener('DOMContentLoaded', function() {
    // Check URL for section parameter (deep-link from top banner dropdown)
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');

    SidebarNav.init({
        onBeforeSectionChange: async function(section) {
            // Prompt for unsaved detail-panel changes before switching
            if (typeof checkDirtyBeforeSwitch === 'function') {
                await checkDirtyBeforeSwitch();
            }
            // Flush unsaved design layout before leaving the design section
            const currentSection = SidebarNav.getActiveSection();
            if (currentSection === 'design' && typeof flushDesignLayout === 'function') {
                await flushDesignLayout();
            }
            return true; // Allow switch
        },
        onSectionChange: function(section, targetSection) {
            // Initialize Wizard when switching to wizard section
            if (section === 'wizard' && typeof initOntologyWizard === 'function') {
                initOntologyWizard();
            }
            // Initialize OntoViz designer when switching to design section
            if (section === 'design' && typeof initOntologyDesigner === 'function') {
                initOntologyDesigner();
            }
            // Initialize D3.js map when switching to map section
            if (section === 'map') {
                // Show spinner immediately so the user never sees stale/empty content
                if (typeof showOntologyMapLoading === 'function') {
                    showOntologyMapLoading(true);
                }
                // Retry until container is visible and ontology data is loaded
                const tryInitMap = (retries = 0) => {
                    const container = document.getElementById('ontology-map-container');
                    const isVisible = container && container.offsetParent !== null;

                    if (isVisible && OntologyState.loaded) {
                        if (typeof initOntologyMap === 'function') initOntologyMap();
                    } else if (retries < 20) {
                        setTimeout(() => tryInitMap(retries + 1), 100);
                    } else {
                        console.warn('Ontology Model: Force initializing after timeout');
                        if (typeof initOntologyMap === 'function') initOntologyMap();
                    }
                };
                setTimeout(() => tryInitMap(0), 50);
            }
            // Refresh entities list when switching to entities section
            if (section === 'entities' && typeof updateClassesList === 'function') {
                updateClassesList();
            }
            // Refresh relationships list when switching to relationships section
            if (section === 'relationships' && typeof updatePropertiesList === 'function') {
                updatePropertiesList();
            }
            // Initialize Business Rules module (includes SWRL) when switching to swrl section
            if (section === 'swrl' && typeof BusinessRulesModule !== 'undefined') {
                BusinessRulesModule.init();
            }
            // Initialize Constraints module when switching to constraints section
            if (section === 'dataquality' && typeof DataQualityModule !== 'undefined') {
                DataQualityModule.init();
            }
            // Initialize Axioms module when switching to axioms section
            if (section === 'axioms' && typeof AxiomsModule !== 'undefined') {
                AxiomsModule.init();
            }
            // Load OWL content when switching to owl section
            if (section === 'owl' && typeof autoGenerateOwl === 'function') {
                autoGenerateOwl();
            }
        }
    });
    
    // Initialize the default section after ontology data is loaded
    // This is needed because onSectionChange doesn't fire for the initial active section
    initializeDefaultSection();

    // If section parameter was passed, navigate to that section
    if (initialSection) {
        const link = document.querySelector(`[data-section="${initialSection}"]`);
        if (link) {
            setTimeout(() => link.click(), 200);
        }
    }

    // Deep-link: auto-select an entity or relationship by name
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
 * Initialize the default active section after ensuring data is loaded
 */
async function initializeDefaultSection() {
    // Wait for ontology data to be fully loaded from session
    // This ensures OntologyState.config is populated before initializing the designer
    if (typeof window.waitForOntologyLoaded === 'function') {
        console.log('Waiting for ontology data to load...');
        await window.waitForOntologyLoaded();
        console.log('Ontology data loaded, initializing default section');
    }
    
    // Get the current active section
    const activeSection = SidebarNav.getActiveSection();
    console.log('Default section:', activeSection);
    
    // Initialize based on active section
    if (activeSection === 'wizard' && typeof initOntologyWizard === 'function') {
        setTimeout(() => {
            initOntologyWizard();
        }, 150);
    } else if (activeSection === 'design' && typeof initOntologyDesigner === 'function') {
        // Small delay to ensure DOM is fully ready after data load
        setTimeout(() => {
            initOntologyDesigner();
        }, 150);
    } else if (activeSection === 'map') {
        if (typeof showOntologyMapLoading === 'function') {
            showOntologyMapLoading(true);
        }
        const tryInitMap = (retries = 0) => {
            const container = document.getElementById('ontology-map-container');
            const isVisible = container && container.offsetParent !== null;

            if (isVisible && OntologyState.loaded) {
                if (typeof initOntologyMap === 'function') initOntologyMap();
            } else if (retries < 20) {
                setTimeout(() => tryInitMap(retries + 1), 100);
            } else {
                console.warn('Ontology Model: Force initializing after timeout');
                if (typeof initOntologyMap === 'function') initOntologyMap();
            }
        };
        setTimeout(() => tryInitMap(0), 50);
    } else if (activeSection === 'entities' && typeof updateClassesList === 'function') {
        updateClassesList();
    } else if (activeSection === 'relationships' && typeof updatePropertiesList === 'function') {
        updatePropertiesList();
    } else if (activeSection === 'owl' && typeof autoGenerateOwl === 'function') {
        autoGenerateOwl();
    }
}

