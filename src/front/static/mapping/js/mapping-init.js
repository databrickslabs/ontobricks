/**
 * OntoBricks - mapping-init.js
 * Mapping page initialization - extracted from mapping.html per code_instructions.txt
 */

// =====================================================
// MAPPING PAGE - Sidebar Navigation & Initialization
// =====================================================

// Enable full-width layout for this page
document.body.classList.add('full-width-layout');

// Configure sidebar navigation with callbacks
window.SIDEBAR_NAV_MANUAL_INIT = true;

// Main initialization function
async function initializeMappingPage() {
    console.log('Mapping Page: Starting initialization...');
    
    // Check MappingState
    if (typeof MappingState === 'undefined') {
        console.error('Mapping Page: MappingState not defined!');
        return;
    }
    
    console.log('Mapping Page: MappingState found, loading data...');
    
    // Load existing mappings
    try {
        const mappingResponse = await fetch('/mapping/load', { credentials: 'same-origin' });
        const result = await mappingResponse.json();
        console.log('Mapping Page: Raw mapping response:', result);
        
        // API returns { success: true, config: { entities: [...], relationships: [...], ... } }
        const savedConfig = result.config || result;
        
        if (savedConfig && Object.keys(savedConfig).length > 0) {
            const entitySource = savedConfig.entities || savedConfig.data_source_mappings;
            if (entitySource) {
                // Deduplicate entity mappings by ontology_class
                const entityMap = new Map();
                entitySource.forEach(m => {
                    const key = m.ontology_class || m.class_uri;
                    if (key) {
                        entityMap.set(key, m);
                    }
                });
                MappingState.config.entities = Array.from(entityMap.values());
            }
            const relSource = savedConfig.relationships || savedConfig.relationship_mappings;
            if (relSource) {
                // Deduplicate relationship mappings by property URI
                const relMap = new Map();
                relSource.forEach(m => {
                    if (m.property) {
                        relMap.set(m.property, m);
                    }
                });
                MappingState.config.relationships = Array.from(relMap.values());
            }
        }
        console.log('Mapping Page: Entity mappings:', MappingState.config.entities.length);
        console.log('Mapping Page: Relationship mappings:', MappingState.config.relationships.length);
    } catch (e) {
        console.error('Mapping Page: Error loading mappings:', e);
    }
    
    // Load ontology
    try {
        const ontologyResponse = await fetch('/ontology/get-loaded-ontology', { credentials: 'same-origin' });
        const result = await ontologyResponse.json();
        console.log('Mapping Page: Raw ontology response:', result);
        
        if (result.success && result.ontology) {
            MappingState.loadedOntology = result.ontology;
            console.log('Mapping Page: Ontology classes:', result.ontology.classes?.length);
            console.log('Mapping Page: Ontology properties:', result.ontology.properties?.length);
            
            // Clean up orphaned mappings (for items that no longer exist in the ontology)
            // Build a robust set of valid identifiers (URI, name, localName, and generated URI patterns)
            if (MappingState.loadedOntology.classes) {
                const validClassIdentifiers = new Set();
                const baseUri = MappingState.loadedOntology.base_uri || '';
                MappingState.loadedOntology.classes.forEach(c => {
                    if (c.uri) validClassIdentifiers.add(c.uri);
                    if (c.name) {
                        validClassIdentifiers.add(c.name);
                        // Also add generated URI patterns
                        if (baseUri) validClassIdentifiers.add(baseUri + c.name);
                    }
                    if (c.localName) {
                        validClassIdentifiers.add(c.localName);
                        if (baseUri) validClassIdentifiers.add(baseUri + c.localName);
                    }
                });
                
                const beforeEntityCount = MappingState.config.entities.length;
                MappingState.config.entities = MappingState.config.entities.filter(m => {
                    // Check various identifier fields
                    const mappingUri = m.ontology_class || m.class_uri;
                    if (!mappingUri) return false;
                    
                    // Direct match
                    if (validClassIdentifiers.has(mappingUri)) return true;
                    
                    // Extract class name from URI (e.g., "https://example.org/MyOrg#Person" -> "Person")
                    const uriParts = mappingUri.split(/[#\/]/);
                    const className = uriParts[uriParts.length - 1];
                    if (className && validClassIdentifiers.has(className)) return true;
                    
                    return false;
                });
                if (MappingState.config.entities.length < beforeEntityCount) {
                    console.log(`Mapping Page: Cleaned up ${beforeEntityCount - MappingState.config.entities.length} orphaned entity mappings`);
                }
            }
            
            if (MappingState.loadedOntology.properties) {
                const validPropIdentifiers = new Set();
                const baseUri = MappingState.loadedOntology.base_uri || '';
                MappingState.loadedOntology.properties.forEach(p => {
                    if (p.uri) validPropIdentifiers.add(p.uri);
                    if (p.name) {
                        validPropIdentifiers.add(p.name);
                        if (baseUri) validPropIdentifiers.add(baseUri + p.name);
                    }
                    if (p.localName) {
                        validPropIdentifiers.add(p.localName);
                        if (baseUri) validPropIdentifiers.add(baseUri + p.localName);
                    }
                });
                
                const beforeRelCount = MappingState.config.relationships.length;
                MappingState.config.relationships = MappingState.config.relationships.filter(m => {
                    const propUri = m.property;
                    if (!propUri) return false;
                    
                    // Direct match
                    if (validPropIdentifiers.has(propUri)) return true;
                    
                    // Extract property name from URI
                    const uriParts = propUri.split(/[#\/]/);
                    const propName = uriParts[uriParts.length - 1];
                    if (propName && validPropIdentifiers.has(propName)) return true;
                    
                    return false;
                });
                if (MappingState.config.relationships.length < beforeRelCount) {
                    console.log(`Mapping Page: Cleaned up ${beforeRelCount - MappingState.config.relationships.length} orphaned relationship mappings`);
                }
                
                // Ensure direction is set on relationship mappings (derive from ontology if missing)
                MappingState.config.relationships.forEach(m => {
                    if (!m.direction) {
                        const ontProp = MappingState.loadedOntology.properties.find(p => p.uri === m.property);
                        if (ontProp) {
                            m.direction = ontProp.direction || 'forward';
                            console.log(`Mapping Page: Set direction '${m.direction}' for ${m.property_label || m.property}`);
                        }
                    }
                });
            }
        } else {
            console.log('Mapping Page: No ontology in response');
            MappingState.loadedOntology = null;
        }
    } catch (e) {
        console.error('Mapping Page: Error loading ontology:', e);
    }
    
    // Stamp excluded flags onto ontology objects from mapping entries
    if (typeof _stampExcludedFlags === 'function') {
        _stampExcludedFlags();
    }
    
    MappingState.initialized = true;
    
    // Now update the UI - wait a moment for DOM to be ready
    console.log('Mapping Page: Updating UI components...');
    console.log('Mapping Page: Available functions check:', {
        updateTaxonomyStatus: typeof updateTaxonomyStatus,
        updateMappingCompletionStatus: typeof updateMappingCompletionStatus
    });
    
    // Call UI update functions
    try {
        if (typeof updateTaxonomyStatus === 'function') {
            updateTaxonomyStatus();
            console.log('Mapping Page: updateTaxonomyStatus called');
        }
    } catch (e) {
        console.error('Mapping Page: Error in updateTaxonomyStatus:', e);
    }
    
    try {
        if (typeof updateMappingCompletionStatus === 'function') {
            updateMappingCompletionStatus();
            console.log('Mapping Page: updateMappingCompletionStatus called');
        }
    } catch (e) {
        console.error('Mapping Page: Error in updateMappingCompletionStatus:', e);
    }
    
    console.log('Mapping Page: Initialization complete');
}

// Navigate to initial section after data is loaded
async function navigateToInitialSection(initialSection) {
    if (!initialSection) return;
    
    // Wait for initialization to complete
    let attempts = 0;
    while (!MappingState.initialized && attempts < 50) {
        await new Promise(resolve => setTimeout(resolve, 100));
        attempts++;
    }
    
    // Now navigate to the section
    const link = document.querySelector(`[data-section="${initialSection}"]`);
    if (link) {
        link.click();
        
        // Remove the temporary style that was hiding the flash
        setTimeout(() => {
            const tempStyle = document.getElementById('initial-section-style');
            if (tempStyle) tempStyle.remove();
        }, 50);
    }
}

// Deep-link: auto-select an entity or relationship by name after the section is ready
function autoSelectMappingItem(itemName, itemType) {
    if (!itemName) return;
    const trySelect = (retries = 0) => {
        if (!MappingState.initialized || !MappingState.loadedOntology) {
            if (retries < 40) setTimeout(() => trySelect(retries + 1), 150);
            return;
        }
        if (itemType === 'relationship') {
            if (typeof openRelationshipMappingFromDesign === 'function') {
                openRelationshipMappingFromDesign({ name: itemName });
            } else if (typeof ManualModule !== 'undefined') {
                ManualModule.openItem(itemName, 'relationship');
            }
        } else {
            if (typeof openEntityMappingFromDesign === 'function') {
                openEntityMappingFromDesign({ name: itemName });
            } else if (typeof ManualModule !== 'undefined') {
                ManualModule.openItem(itemName, 'entity');
            }
        }
    };
    setTimeout(() => trySelect(0), 600);
}

// Common SidebarNav configuration for Mapping
const mappingSidebarConfig = {
    onBeforeSectionChange: async function(section) {
        // Block navigation to other sections while auto-map is running
        if (typeof AutoAssignModule !== 'undefined' && AutoAssignModule.isRunning && section !== 'autoassign') {
            showNotification('Please wait until Auto-Map finishes before switching sections.', 'warning');
            return false;
        }
        return true; // Allow switch
    },
    onSectionChange: function(section, targetSection) {
        if (section === 'design') {
            // Show spinner immediately so the user never sees stale/empty content
            if (typeof showMappingDesignerLoading === 'function') {
                showMappingDesignerLoading(true);
            }
            // Add delay to ensure section is visible and container is ready
            // Also wait for MappingState to be initialized
            const tryInitDesigner = (retries = 0) => {
                const container = document.getElementById('mapping-map-container');
                const isVisible = container && container.offsetParent !== null;
                
                if (isVisible && MappingState.initialized) {
                    initMappingDesigner();
                } else if (retries < 10) {
                    // Retry after a short delay (up to 10 times = 1 second total)
                    setTimeout(() => tryInitDesigner(retries + 1), 100);
                } else {
                    // Force init after max retries
                    console.warn('Mapping Page: Force initializing designer after timeout');
                    initMappingDesigner();
                }
            };
            
            // Start with a small initial delay for CSS transitions
            setTimeout(() => tryInitDesigner(0), 50);
        }
        if (section === 'manual' && typeof ManualModule !== 'undefined') {
            // Add delay and retry logic to ensure section is visible and data is loaded
            const tryInitManual = (retries = 0) => {
                const container = document.getElementById('manualAssignmentTree');
                const isVisible = container && container.offsetParent !== null;
                
                if (isVisible && MappingState.initialized && MappingState.loadedOntology) {
                    ManualModule.init();
                } else if (retries < 10) {
                    // Retry after a short delay (up to 10 times = 1 second total)
                    setTimeout(() => tryInitManual(retries + 1), 100);
                } else {
                    // Force init after max retries
                    console.warn('Mapping Page: Force initializing Manual module after timeout');
                    ManualModule.init();
                }
            };
            
            setTimeout(() => tryInitManual(0), 50);
        }
        if (section === 'r2rml' && typeof generateR2RMLPreview === 'function') {
            setTimeout(() => generateR2RMLPreview(), 50);
        }
        if (section === 'sparksql' && typeof refreshMappingSql === 'function') {
            setTimeout(() => refreshMappingSql(), 50);
        }
        // Dispatch event for other modules
        document.dispatchEvent(new CustomEvent('sectionChange', { detail: { section } }));
    }
};

// Initialize the default section after page data is loaded
async function initializeDefaultSection() {
    // Wait for MappingState to be initialized
    let attempts = 0;
    while (!MappingState.initialized && attempts < 50) {
        await new Promise(resolve => setTimeout(resolve, 100));
        attempts++;
    }
    
    // Check which section is currently active (default)
    const activeSection = document.querySelector('.sidebar-section.active');
    if (activeSection) {
        const sectionId = activeSection.id.replace('-section', '');
        console.log('Mapping Page: Initializing default section:', sectionId);
        
        // Trigger the section initialization
        if (sectionId === 'design') {
            if (typeof showMappingDesignerLoading === 'function') showMappingDesignerLoading(true);
            if (typeof initMappingDesigner === 'function') initMappingDesigner();
        } else if (sectionId === 'manual' && typeof ManualModule !== 'undefined') {
            ManualModule.init();
        } else if (sectionId === 'r2rml' && typeof generateR2RMLPreview === 'function') {
            generateR2RMLPreview();
        } else if (sectionId === 'sparksql' && typeof refreshMappingSql === 'function') {
            refreshMappingSql();
        }
    }
}

// Run initialization when DOM is ready
function _bootstrapMappingPage() {
    const urlParams = new URLSearchParams(window.location.search);
    const initialSection = urlParams.get('section');
    const selectItem = urlParams.get('select');
    const selectType = urlParams.get('type') || 'entity';

    SidebarNav.init(mappingSidebarConfig);
    setTimeout(initializeMappingPage, 100);

    if (initialSection) {
        navigateToInitialSection(initialSection);
    } else {
        setTimeout(initializeDefaultSection, 200);
    }

    if (selectItem) {
        autoSelectMappingItem(selectItem, selectType);
    }
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _bootstrapMappingPage);
} else {
    _bootstrapMappingPage();
}

// Fallback: also run on window.load to be absolutely sure
window.addEventListener('load', function() {
    console.log('Mapping Page: window.load fired');
    if (!MappingState.initialized) {
        console.log('Mapping Page: Running fallback initialization');
        initializeMappingPage();
    } else {
        // Re-run UI updates just in case
        console.log('Mapping Page: Re-running UI updates on window.load');
        if (typeof updateTaxonomyStatus === 'function') updateTaxonomyStatus();
        if (typeof updateMappingCompletionStatus === 'function') updateMappingCompletionStatus();
    }
});
