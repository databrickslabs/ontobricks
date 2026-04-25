/**
 * OntoBricks - Ontology Core Module
 * Shared state and utilities for ontology management
 */

// =====================================================
// SHARED STATE
// =====================================================

// Ontology configuration - THE central state object
const OntologyState = {
    config: {
        name: 'myontology',
        base_uri: '',
        classes: [],
        properties: []
    },
    
    // Track editing state
    editingClassIndex: -1,
    editingPropertyIndex: -1,
    
    // Default emoji for classes
    defaultClassEmoji: '📦',
    
    // Base URI domain (loaded from config)
    baseUriDomain: 'https://databricks-ontology.com',
    
    // Vis.js network instance
    network: null,
    
    // Flag indicating if data has been loaded from session
    loaded: false
};

// =====================================================
// CORE UTILITY FUNCTIONS
// =====================================================

/**
 * Load base URI from domain session (managed on the Domain page)
 * Base URI is no longer auto-generated - it's managed in the Project Information page.
 */
function generateBaseUri() {
    // This function is kept for backward compatibility but no longer auto-generates
    // The base URI is managed in the Project page
    loadBaseUriFromDomain();
}

/**
 * Load base URI from domain session
 */
async function loadBaseUriFromDomain() {
    const baseUriInput = document.getElementById('baseUri');
    if (!baseUriInput) return;
    
    try {
        const response = await fetch('/domain/info', { credentials: 'same-origin' });
        const result = await response.json();
        if (result.success && result.info && result.info.base_uri) {
            baseUriInput.value = result.info.base_uri;
            OntologyState.config.base_uri = result.info.base_uri;
        }
    } catch (error) {
        console.log('Could not load base URI from domain session');
    }
}

/**
 * Load base URI domain from config (kept for backward compatibility)
 */
async function loadBaseUriDomain() {
    try {
        const response = await fetch('/settings/get-base-uri', { credentials: 'same-origin' });
        const result = await response.json();
        if (result.success && result.base_uri) {
            OntologyState.baseUriDomain = result.base_uri;
        }
    } catch (error) {
        console.log('Using default base URI domain');
    }
    // Load base URI from domain session instead of generating
    loadBaseUriFromDomain();
}

/**
 * Load default emoji from config
 */
async function loadDefaultEmoji() {
    try {
        const response = await fetch('/settings/get-default-emoji', { credentials: 'same-origin' });
        const result = await response.json();
        if (result.success && result.emoji) {
            OntologyState.defaultClassEmoji = result.emoji;
        }
    } catch (error) {
        console.log('Using default emoji:', OntologyState.defaultClassEmoji);
    }
}

/**
 * Save current OntologyState.config to session
 * This is the central save function called after any config change
 */
async function saveConfigToSession() {
    try {
        // Strip sub-collections managed by their own dedicated endpoints
        // so we don't overwrite them with stale frontend state.
        var payload = Object.assign({}, OntologyState.config);
        delete payload.swrl_rules;
        delete payload.constraints;
        delete payload.axioms;
        delete payload.expressions;

        const response = await fetch('/ontology/save', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            credentials: 'same-origin'
        });
        const result = await response.json();
        if (!result.success) {
            console.error('[AUTO-SAVE] Failed:', result.message);
        } else {
            console.log('[AUTO-SAVE] Config saved to session');
        }
        return result.success;
    } catch (error) {
        console.error('[AUTO-SAVE] Error:', error);
        return false;
    }
}

// Make it globally available
window.saveConfigToSession = saveConfigToSession;

/**
 * Auto-generate OWL whenever ontology design changes
 * Also saves the config to session after successful generation
 */
async function autoGenerateOwl() {
    const nameInput = document.getElementById('ontologyName');
    const baseUriInput = document.getElementById('baseUri');
    const owlPreview = document.getElementById('owlPreview');
    
    // Helper to format validation messages (like the Validate button)
    const formatValidationReport = (config, issues, warnings, serverError = null) => {
        let report = '';
        report += '╔══════════════════════════════════════════════════════════════════╗\n';
        report += '║                    ONTOLOGY VALIDATION REPORT                    ║\n';
        report += '╚══════════════════════════════════════════════════════════════════╝\n\n';
        
        // Summary
        report += '┌─────────────────────────────────────────────────────────────────┐\n';
        report += '│ SUMMARY                                                          │\n';
        report += '├─────────────────────────────────────────────────────────────────┤\n';
        report += `│ Name:          ${(config.name || 'Not defined').padEnd(50)}│\n`;
        report += `│ Base URI:      ${(config.base_uri || 'Not defined').padEnd(50)}│\n`;
        report += `│ Entities:      ${String(config.classes?.length || 0).padEnd(50)}│\n`;
        report += `│ Relationships: ${String(config.properties?.length || 0).padEnd(50)}│\n`;
        report += '└─────────────────────────────────────────────────────────────────┘\n\n';
        
        // Issues (Errors)
        if (issues.length > 0) {
            report += '┌─────────────────────────────────────────────────────────────────┐\n';
            report += '│ ✗ ISSUES (must fix before OWL can be generated)                 │\n';
            report += '├─────────────────────────────────────────────────────────────────┤\n';
            issues.forEach((issue, i) => {
                const lines = wrapText(issue, 63);
                lines.forEach((line, j) => {
                    const prefix = j === 0 ? `│ ${i + 1}. ` : '│    ';
                    report += `${prefix}${line.padEnd(64 - prefix.length + 1)}│\n`;
                });
            });
            report += '└─────────────────────────────────────────────────────────────────┘\n\n';
        }
        
        // Warnings
        if (warnings.length > 0) {
            report += '┌─────────────────────────────────────────────────────────────────┐\n';
            report += '│ ⚠ WARNINGS                                                      │\n';
            report += '├─────────────────────────────────────────────────────────────────┤\n';
            warnings.forEach((warning, i) => {
                const lines = wrapText(warning, 63);
                lines.forEach((line, j) => {
                    const prefix = j === 0 ? `│ ${i + 1}. ` : '│    ';
                    report += `${prefix}${line.padEnd(64 - prefix.length + 1)}│\n`;
                });
            });
            report += '└─────────────────────────────────────────────────────────────────┘\n\n';
        }
        
        // Server Error
        if (serverError) {
            report += '┌─────────────────────────────────────────────────────────────────┐\n';
            report += '│ ✗ SERVER ERROR                                                  │\n';
            report += '├─────────────────────────────────────────────────────────────────┤\n';
            const lines = wrapText(serverError, 63);
            lines.forEach((line) => {
                report += `│ ${line.padEnd(64)}│\n`;
            });
            report += '└─────────────────────────────────────────────────────────────────┘\n\n';
        }
        
        // Status
        const hasErrors = issues.length > 0 || serverError;
        report += '═══════════════════════════════════════════════════════════════════\n';
        if (hasErrors) {
            report += '  STATUS: ✗ OWL GENERATION FAILED - Please fix the issues above\n';
        } else if (warnings.length > 0) {
            report += '  STATUS: ⚠ OWL generated with warnings\n';
        } else {
            report += '  STATUS: ✓ OWL generated successfully\n';
        }
        report += '═══════════════════════════════════════════════════════════════════\n';
        
        return report;
    };
    
    // Helper to wrap long text
    const wrapText = (text, maxWidth) => {
        const words = text.split(' ');
        const lines = [];
        let currentLine = '';
        
        words.forEach(word => {
            if ((currentLine + ' ' + word).trim().length <= maxWidth) {
                currentLine = (currentLine + ' ' + word).trim();
            } else {
                if (currentLine) lines.push(currentLine);
                currentLine = word.length > maxWidth ? word.substring(0, maxWidth) : word;
            }
        });
        if (currentLine) lines.push(currentLine);
        return lines.length ? lines : [''];
    };
    
    // Helper to show validation report in OWL preview
    const showOwlValidation = (config, issues, warnings, serverError = null) => {
        if (owlPreview) {
            owlPreview.value = formatValidationReport(config, issues, warnings, serverError);
            owlPreview.classList.toggle('owl-error', issues.length > 0 || serverError);
        }
    };
    
    // Helper to show success
    const showOwlSuccess = (owlContent, config, warnings) => {
        if (owlPreview) {
            if (warnings.length > 0) {
                // Show warnings header before the OWL content
                let header = '# WARNINGS:\n';
                warnings.forEach((w, i) => {
                    header += `#   ${i + 1}. ${w}\n`;
                });
                header += '#\n# ─────────────────────────────────────────────────────────────\n\n';
                owlPreview.value = header + owlContent;
            } else {
                owlPreview.value = owlContent;
            }
            owlPreview.classList.remove('owl-error');
        }
    };
    
    if (!nameInput || !baseUriInput) {
        showOwlValidation({}, ['Missing required form elements (ontologyName or baseUri)'], []);
        return;
    }
    
    const name = nameInput.value.trim();
    const baseUri = baseUriInput.value.trim();
    
    // Build issues and warnings (same as Validate button)
    const issues = [];
    const warnings = [];
    
    if (!name) {
        issues.push('Ontology name is required');
    } else if (name === 'myontology' || name === 'newproject' || name === 'newdomain') {
        warnings.push('Ontology name is still set to the default. Set a Domain Name in Domain Information.');
    }
    
    if (!baseUri) {
        issues.push('Base URI is required');
    }
    
    // If we have critical issues, show them and stop
    if (!name || !baseUri) {
        const config = { name, base_uri: baseUri, classes: [], properties: [] };
        showOwlValidation(config, issues, warnings);
        return;
    }
    
    OntologyState.config.name = name;
    OntologyState.config.base_uri = baseUri;
    
    // Check ontology content
    if (!OntologyState.config.classes || OntologyState.config.classes.length === 0) {
        issues.push('No entities defined. Please add at least one entity.');
    }
    
    const objectProperties = (OntologyState.config.properties || []).filter(p => p.type !== 'DatatypeProperty');
    if (objectProperties.length === 0) {
        warnings.push('No relationships defined');
    } else {
        const propsWithoutDomain = objectProperties.filter(p => !p.domain);
        if (propsWithoutDomain.length > 0) {
            const names = propsWithoutDomain.map(p => p.label || p.name || '(unnamed)').join(', ');
            warnings.push(`${propsWithoutDomain.length} relationship(s) without domain (source entity): ${names}`);
        }
        
        const propsWithoutRange = objectProperties.filter(p => !p.range);
        if (propsWithoutRange.length > 0) {
            const names = propsWithoutRange.map(p => p.label || p.name || '(unnamed)').join(', ');
            warnings.push(`${propsWithoutRange.length} relationship(s) without range (target entity): ${names}`);
        }
    }
    
    // If we have blocking issues, show them
    if (issues.length > 0) {
        showOwlValidation(OntologyState.config, issues, warnings);
        return;
    }
    
    // When the user is viewing an inactive (older) version we cannot POST
    // to /ontology/generate-owl because it persists the domain and is
    // blocked by the backend PermissionMiddleware. Fall back to GET
    // /ontology/export-owl, which renders the same OWL content
    // server-side without persisting anything.
    //
    // Viewers no longer need a JS guard here: their write surfaces are
    // hidden / disabled declaratively by ``permissions.css`` so the POST
    // path is unreachable for them. We still keep the read-only
    // endpoint for the inactive-version flow, where admins/builders can
    // legitimately preview the OWL without persisting changes.
    const isReadOnly = () => window.isActiveVersion === false;

    const readOnlyFetch = () => fetch('/ontology/export-owl', {
        method: 'GET',
        credentials: 'same-origin'
    });

    try {
        let response;
        let usedReadOnlyEndpoint = isReadOnly();
        if (usedReadOnlyEndpoint) {
            response = await readOnlyFetch();
        } else {
            response = await fetch('/ontology/generate-owl', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(OntologyState.config),
                credentials: 'same-origin'
            });
        }

        // Always try to parse the JSON response (even for error status codes)
        let result;
        try {
            result = await response.json();
        } catch (jsonError) {
            // If JSON parsing fails, show the HTTP error
            showOwlValidation(OntologyState.config, issues, warnings, 
                `Server error (HTTP ${response.status}): ${response.statusText}`);
            return;
        }
        
        if (result.success) {
            // /generate-owl returns {owl}, /export-owl returns {owl_content}.
            const owl = result.owl || result.owl_content || '';
            showOwlSuccess(owl, OntologyState.config, warnings);

            if (!usedReadOnlyEndpoint) {
                // Save config to session after successful generation
                await saveConfigToSession();
            }

            // Auto-validate and update navbar
            if (typeof window.autoValidateOntology === 'function') {
                window.autoValidateOntology();
            }
        } else {
            // Show the actual error message from the server
            showOwlValidation(OntologyState.config, issues, warnings, 
                result.message || result.error || 'Unknown error during OWL generation');
        }
    } catch (error) {
        showOwlValidation(OntologyState.config, issues, warnings, 
            `Network or parsing error: ${error.message}`);
    }
}

// Promise that resolves when ontology data is loaded
let ontologyLoadedResolve;
const ontologyLoadedPromise = new Promise(resolve => {
    ontologyLoadedResolve = resolve;
});

// Make promise available globally
window.waitForOntologyLoaded = function() {
    return ontologyLoadedPromise;
};

/**
 * Load ontology from session
 */
async function loadOntologyFromSession() {
    try {
        console.log('[DEBUG] loadOntologyFromSession: Fetching from /ontology/load');
        const response = await fetch('/ontology/load', { credentials: 'same-origin' });
        const result = await response.json();
        
        console.log('[DEBUG] loadOntologyFromSession: Response received', {
            success: result.success,
            hasConfig: !!result.config,
            configKeys: result.config ? Object.keys(result.config) : [],
            classCount: result.config?.classes?.length || 0,
            propCount: result.config?.properties?.length || 0,
            name: result.config?.name
        });
        
        if (result.success && result.config && Object.keys(result.config).length > 0) {
            OntologyState.config = result.config;
            
            const nameInput = document.getElementById('ontologyName');
            const baseUriInput = document.getElementById('baseUri');
            
            if (OntologyState.config.name && nameInput) {
                nameInput.value = OntologyState.config.name;
                generateBaseUri();
            }
            if (OntologyState.config.base_uri && baseUriInput) {
                baseUriInput.value = OntologyState.config.base_uri;
            }
            
            // Update lists if functions are available
            if (typeof updateClassesList === 'function') updateClassesList();
            if (typeof updatePropertiesList === 'function') updatePropertiesList();
            autoGenerateOwl();
        }
        
        // Signal that ontology data is loaded (even if empty)
        OntologyState.loaded = true;
        if (ontologyLoadedResolve) {
            ontologyLoadedResolve();
        }
        
        // Dispatch custom event for other scripts
        window.dispatchEvent(new CustomEvent('ontologyLoaded', { 
            detail: { config: OntologyState.config } 
        }));
        
    } catch (error) {
        console.error('Error loading ontology:', error);
        // Still signal as loaded (with error) so initialization can proceed
        OntologyState.loaded = true;
        if (ontologyLoadedResolve) {
            ontologyLoadedResolve();
        }
    }
}

// =====================================================
// INITIALIZATION
// =====================================================

document.addEventListener('DOMContentLoaded', async function() {
    await loadDefaultEmoji();
    await loadBaseUriDomain();
    await loadOntologyFromSession();
});

