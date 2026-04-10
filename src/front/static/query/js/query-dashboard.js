/**
 * OntoBricks - query-dashboard.js
 * Dashboard modal: URL building with parameters and iframe embedding.
 * Extracted from query.js per code_instructions.txt
 */

// =====================================================
// DASHBOARD MODAL FUNCTIONS
// =====================================================

/**
 * Build dashboard URL with parameters
 * 
 * For Databricks dashboards, use the EMBED URL from the Share dialog:
 * https://xxx.cloud.databricks.com/embed/dashboardsv3/{dashboardId}?o={workspaceId}
 * 
 * Parameters are passed based on the mappings configured in the ontology.
 * 
 * @param {string} baseUrl - The base dashboard URL
 * @param {string} entityId - The entity ID (fallback if no specific mapping)
 * @param {Object} paramValues - Mapped parameter values { paramName: value }
 */
function buildDashboardUrl(baseUrl, entityId, paramValues = {}) {
    if (!baseUrl) return baseUrl;
    
    try {
        let url = new URL(baseUrl);
        
        // If URL already has embed format, use it as-is
        // The user should save the complete embed URL from Databricks Share dialog
        if (url.pathname.includes('/embed/')) {
            console.log('[Dashboard] Using embed URL as-is');
        } else {
            // For non-embed URLs, try to convert to embed format
            // Note: This may not work without the workspace ID (o= parameter)
            let pathname = url.pathname;
            
            if (pathname.includes('/sql/dashboards/')) {
                pathname = pathname.replace('/sql/dashboards/', '/embed/dashboardsv3/');
                console.log('[Dashboard] Converted SQL dashboard to embed format');
            } else if (pathname.match(/\/dashboards\/[a-f0-9]+/)) {
                pathname = pathname.replace(/\/dashboards\//, '/embed/dashboardsv3/');
                console.log('[Dashboard] Converted dashboard to embed format');
            }
            
            url.pathname = pathname;
        }
        
        // Add mapped parameter values
        // Databricks AI/BI embedded dashboard format: f_{pageId}~{widgetId}=value
        if (Object.keys(paramValues).length > 0) {
            console.log('[Dashboard] Adding mapped parameters:', paramValues);
            for (const [paramKeyword, paramInfo] of Object.entries(paramValues)) {
                // Handle both old format (just value) and new format (object with IDs)
                let value, pageId, widgetId;
                if (typeof paramInfo === 'object' && paramInfo !== null) {
                    value = paramInfo.value;
                    pageId = paramInfo.pageId;
                    widgetId = paramInfo.widgetId;
                } else {
                    value = paramInfo;
                    pageId = '';
                    widgetId = '';
                }
                
                if (value) {
                    if (pageId && widgetId) {
                        // Databricks embed format: f_{pageId}~{widgetId}=value
                        const filterParamName = `f_${pageId}~${widgetId}`;
                        url.searchParams.set(filterParamName, value);
                        console.log(`[Dashboard] Added Databricks param: ${filterParamName}=${value}`);
                    } else {
                        // Fallback to simple format
                        url.searchParams.set(paramKeyword, value);
                        console.log(`[Dashboard] Added simple param: ${paramKeyword}=${value}`);
                    }
                }
            }
        } else if (entityId) {
            // Fallback: add ID as a generic parameter if no specific mappings
            url.searchParams.set('ID', entityId);
            console.log('[Dashboard] Added fallback ID parameter:', entityId);
        }
        
        console.log('[Dashboard] Final URL:', url.toString());
        return url.toString();
    } catch (e) {
        console.error('[Dashboard] URL parsing error:', e);
        // If URL parsing fails, try simple append
        const params = [];
        for (const [paramKeyword, paramInfo] of Object.entries(paramValues)) {
            const value = typeof paramInfo === 'object' ? paramInfo.value : paramInfo;
            const pageId = typeof paramInfo === 'object' ? paramInfo.pageId : '';
            const widgetId = typeof paramInfo === 'object' ? paramInfo.widgetId : '';
            if (value) {
                if (pageId && widgetId) {
                    params.push(`f_${pageId}~${widgetId}=${encodeURIComponent(value)}`);
                } else {
                    params.push(`${paramKeyword}=${encodeURIComponent(value)}`);
                }
            }
        }
        if (params.length === 0 && entityId) {
            params.push(`ID=${encodeURIComponent(entityId)}`);
        }
        if (params.length > 0) {
            const separator = baseUrl.includes('?') ? '&' : '?';
            return `${baseUrl}${separator}${params.join('&')}`;
        }
        return baseUrl;
    }
}

/**
 * Open dashboard in a modal popup with embedded iframe
 * Note: Requires Databricks workspace to have embedding enabled (see workspace admin settings)
 */
function openDashboardModal(dashboardUrl, entityType, entityId) {
    // Remove existing modal if any
    const existingModal = document.getElementById('dashboardModal');
    if (existingModal) {
        existingModal.remove();
    }
    
    // Create modal HTML - 90% width and height, centered
    const modalHtml = `
        <div class="modal fade" id="dashboardModal" tabindex="-1" aria-labelledby="dashboardModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-dialog-centered" style="max-width: 90vw; width: 90vw;">
                <div class="modal-content" style="height: 90vh;">
                    <div class="modal-header bg-dark text-white py-2">
                        <h5 class="modal-title" id="dashboardModalLabel">
                            <i class="bi bi-speedometer2 me-2"></i>
                            ${escapeHtml(entityType)} Dashboard
                            ${entityId ? `<span class="badge bg-info ms-2">${escapeHtml(entityId)}</span>` : ''}
                        </h5>
                        <div class="d-flex align-items-center gap-2">
                            <a href="${escapeHtml(dashboardUrl)}" target="_blank" class="btn btn-sm btn-outline-light" title="Open in new tab">
                                <i class="bi bi-box-arrow-up-right"></i>
                            </a>
                            <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                    </div>
                    <div class="modal-body p-0" style="height: calc(90vh - 56px); overflow: hidden; background: #1a1a2e;">
                        <iframe id="dashboardIframe" 
                                src="${escapeHtml(dashboardUrl)}" 
                                style="width: 100%; height: 100%; border: none;"
                                sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
                                allow="fullscreen"
                                referrerpolicy="no-referrer">
                        </iframe>
                    </div>
                </div>
            </div>
        </div>
    `;
    
    // Append modal to body
    document.body.insertAdjacentHTML('beforeend', modalHtml);
    
    // Initialize and show Bootstrap modal
    const modalElement = document.getElementById('dashboardModal');
    const modal = new bootstrap.Modal(modalElement, {
        backdrop: 'static',
        keyboard: true
    });
    
    // Clean up when modal is hidden
    modalElement.addEventListener('hidden.bs.modal', function () {
        modalElement.remove();
    });
    
    modal.show();
}

function findAttributeValue(attributesMap, columnName) {
    if (!attributesMap || !columnName) return null;
    
    const colLower = columnName.toLowerCase();
    // Normalize: remove underscores and hyphens for fuzzy matching
    const colNormalized = colLower.replace(/[_-]/g, '');
    
    // Try exact match first
    for (const [key, value] of attributesMap.entries()) {
        if (key.toLowerCase() === colLower) {
            return value;
        }
    }
    
    // Try partial match (column name might be in predicate URI)
    for (const [key, value] of attributesMap.entries()) {
        const keyLower = key.toLowerCase();
        if (keyLower.includes(colLower) || keyLower.endsWith(colLower)) {
            return value;
        }
    }
    
    // Try normalized match (ignore underscores/hyphens: first_name matches FirstName)
    for (const [key, value] of attributesMap.entries()) {
        const keyNormalized = key.toLowerCase().replace(/[_-]/g, '');
        if (keyNormalized === colNormalized || keyNormalized.includes(colNormalized) || colNormalized.includes(keyNormalized)) {
            return value;
        }
    }
    
    return null;
}
