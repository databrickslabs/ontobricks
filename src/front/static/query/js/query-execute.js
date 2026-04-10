/**
 * OntoBricks - query-execute.js
 * Query execution, results display (Grid.js), filtering, and grouping.
 * Extracted from query.js per code_instructions.txt
 */

async function loadBaseUri() {
    try {
        const response = await fetch('/session-status');
        const data = await response.json();
        const baseUriElement = document.getElementById('baseUriValue');
        const sparqlQuery = document.getElementById('sparqlQuery');
        let baseUri = 'https://databricks-ontology.com/';
        
        // Check if ontology is loaded by checking class_count from session-status
        const hasOntology = data.class_count && data.class_count > 0;
        
        if (hasOntology) {
            const taxResponse = await fetch('/ontology/get-loaded-ontology');
            const taxData = await taxResponse.json();
            
            // The ontology data is directly in taxData.ontology (with base_uri field)
            if (taxData.success && taxData.ontology) {
                baseUri = taxData.ontology.base_uri || baseUri;
                baseUriElement.textContent = baseUri;
                baseUriElement.classList.remove('text-muted');
                baseUriElement.classList.add('text-primary');
            } else {
                baseUriElement.textContent = baseUri;
            }
        } else {
            baseUriElement.textContent = 'No ontology loaded';
            baseUriElement.classList.remove('text-primary');
            baseUriElement.classList.add('text-muted');
        }
        
        if (sparqlQuery && sparqlQuery.value.includes('BASE_URI_PLACEHOLDER')) {
            sparqlQuery.value = sparqlQuery.value.replace('BASE_URI_PLACEHOLDER', baseUri);
        }
    } catch (error) {
        console.error('Error loading base URI:', error);
        document.getElementById('baseUriValue').textContent = 'Error loading';
    }
}

function updateEngineInfo() {
    // Engine selection removed - always use Spark SQL
}

function clearQuery() {
    // Get the current base URI
    const baseUri = document.getElementById('baseUriValue').textContent || 'https://example.org/ontology#';
    
    // Default query template
    const defaultQuery = `PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ont: <${baseUri}>

SELECT ?subject ?predicate ?object
WHERE {
    ?subject ?predicate ?object .
}
LIMIT 500`;
    
    document.getElementById('sparqlQuery').value = defaultQuery;
}

function toggleQueryLimit() {
    const checkbox = document.getElementById('enableQueryLimit');
    const limitInput = document.getElementById('resultLimit');
    if (checkbox && limitInput) {
        limitInput.disabled = !checkbox.checked;
    }
}

async function executeQuery() {
    const query = document.getElementById('sparqlQuery').value.trim();
    const engine = document.getElementById('executionEngine').value;
    const limitCheckbox = document.getElementById('enableQueryLimit');
    const limitEnabled = limitCheckbox ? limitCheckbox.checked : false;
    const limit = limitEnabled ? document.getElementById('resultLimit').value : null;
    
    if (!query) {
        showNotification('Please enter a SPARQL query', 'warning');
        return;
    }
    
    const btn = document.getElementById('queryBtn');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Executing...';
    
    document.getElementById('queryStatus').style.display = 'block';
    document.getElementById('queryStatusText').textContent = 
        engine === 'sansa' ? 'Translating SPARQL to Spark SQL...' : 'Executing locally...';
    
    const startTime = performance.now();
    
    try {
        const response = await fetch('/dtwin/execute', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query, engine, limit: limit ? parseInt(limit) : null })
        });
        
        const result = await response.json();
        const endTime = performance.now();
        
        document.getElementById('queryStatus').style.display = 'none';
        
        if (result.success) {
            queryResults = result.results;
            generatedSql = result.generated_sql;
            
            const count = result.results ? result.results.length : 0;
            var _badge = document.getElementById('resultCountBadge');
            if (_badge) _badge.textContent = count;
            var _rcEl = document.getElementById('resultCount');
            if (_rcEl) _rcEl.textContent = count + ' results';
            
            displayResults(result);
            displayGeneratedSql(result.generated_sql);
            document.getElementById('downloadBtn').disabled = false;
            
            // Debug: Log first few results to see structure
            console.log('=== QUERY RESULTS DEBUG ===');
            console.log('Columns:', result.columns);
            console.log('First 3 results:', result.results?.slice(0, 3));
            
            // Switch to visualization - the graph will be built from the view
            // (query execution also creates/updates the mv_query_result view)
            graphJustBuilt = true;  // Flag to indicate fresh query execution
            console.log('>>> SWITCHING TO KNOWLEDGE GRAPH TAB');
            SidebarNav.switchTo('sigmagraph');
            
            // Log execution info
            console.log(`Query executed in ${((endTime - startTime) / 1000).toFixed(2)}s using ${engine === 'sansa' ? 'Spark SQL' : 'RDFLib'}`);
            if (result.tables_queried) {
                console.log('Tables queried:', result.tables_queried.join(', '));
            }
        } else {
            displayError(result.message, result.generated_sql);
        }
    } catch (error) {
        document.getElementById('queryStatus').style.display = 'none';
        displayError('Error: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-play-fill"></i> Execute Query';
    }
}

// Grid.js instance
let resultsGrid = null;
let currentResultData = null;  // Store current result for re-grouping

function displayResults(result) {
    console.log('[displayResults] Called with', result?.results?.length || 0, 'results');
    
    const container = document.getElementById('resultsContainer');
    
    // Critical: Check if container exists
    if (!container) {
        console.error('[displayResults] ERROR: resultsContainer not found in DOM!');
        return;
    }
    
    // IMMEDIATELY clear the container to remove any initial message
    // This ensures we never show stale content
    container.innerHTML = '<div class="text-center py-4"><i class="bi bi-hourglass-split"></i> Loading results...</div>';
    
    // Store results for entity details panel and grouping
    lastQueryResults = result;
    currentResultData = result;
    
    // Clear old graph data so visualization will rebuild with new results
    if (typeof d3NodesData !== 'undefined') d3NodesData = [];
    if (typeof d3LinksData !== 'undefined') d3LinksData = [];
    
    if (!result || !result.results || result.results.length === 0) {
        // Destroy existing grid if any
        if (resultsGrid) {
            try { resultsGrid.destroy(); } catch(e) {}
            resultsGrid = null;
        }
        container.innerHTML = `
            <div class="results-empty-state">
                <i class="bi bi-search icon-lg"></i>
                <p class="mt-2">No results found</p>
            </div>`;
        // Reset grouping dropdown
        const groupSelect = document.getElementById('groupByColumn');
        if (groupSelect) groupSelect.innerHTML = '<option value="">No grouping</option>';
        return;
    }
    
    const rows = result.results;
    const columns = result.columns || Object.keys(rows[0] || {});
    
    console.log('[displayResults] Rendering', rows.length, 'rows with columns:', columns);
    
    // Populate grouping dropdown
    const groupSelect = document.getElementById('groupByColumn');
    if (groupSelect) {
        groupSelect.innerHTML = '<option value="">No grouping</option>';
        columns.forEach(col => {
            groupSelect.innerHTML += `<option value="${col}">${col}</option>`;
        });
    }
    
    // Display without grouping initially
    try {
        renderResultsGrid(rows, columns, container);
        console.log('[displayResults] Grid rendered successfully');
    } catch (error) {
        console.error('[displayResults] Error rendering grid:', error);
        // Fallback: use simple HTML table
        try {
            renderFallbackTable(rows, columns, (v) => v ? String(v) : '', container, 100);
        } catch (e2) {
            container.innerHTML = `
                <div class="results-empty-state">
                    <i class="bi bi-exclamation-triangle icon-lg text-warning"></i>
                    <p class="mt-2">Error displaying ${rows.length} results. Check console for details.</p>
                </div>`;
        }
    }
}

function renderResultsGrid(rows, columns, container, groupByCol = null) {
    console.log('[renderResultsGrid] Starting with', rows.length, 'rows,', columns.length, 'columns');
    
    // Format cell values for display
    function formatCell(value) {
        if (!value) return '';
        const strValue = String(value);
        if (strValue.startsWith('http://') || strValue.startsWith('https://')) {
            const localName = strValue.includes('#') ? strValue.split('#').pop() : strValue.split('/').pop();
            return localName || strValue;
        }
        return strValue;
    }
    
    // Clear container - this removes the initial "Execute a query to see results" message
    console.log('[renderResultsGrid] Clearing container');
    container.innerHTML = '';
    
    // Destroy existing grid if any
    if (resultsGrid) {
        resultsGrid.destroy();
        resultsGrid = null;
    }
    
    if (groupByCol) {
        // Grouped display
        renderGroupedResults(rows, columns, container, groupByCol, formatCell);
    } else {
        // Flat display with Grid.js and column filters
        
        // Store original data for filtering
        window._resultsOriginalData = rows;
        window._resultsColumns = columns;
        window._resultsFormatCell = formatCell;
        
        // Get unique values for each column (for filter dropdowns)
        window._columnValues = columns.map((col, idx) => {
            const values = new Set();
            rows.forEach(row => {
                const val = formatCell(row[col]);
                if (val) values.add(val);
            });
            return Array.from(values).sort((a, b) => a.localeCompare(b));
        });
        
        // Render grid with data directly into container (no intermediate div)
        // This ensures CSS selectors like #resultsContainer > .gridjs-container work correctly
        renderGridWithData(rows, columns, formatCell, container);
    }
}

function renderGridWithData(rows, columns, formatCell, container, preserveFilters = false) {
    console.log('[renderGridWithData] Starting render for', rows.length, 'rows');
    
    try {
        const gridData = rows.map(row => columns.map(col => formatCell(row[col])));
        
        // Get pagination limit from dropdown
        const paginationSelect = document.getElementById('paginationLimit');
        const paginationLimit = paginationSelect ? parseInt(paginationSelect.value) : 20;
        
        // Save current filter values if preserving
        let savedFilters = {};
        if (preserveFilters) {
            document.querySelectorAll('.column-header-filter').forEach(f => {
                savedFilters[f.dataset.column] = f.value;
            });
        }
        
        // Build Grid.js config with custom header rendering
        const columnValues = window._columnValues || [];
        
        const gridConfig = {
            columns: columns.map((col, idx) => ({
                id: `col_${idx}`,  // Required: unique ID for each column
                name: gridjs.html(`
                    <div class="column-header-content">
                        <span class="column-title">${escapeHtml(col)}</span>
                        ${buildFilterElement(idx, columnValues[idx] || [], savedFilters[idx])}
                    </div>
                `),
                sort: true
            })),
            data: gridData,
            sort: true,
            resizable: true,
            fixedHeader: true,
            autoWidth: true
        };
        
        // Add pagination if limit > 0
        if (paginationLimit > 0) {
            gridConfig.pagination = {
                limit: paginationLimit,
                summary: true
            };
        }
        
        // Destroy existing grid
        if (resultsGrid) {
            resultsGrid.destroy();
            resultsGrid = null;
        }
        
        // Clear container and render
        container.innerHTML = '';
        
        // Check if gridjs is available
        if (typeof gridjs === 'undefined' || !gridjs.Grid) {
            console.error('[Results] Grid.js library not loaded');
            renderFallbackTable(rows, columns, formatCell, container, paginationLimit);
            return;
        }
        
        resultsGrid = new gridjs.Grid(gridConfig).render(container);
        
        // Use event delegation for filter changes (Grid.js may strip inline handlers)
        resultsGrid.on('ready', () => {
            // Remove any existing delegated handler
            container.removeEventListener('change', handleFilterChange);
            container.removeEventListener('input', handleFilterChange);
            // Add delegated event handlers
            container.addEventListener('change', handleFilterChange);
            container.addEventListener('input', handleFilterChange);
        });
    } catch (error) {
        console.error('[Results] Error rendering Grid.js:', error);
        // Fallback to simple HTML table if Grid.js fails
        renderFallbackTable(rows, columns, formatCell, container, 20);
    }
}

// Fallback table renderer if Grid.js fails
function renderFallbackTable(rows, columns, formatCell, container, limit) {
    const displayRows = limit > 0 ? rows.slice(0, limit) : rows;
    
    let html = `
        <div class="table-responsive" style="height: 100%; overflow: auto;">
            <table class="table table-sm table-striped table-hover">
                <thead class="table-light sticky-top">
                    <tr>${columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}</tr>
                </thead>
                <tbody>
    `;
    
    displayRows.forEach(row => {
        html += '<tr>';
        columns.forEach(col => {
            html += `<td>${escapeHtml(formatCell(row[col]))}</td>`;
        });
        html += '</tr>';
    });
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    if (limit > 0 && rows.length > limit) {
        html += `<div class="text-muted small p-2">Showing ${limit} of ${rows.length} rows</div>`;
    }
    
    container.innerHTML = html;
}

let isFilteringInProgress = false;

function handleFilterChange(e) {
    if (e.target.classList.contains('column-header-filter')) {
        e.stopPropagation();
        // Only auto-apply for select dropdowns; text inputs use the explicit Apply button / Enter key
        if (e.target.tagName === 'SELECT' && !isFilteringInProgress) {
            applyColumnFilters();
        }
    }
}

function buildFilterElement(idx, values, savedValue) {
    if (values.length > 100) {
        // Use text input with an explicit Apply button (no filtering on every keystroke)
        return `
            <div class="input-group input-group-sm" onclick="event.stopPropagation()">
                <input type="text" 
                       class="form-control form-control-sm column-header-filter" 
                       data-column="${idx}"
                       data-type="text"
                       placeholder="Filter..."
                       value="${savedValue || ''}"
                       onclick="event.stopPropagation()"
                       onkeydown="if(event.key==='Enter'){event.preventDefault();applyColumnFilters();}">
                <button class="btn btn-outline-secondary btn-sm" type="button"
                        onclick="event.stopPropagation();applyColumnFilters();"
                        title="Apply filter">
                    <i class="bi bi-funnel-fill"></i>
                </button>
            </div>
        `;
    }
    
    const optionsHtml = values.map(v => {
        const selected = savedValue === v ? 'selected' : '';
        const displayVal = v.length > 30 ? v.substring(0, 30) + '...' : v;
        return `<option value="${escapeHtml(v)}" ${selected}>${escapeHtml(displayVal)}</option>`;
    }).join('');
    
    return `
        <select class="form-select form-select-sm column-header-filter" 
                data-column="${idx}"
                data-type="select"
                onclick="event.stopPropagation()"
                onchange="applyColumnFilters()">
            <option value="" ${!savedValue ? 'selected' : ''}>All (${values.length})</option>
            ${optionsHtml}
        </select>
    `;
}

function applyColumnFilters() {
    if (isFilteringInProgress) return;
    isFilteringInProgress = true;
    
    const filters = document.querySelectorAll('.column-header-filter');
    
    const originalData = window._resultsOriginalData;
    const columns = window._resultsColumns;
    const formatCell = window._resultsFormatCell;
    
    if (!originalData || !columns) {
        isFilteringInProgress = false;
        return;
    }
    
    // Build filter criteria from each filter element
    const filterCriteria = Array.from(filters).map(f => ({
        value: f.value.trim(),
        type: f.dataset.type || 'select',
        column: parseInt(f.dataset.column)
    }));
    
    // Check if any filter is active
    const hasActiveFilters = filterCriteria.some(c => c.value);
    
    // Filter rows
    const filteredRows = originalData.filter(row => {
        return filterCriteria.every(criteria => {
            if (!criteria.value) return true;
            const cellValue = formatCell(row[columns[criteria.column]]);
            
            if (criteria.type === 'text') {
                // Text input: partial match (case-insensitive)
                return cellValue.toLowerCase().includes(criteria.value.toLowerCase());
            } else {
                // Select dropdown: exact match
                return cellValue === criteria.value;
            }
        });
    });
    
    var _rcEl = document.getElementById('resultCount');
    if (_rcEl) {
        if (hasActiveFilters) {
            _rcEl.textContent = `${filteredRows.length} of ${originalData.length} results`;
        } else {
            _rcEl.textContent = `${originalData.length} results`;
        }
    }
    
    // Re-render grid preserving filter values
    const resultsContainer = document.getElementById('resultsContainer');
    if (resultsContainer) {
        renderGridWithData(filteredRows, columns, formatCell, resultsContainer, true);
    }
    
    // Reset flag after a short delay to allow grid to render
    setTimeout(() => { isFilteringInProgress = false; }, 200);
}

function clearColumnFilters() {
    const filters = document.querySelectorAll('.column-header-filter');
    filters.forEach(f => {
        if (f.tagName === 'SELECT') {
            f.selectedIndex = 0;
        } else {
            f.value = '';
        }
    });
    
    // Reset to original data
    const originalData = window._resultsOriginalData;
    const columns = window._resultsColumns;
    const formatCell = window._resultsFormatCell;
    
    if (originalData && columns) {
        var _rcEl2 = document.getElementById('resultCount');
        if (_rcEl2) _rcEl2.textContent = `${originalData.length} results`;
        const resultsContainer = document.getElementById('resultsContainer');
        if (resultsContainer) {
            renderGridWithData(originalData, columns, formatCell, resultsContainer, false);
        }
    }
}

function renderGroupedResults(rows, columns, container, groupByCol, formatCell) {
    // Group rows by the selected column
    const groups = new Map();
    rows.forEach(row => {
        const key = formatCell(row[groupByCol]) || '(empty)';
        if (!groups.has(key)) {
            groups.set(key, []);
        }
        groups.get(key).push(row);
    });
    
    // Sort groups by key
    const sortedGroups = Array.from(groups.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    
    // Get other columns (excluding the group column)
    const otherColumns = columns.filter(c => c !== groupByCol);
    
    // Build grouped HTML
    let html = '<div class="grouped-results">';
    
    sortedGroups.forEach(([groupKey, groupRows], idx) => {
        const isExpanded = idx < 5; // Expand first 5 groups by default
        html += `
            <div class="result-group">
                <div class="group-header" onclick="toggleResultGroup(this)">
                    <i class="bi bi-chevron-${isExpanded ? 'down' : 'right'} group-chevron"></i>
                    <span class="group-key">${escapeHtml(groupKey)}</span>
                    <span class="badge bg-secondary ms-2">${groupRows.length}</span>
                </div>
                <div class="group-content" style="display: ${isExpanded ? 'block' : 'none'};">
                    <table class="table table-sm table-hover mb-0">
                        <thead>
                            <tr>
                                ${otherColumns.map(c => `<th>${escapeHtml(c)}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${groupRows.map(row => `
                                <tr>
                                    ${otherColumns.map(c => `<td>${escapeHtml(formatCell(row[c]))}</td>`).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    });
    
    html += '</div>';
    html += `<div class="text-muted small p-2">Grouped by <strong>${escapeHtml(groupByCol)}</strong>: ${sortedGroups.length} groups, ${rows.length} total rows</div>`;
    
    container.innerHTML = html;
}

function toggleResultGroup(header) {
    const content = header.nextElementSibling;
    const chevron = header.querySelector('.group-chevron');
    
    if (content.style.display === 'none') {
        content.style.display = 'block';
        chevron.classList.remove('bi-chevron-right');
        chevron.classList.add('bi-chevron-down');
    } else {
        content.style.display = 'none';
        chevron.classList.remove('bi-chevron-down');
        chevron.classList.add('bi-chevron-right');
    }
}

function applyGrouping() {
    const groupByCol = document.getElementById('groupByColumn').value;
    const container = document.getElementById('resultsContainer');
    
    if (!currentResultData || !currentResultData.results) return;
    
    const rows = currentResultData.results;
    const columns = currentResultData.columns || Object.keys(rows[0] || {});
    
    renderResultsGrid(rows, columns, container, groupByCol || null);
}

function displayGeneratedSql(sql) {
    const container = document.getElementById('sqlContent');
    
    // sqlContent was moved to Mapping page, skip if not present
    if (!container) return;
    
    if (!sql) {
        container.innerHTML = `
            <div class="d-flex flex-column align-items-center justify-content-center h-100 text-muted">
                <i class="bi bi-code-square icon-xl"></i>
                <p class="mt-2">No SQL generated</p>
            </div>`;
        return;
    }
    
    let highlighted = escapeHtml(sql)
        .replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|ON|AND|OR|AS|UNION|ALL|DISTINCT|LIMIT|ORDER|BY|GROUP|HAVING|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TABLE|INDEX|VIEW|CASE|WHEN|THEN|ELSE|END|NULL|NOT|IN|LIKE|BETWEEN|IS|EXISTS|CAST|CONCAT|stack)\b/gi, '<span class="sql-keyword">$1</span>')
        .replace(/\b(\d+)\b/g, '<span class="sql-number">$1</span>')
        .replace(/'([^']*)'/g, "'<span class=\"sql-string\">$1</span>'");
    
    container.innerHTML = `<pre class="sql-code">${highlighted}</pre>`;
}

function displayError(message, sql = null) {
    var _errContainer = document.getElementById('resultsContainer');
    if (_errContainer) {
        _errContainer.innerHTML = `
            <div class="results-empty-state p-4">
                <div class="alert alert-danger w-100" style="max-width: 600px;">
                    <i class="bi bi-exclamation-triangle"></i> ${escapeHtml(message)}
                </div>
            </div>`;
    }

    if (sql) {
        displayGeneratedSql(sql);
    }

    var _rcEl = document.getElementById('resultCount');
    if (_rcEl) _rcEl.textContent = 'Error';
    var _badge = document.getElementById('resultCountBadge');
    if (_badge) _badge.textContent = '!';
}
