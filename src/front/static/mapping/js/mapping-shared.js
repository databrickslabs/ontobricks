/**
 * OntoBricks - mapping-shared.js
 * Extracted from mapping templates per code_instructions.txt
 */

// ==========================================================================
// SHARED MAPPING POPUP UTILITIES
// ==========================================================================

/**
 * Remove LIMIT clause from SQL query before saving.
 * The LIMIT is only used for preview, not for the final saved query.
 */
function stripLimitClause(sql) {
    if (!sql) return sql;
    // Remove LIMIT clause (case-insensitive) - matches LIMIT followed by a number
    return sql.replace(/\s+LIMIT\s+\d+\s*$/i, '').trim();
}

// Close all mapping dropdowns when clicking outside
document.addEventListener('click', () => {
    document.querySelectorAll('.mapping-dropdown.show').forEach(d => d.classList.remove('show'));
});

// ==========================================================================
// SQL WIZARD BASE CLASS (Metadata-based)
// Creates a reusable wizard using project metadata tables
// ==========================================================================
class SQLWizardBase {
    constructor(config) {
        // Element IDs configuration
        this.ids = {
            llmEndpoint: config.llmEndpointId,
            tableList: config.tableListId,
            prompt: config.promptId,
            promptOverlay: config.promptOverlayId,
            schemaInfo: config.schemaInfoId,
            tableCount: config.tableCountId,
            selectedCount: config.selectedCountId,
            limit: config.limitId,
            generateBtn: config.generateBtnId,
            result: config.resultId,
            generatedSql: config.generatedSqlId,
            warning: config.warningId,
            warningText: config.warningTextId,
            error: config.errorId,
            errorText: config.errorTextId,
            copyBtn: config.copyBtnId,
            useBtn: config.useBtnId,
            refreshBtn: config.refreshBtnId,
            noMetadataWarning: config.noMetadataWarningId
        };
        
        this.metadata = null;  // Project metadata (catalog, schema, tables)
        this.selectedTables = {};  // Track selected tables
        this.schemaContext = null;
        this.mappingType = config.mappingType || null;  // 'entity', 'relationship', or null
        this.onUseSql = config.onUseSql || (() => {});
        
        // Bind methods
        this.loadEndpoints = this.loadEndpoints.bind(this);
        this.loadMetadataTables = this.loadMetadataTables.bind(this);
        this.toggleTable = this.toggleTable.bind(this);
        this.selectAllTables = this.selectAllTables.bind(this);
        this.buildSchemaContext = this.buildSchemaContext.bind(this);
        this.updateGenerateButton = this.updateGenerateButton.bind(this);
        this.generateSql = this.generateSql.bind(this);
        this.useGeneratedSql = this.useGeneratedSql.bind(this);
        this.reset = this.reset.bind(this);
    }
    
    async loadEndpoints() {
        const hiddenInput = document.getElementById(this.ids.llmEndpoint);
        const displayEl = document.getElementById(this.ids.llmEndpoint + 'Display');
        
        if (displayEl) {
            displayEl.innerHTML = '<span class="text-muted">Loading...</span>';
        }
        
        try {
            const response = await fetch('/project/info', { credentials: 'same-origin' });
            const data = await response.json();
            
            if (data.success && data.info?.llm_endpoint) {
                if (hiddenInput) hiddenInput.value = data.info.llm_endpoint;
                if (displayEl) displayEl.innerHTML = `<i class="bi bi-robot me-1"></i>${data.info.llm_endpoint}`;
            } else {
                if (hiddenInput) hiddenInput.value = '';
                if (displayEl) displayEl.innerHTML = '<span class="text-warning"><i class="bi bi-exclamation-triangle me-1"></i>Not configured - set in Project settings</span>';
            }
        } catch (e) {
            if (hiddenInput) hiddenInput.value = '';
            if (displayEl) displayEl.innerHTML = '<span class="text-danger"><i class="bi bi-x-circle me-1"></i>Error loading</span>';
        }
        this.updateGenerateButton();
    }
    
    async loadMetadataTables() {
        const tableListEl = document.getElementById(this.ids.tableList);
        const schemaInfoEl = document.getElementById(this.ids.schemaInfo);
        const noMetadataEl = document.getElementById(this.ids.noMetadataWarning);
        
        if (tableListEl) tableListEl.innerHTML = '<div class="text-muted small p-2">Loading...</div>';
        if (schemaInfoEl) schemaInfoEl.classList.add('hidden-initial');
        if (noMetadataEl) noMetadataEl.classList.add('hidden-initial');
        
        try {
            const response = await fetch('/project/metadata', { credentials: 'same-origin' });
            const data = await response.json();
            
            if (data.success && data.has_metadata && data.metadata) {
                this.metadata = data.metadata;
                const tables = data.metadata.tables || [];
                
                // Select all tables by default
                this.selectedTables = {};
                tables.forEach(t => {
                    this.selectedTables[t.name] = true;
                });
                
                // Render table list with checkboxes
                this.renderTableList(tables);
                
                // Update counts
                if (document.getElementById(this.ids.tableCount)) {
                    document.getElementById(this.ids.tableCount).textContent = tables.length;
                }
                this.updateSelectedCount();
                
                if (schemaInfoEl) schemaInfoEl.classList.remove('hidden-initial');
                
                // Build schema context from selected tables
                this.buildSchemaContext();
            } else {
                this.metadata = null;
                this.selectedTables = {};
                if (tableListEl) tableListEl.innerHTML = '';
                if (noMetadataEl) noMetadataEl.classList.remove('hidden-initial');
            }
        } catch (e) {
            console.error('[SQLWizard] Error loading metadata:', e);
            if (tableListEl) tableListEl.innerHTML = '<div class="text-danger small p-2">Error loading metadata</div>';
        }
        
        this.updateGenerateButton();
    }
    
    renderTableList(tables) {
        const tableListEl = document.getElementById(this.ids.tableList);
        if (!tableListEl) return;
        
        if (tables.length === 0) {
            tableListEl.innerHTML = '<div class="text-muted small p-2">No tables in metadata</div>';
            return;
        }
        
        const html = tables.map(table => {
            const isSelected = this.selectedTables[table.name] !== false;
            const colCount = table.columns?.length || 0;
            const desc = table.comment || table.description || '';
            // Use full_name - it's required in the new structure
            const fullName = table.full_name || table.name;
            
            return `
                <div class="form-check wizard-table-item ${isSelected ? '' : 'text-muted'}" data-table="${table.name}">
                    <input class="form-check-input wizard-table-checkbox" type="checkbox" 
                           id="wizTable_${table.name}" value="${table.name}" 
                           ${isSelected ? 'checked' : ''}
                           onchange="this.closest('.wizard-table-item').classList.toggle('text-muted', !this.checked)">
                    <label class="form-check-label small" for="wizTable_${table.name}" title="${fullName}">
                        <strong>${fullName}</strong>
                        <span class="badge bg-secondary ms-1">${colCount}</span>
                        ${desc ? `<span class="text-muted ms-1">- ${desc.substring(0, 40)}${desc.length > 40 ? '...' : ''}</span>` : ''}
                    </label>
                </div>
            `;
        }).join('');
        
        tableListEl.innerHTML = html;
        
        // Add event listeners
        tableListEl.querySelectorAll('.wizard-table-checkbox').forEach(cb => {
            cb.addEventListener('change', (e) => {
                this.toggleTable(e.target.value, e.target.checked);
            });
        });
    }
    
    toggleTable(tableName, isSelected) {
        this.selectedTables[tableName] = isSelected;
        this.updateSelectedCount();
        this.buildSchemaContext();
        this.updateGenerateButton();
    }
    
    selectAllTables(selectAll) {
        if (!this.metadata?.tables) return;
        
        this.metadata.tables.forEach(t => {
            this.selectedTables[t.name] = selectAll;
        });
        
        // Update checkboxes
        const tableListEl = document.getElementById(this.ids.tableList);
        if (tableListEl) {
            tableListEl.querySelectorAll('.wizard-table-checkbox').forEach(cb => {
                cb.checked = selectAll;
                cb.closest('.wizard-table-item').classList.toggle('text-muted', !selectAll);
            });
        }
        
        this.updateSelectedCount();
        this.buildSchemaContext();
        this.updateGenerateButton();
    }
    
    updateSelectedCount() {
        const selectedCount = Object.values(this.selectedTables).filter(v => v).length;
        const totalCount = Object.keys(this.selectedTables).length;
        
        const countEl = document.getElementById(this.ids.selectedCount);
        if (countEl) {
            countEl.textContent = `${selectedCount}/${totalCount} selected`;
            countEl.className = selectedCount === 0 ? 'text-danger small' : 'text-muted small';
        }
    }
    
    buildSchemaContext() {
        if (!this.metadata?.tables) {
            this.schemaContext = null;
            return;
        }
        
        // Build context from selected tables only
        const selectedTableData = this.metadata.tables.filter(t => this.selectedTables[t.name] === true);
        
        // No separate catalog/schema - rely on full_name in each table
        this.schemaContext = {
            tables: selectedTableData.map(t => ({
                name: t.name,
                full_name: t.full_name || t.name,
                comment: t.comment || t.description || '',
                columns: (t.columns || []).map(c => ({
                    name: c.name || c.col_name,
                    type: c.type || c.data_type,
                    comment: c.comment || ''
                }))
            }))
        };
    }
    
    updateGenerateButton() {
        const btn = document.getElementById(this.ids.generateBtn);
        if (!btn) return;
        
        const endpoint = document.getElementById(this.ids.llmEndpoint)?.value;
        const prompt = document.getElementById(this.ids.prompt)?.value?.trim();
        const hasSelectedTables = Object.values(this.selectedTables).some(v => v);
        
        btn.disabled = !endpoint || !prompt || !hasSelectedTables;
    }
    
    async generateSql() {
        const btn = document.getElementById(this.ids.generateBtn);
        const spinner = btn.querySelector('.spinner-border');
        const resultDiv = document.getElementById(this.ids.result);
        const errorDiv = document.getElementById(this.ids.error);
        const warningDiv = document.getElementById(this.ids.warning);
        
        resultDiv.classList.add('hidden-initial');
        errorDiv.classList.add('hidden-initial');
        warningDiv.classList.add('hidden-initial');
        
        spinner.classList.remove('d-none');
        btn.disabled = true;
        
        try {
            // Build schema context from selected tables
            this.buildSchemaContext();
            
            const response = await fetch('/mapping/wizard/generate-sql', {
                method: 'POST',
                credentials: 'same-origin',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    endpoint_name: document.getElementById(this.ids.llmEndpoint).value,
                    catalog: this.metadata?.catalog || '',
                    schema: this.metadata?.schema || '',
                    prompt: document.getElementById(this.ids.prompt).value,
                    limit: parseInt(document.getElementById(this.ids.limit).value) || 10,
                    validate_plan: false,
                    schema_context: this.schemaContext,  // Pass pre-built context
                    mapping_type: this.mappingType  // 'entity', 'relationship', or null
                })
            });
            
            const data = await response.json();
            
            if (data.success && data.sql) {
                document.getElementById(this.ids.generatedSql).textContent = data.sql;
                resultDiv.classList.remove('hidden-initial');
                
                if (data.warnings && data.warnings.length > 0) {
                    document.getElementById(this.ids.warningText).textContent = data.warnings.join('; ');
                    warningDiv.classList.remove('hidden-initial');
                }
            } else {
                document.getElementById(this.ids.errorText).textContent = data.error || 'Failed to generate SQL';
                errorDiv.classList.remove('hidden-initial');
            }
        } catch (e) {
            document.getElementById(this.ids.errorText).textContent = e.message;
            errorDiv.classList.remove('hidden-initial');
        } finally {
            spinner.classList.add('d-none');
            this.updateGenerateButton();
        }
    }
    
    useGeneratedSql() {
        const sql = document.getElementById(this.ids.generatedSql).textContent;
        if (sql) {
            this.onUseSql(sql);
        }
    }
    
    reset() {
        this.schemaContext = null;
        const promptEl = document.getElementById(this.ids.prompt);
        if (promptEl) promptEl.value = '';
        
        const resultEl = document.getElementById(this.ids.result);
        if (resultEl) resultEl.classList.add('hidden-initial');
        
        const errorEl = document.getElementById(this.ids.error);
        if (errorEl) errorEl.classList.add('hidden-initial');
        
        const warningEl = document.getElementById(this.ids.warning);
        if (warningEl) warningEl.classList.add('hidden-initial');
        
        const sqlEl = document.getElementById(this.ids.generatedSql);
        if (sqlEl) sqlEl.textContent = '';
        
        const overlayEl = document.getElementById(this.ids.promptOverlay);
        if (overlayEl) overlayEl.style.display = 'none';
    }
    
    init(defaultPrompt = '') {
        this.reset();
        if (defaultPrompt) {
            document.getElementById(this.ids.prompt).value = defaultPrompt;
        }
        this.loadEndpoints();
        this.loadMetadataTables();
    }
    
    // Setup event listeners
    setupEventListeners() {
        const refreshBtn = document.getElementById(this.ids.refreshBtn);
        const promptInput = document.getElementById(this.ids.prompt);
        const generateBtn = document.getElementById(this.ids.generateBtn);
        const useBtn = document.getElementById(this.ids.useBtn);
        const copyBtn = document.getElementById(this.ids.copyBtn);
        
        if (refreshBtn) refreshBtn.addEventListener('click', this.loadEndpoints);
        if (promptInput) promptInput.addEventListener('input', this.updateGenerateButton);
        if (generateBtn) generateBtn.addEventListener('click', this.generateSql);
        if (useBtn) useBtn.addEventListener('click', this.useGeneratedSql);
        
        if (copyBtn) {
            copyBtn.addEventListener('click', () => {
                const sql = document.getElementById(this.ids.generatedSql).textContent;
                navigator.clipboard.writeText(sql).then(() => {
                    showNotification('SQL copied to clipboard', 'success', 1500);
                });
            });
        }
    }
}

// ==========================================================================
// TAB NAVIGATION HELPER
// Creates reusable tab navigation for modals
// ==========================================================================
class TabNavigator {
    constructor(config) {
        this.tabsContainerId = config.tabsContainerId;
        this.modalId = config.modalId;
        this.tabs = config.tabs; // { id, paneId, disabled }
    }
    
    enableTab(tabId) {
        const tab = document.getElementById(tabId);
        if (tab) {
            tab.disabled = false;
            tab.classList.remove('disabled');
        }
    }
    
    disableTab(tabId) {
        const tab = document.getElementById(tabId);
        if (tab) {
            tab.disabled = true;
            tab.classList.add('disabled');
        }
    }
    
    switchTo(tabId) {
        const tab = document.getElementById(tabId);
        if (tab && !tab.disabled) {
            const paneId = tab.dataset.bsTarget;
            const pane = document.querySelector(paneId);
            
            const allTabs = document.querySelectorAll(`#${this.tabsContainerId} .nav-link`);
            const allPanes = document.querySelectorAll(`#${this.modalId} .tab-pane`);
            
            allTabs.forEach(t => t.classList.remove('active'));
            allPanes.forEach(p => p.classList.remove('show', 'active'));
            
            tab.classList.add('active');
            if (pane) {
                pane.classList.add('show', 'active');
            }
        }
    }
    
    reset(activeTabId) {
        this.tabs.forEach(tabConfig => {
            const tab = document.getElementById(tabConfig.id);
            const pane = document.querySelector(`#${tabConfig.paneId}`);
            
            if (tab) {
                tab.disabled = tabConfig.disabled || false;
                tab.classList.remove('active');
            }
            if (pane) {
                pane.classList.remove('show', 'active');
            }
        });
        
        // Activate the specified tab
        const activeTab = document.getElementById(activeTabId);
        const activePane = document.querySelector(`#${this.tabs.find(t => t.id === activeTabId)?.paneId}`);
        
        if (activeTab) activeTab.classList.add('active');
        if (activePane) activePane.classList.add('show', 'active');
    }
}

// ==========================================================================
// COLUMN MAPPING TABLE RENDERER
// Shared logic for rendering mapping tables with dropdown menus
// ==========================================================================
function renderMappingTableDropdown(th, column, mappingState, onMappingChange) {
    const dropdown = th.querySelector('.mapping-dropdown');
    
    th.addEventListener('click', (e) => {
        // Close other dropdowns
        document.querySelectorAll('.mapping-dropdown.show').forEach(d => d.classList.remove('show'));
        
        // Position dropdown using fixed positioning
        const rect = th.getBoundingClientRect();
        dropdown.style.top = (rect.bottom + 2) + 'px';
        dropdown.style.left = rect.left + 'px';
        
        // Check if dropdown would go off screen to the right
        const dropdownWidth = 180;
        if (rect.left + dropdownWidth > window.innerWidth) {
            dropdown.style.left = (window.innerWidth - dropdownWidth - 10) + 'px';
        }
        
        // Check if dropdown would go off screen at the bottom
        const dropdownHeight = dropdown.scrollHeight || 200;
        if (rect.bottom + dropdownHeight > window.innerHeight) {
            dropdown.style.top = (rect.top - dropdownHeight - 2) + 'px';
        }
        
        dropdown.classList.toggle('show');
        e.stopPropagation();
    });
    
    dropdown.querySelectorAll('.dropdown-item:not(.disabled)').forEach(item => {
        item.addEventListener('click', (e) => {
            e.stopPropagation();
            const action = item.dataset.action;
            const attr = item.dataset.attr;
            onMappingChange(column, action, attr);
            dropdown.classList.remove('show');
        });
    });
}

// Export utilities to global scope
window.SQLWizardBase = SQLWizardBase;
window.TabNavigator = TabNavigator;
window.renderMappingTableDropdown = renderMappingTableDropdown;
