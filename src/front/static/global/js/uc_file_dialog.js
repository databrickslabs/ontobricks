/**
 * Unity Catalog File Dialog - Shared component for Load and Save operations
 * Usage:
 *   UCFileDialog.open({ mode: 'load', title: 'Load Taxonomy', extensions: ['.owl', '.ttl'], onSelect: (file) => {...} })
 *   UCFileDialog.open({ mode: 'save', title: 'Save Mapping', defaultFilename: 'mapping.ttl', onSave: (location) => {...} })
 */
const UCFileDialog = {
    // State
    mode: 'load', // 'load' or 'save'
    callback: null,
    extensions: ['.ttl'],
    defaultFilename: '',
    currentWarehouseId: null,
    
    /**
     * Get the default warehouse ID from domain / app config
     * @returns {Promise<string|null>} The default warehouse ID or null
     */
    getDefaultWarehouseId: async function() {
        try {
            const response = await fetch('/settings/current', { credentials: 'same-origin' });
            const config = await response.json();
            return config.warehouse_id || null;
        } catch (error) {
            console.error('Error getting default warehouse:', error);
            return null;
        }
    },
    
    /**
     * Load available SQL warehouses
     */
    _loadWarehouses: async function() {
        const select = document.getElementById('ucFileDialogWarehouse');
        const catalogSelect = document.getElementById('ucFileDialogCatalog');
        
        if (!select) return;
        
        select.innerHTML = '<option value="">Loading warehouses...</option>';
        catalogSelect.innerHTML = '<option value="">Select warehouse first...</option>';
        catalogSelect.disabled = true;
        
        try {
            const response = await fetch('/settings/warehouses', { credentials: 'same-origin' });
            const data = await response.json();
            
            select.innerHTML = '<option value="">Select SQL Warehouse...</option>';
            
            if (data.warehouses && data.warehouses.length > 0) {
                // Get default warehouse ID
                const defaultWarehouseId = await this.getDefaultWarehouseId();
                
                data.warehouses.forEach(wh => {
                    const option = document.createElement('option');
                    option.value = wh.id;
                    option.textContent = `${wh.name} (${wh.state || 'unknown'})`;
                    
                    // Select the default warehouse if it matches
                    if (defaultWarehouseId && wh.id === defaultWarehouseId) {
                        option.selected = true;
                    }
                    
                    select.appendChild(option);
                });
                
                // If a default warehouse was selected, trigger the change event
                if (defaultWarehouseId && select.value) {
                    await this._onWarehouseChange(select.value);
                }
            } else {
                select.innerHTML = '<option value="">No warehouses available</option>';
            }
        } catch (error) {
            console.error('Error loading warehouses:', error);
            select.innerHTML = '<option value="">Error loading warehouses</option>';
        }
    },
    
    /**
     * Handle warehouse selection change
     */
    _onWarehouseChange: async function(warehouseId) {
        const catalogSelect = document.getElementById('ucFileDialogCatalog');
        const schemaSelect = document.getElementById('ucFileDialogSchema');
        const volumeSelect = document.getElementById('ucFileDialogVolume');
        
        if (!warehouseId) {
            catalogSelect.innerHTML = '<option value="">Select warehouse first...</option>';
            catalogSelect.disabled = true;
            schemaSelect.innerHTML = '<option value="">Select catalog first...</option>';
            schemaSelect.disabled = true;
            volumeSelect.innerHTML = '<option value="">Select schema first...</option>';
            volumeSelect.disabled = true;
            return;
        }
        
        this.currentWarehouseId = warehouseId;
        
        // Save the selected warehouse to config
        try {
            await fetch('/settings/save', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ warehouse_id: warehouseId }),
                credentials: 'same-origin'
            });
        } catch (error) {
            console.error('Error saving warehouse selection:', error);
        }
        
        // Load catalogs
        await this._loadCatalogs();
    },
    
    /**
     * Open the UC File Dialog
     * @param {Object} options - Configuration options
     * @param {string} options.mode - 'load' or 'save'
     * @param {string} options.title - Dialog title
     * @param {Array} options.extensions - File extensions to filter (load mode)
     * @param {string} options.defaultFilename - Default filename (save mode)
     * @param {Function} options.onSelect - Callback when file is loaded (receives file info with content)
     * @param {Function} options.onSave - Callback when save location is confirmed (receives location info)
     */
    open: async function(options = {}) {
        console.log('[UCFileDialog] Opening with options:', options);
        
        const modal = document.getElementById('ucFileDialogModal');
        if (!modal) {
            console.error('UC File Dialog modal not found');
            return;
        }
        
        // Set mode
        this.mode = options.mode || 'load';
        console.log('[UCFileDialog] Mode set to:', this.mode);
        
        // Set title
        const title = options.title || (this.mode === 'load' ? 'Load from Unity Catalog' : 'Save to Unity Catalog');
        document.getElementById('ucFileDialogTitle').textContent = title;
        
        // Set extensions
        if (options.extensions) {
            this.extensions = options.extensions;
        }
        
        // Set default filename
        this.defaultFilename = options.defaultFilename || '';
        
        // Set callback
        this.callback = this.mode === 'load' ? options.onSelect : options.onSave;
        
        // Configure UI based on mode
        this._configureUI();
        
        // Reset all dropdowns
        const catalogSelect = document.getElementById('ucFileDialogCatalog');
        const schemaSelect = document.getElementById('ucFileDialogSchema');
        const volumeSelect = document.getElementById('ucFileDialogVolume');
        
        catalogSelect.innerHTML = '<option value="">Select warehouse first...</option>';
        catalogSelect.disabled = true;
        schemaSelect.innerHTML = '<option value="">Select catalog first...</option>';
        schemaSelect.disabled = true;
        volumeSelect.innerHTML = '<option value="">Select schema first...</option>';
        volumeSelect.disabled = true;
        
        // Reset files list
        document.getElementById('ucFileDialogFilesList').innerHTML = `
            <div class="text-muted text-center py-3">
                <i class="bi bi-info-circle"></i> Select a volume to browse files
            </div>
        `;
        document.getElementById('ucFileDialogInfo').textContent = '';
        
        // Show modal first, then load warehouses
        const bsModal = new bootstrap.Modal(modal);
        bsModal.show();
        
        // Load warehouses (which will also load catalogs if a default is selected)
        await this._loadWarehouses();
    },
    
    _configureUI: function() {
        const saveSection = document.getElementById('ucFileDialogSaveSection');
        const filenameInput = document.getElementById('ucFileDialogFilename');
        const loadFilesBtn = document.getElementById('ucFileDialogLoadFiles');
        const confirmSaveBtn = document.getElementById('ucFileDialogConfirmSave');
        
        console.log('[UCFileDialog] Configuring UI for mode:', this.mode);
        
        if (this.mode === 'save') {
            // Show save section and button - remove hidden class and set display
            saveSection.classList.remove('hidden-initial');
            saveSection.style.display = 'block';
            filenameInput.value = this.defaultFilename;
            confirmSaveBtn.classList.remove('hidden-initial');
            confirmSaveBtn.style.display = 'inline-block';
            loadFilesBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh';
            console.log('[UCFileDialog] Save mode: showing save section and button');
        } else {
            // Hide save section and button
            saveSection.classList.add('hidden-initial');
            saveSection.style.display = 'none';
            confirmSaveBtn.classList.add('hidden-initial');
            confirmSaveBtn.style.display = 'none';
            loadFilesBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i> Refresh';
            console.log('[UCFileDialog] Load mode: hiding save section and button');
        }
        
        // Disable refresh button until volume is selected
        loadFilesBtn.disabled = true;
    },
    
    _loadCatalogs: async function() {
        const select = document.getElementById('ucFileDialogCatalog');
        
        try {
            select.innerHTML = '<option value="">Loading catalogs...</option>';
            select.disabled = true;
            
            const response = await fetch('/settings/catalogs', { credentials: 'same-origin' });
            const data = await response.json();
            
            select.innerHTML = '<option value="">Select catalog...</option>';
            
            if (data.catalogs && data.catalogs.length > 0) {
                data.catalogs.forEach(catalog => {
                    const option = document.createElement('option');
                    option.value = catalog;
                    option.textContent = catalog;
                    select.appendChild(option);
                });
                select.disabled = false;
            } else {
                select.innerHTML = '<option value="">No catalogs found</option>';
            }
        } catch (error) {
            console.error('Error loading catalogs:', error);
            select.innerHTML = '<option value="">Error loading catalogs</option>';
        }
    },
    
    _loadSchemas: async function(catalog) {
        const schemaSelect = document.getElementById('ucFileDialogSchema');
        const volumeSelect = document.getElementById('ucFileDialogVolume');
        
        // Reset schema and volume
        schemaSelect.innerHTML = '<option value="">Loading schemas...</option>';
        schemaSelect.disabled = true;
        volumeSelect.innerHTML = '<option value="">Select schema first...</option>';
        volumeSelect.disabled = true;
        
        try {
            const response = await fetch(`/settings/schemas/${catalog}`, { credentials: 'same-origin' });
            const data = await response.json();
            
            schemaSelect.innerHTML = '<option value="">Select schema...</option>';
            
            if (data.schemas && data.schemas.length > 0) {
                data.schemas.forEach(schema => {
                    const option = document.createElement('option');
                    option.value = schema;
                    option.textContent = schema;
                    schemaSelect.appendChild(option);
                });
                schemaSelect.disabled = false;
            } else {
                schemaSelect.innerHTML = '<option value="">No schemas found</option>';
            }
        } catch (error) {
            console.error('Error loading schemas:', error);
            schemaSelect.innerHTML = '<option value="">Error loading schemas</option>';
        }
    },
    
    _loadVolumes: async function(catalog, schema) {
        const select = document.getElementById('ucFileDialogVolume');
        const loadFilesBtn = document.getElementById('ucFileDialogLoadFiles');
        
        select.innerHTML = '<option value="">Loading volumes...</option>';
        select.disabled = true;
        loadFilesBtn.disabled = true;
        
        try {
            const response = await fetch(`/settings/volumes/${catalog}/${schema}`, { credentials: 'same-origin' });
            const data = await response.json();
            
            select.innerHTML = '<option value="">Select volume...</option>';
            
            if (data.volumes && data.volumes.length > 0) {
                data.volumes.forEach(volume => {
                    const option = document.createElement('option');
                    option.value = volume;
                    option.textContent = volume;
                    select.appendChild(option);
                });
                select.disabled = false;
            } else {
                select.innerHTML = '<option value="">No volumes found</option>';
            }
        } catch (error) {
            console.error('Error loading volumes:', error);
            select.innerHTML = '<option value="">Error loading volumes</option>';
        }
    },
    
    /**
     * Called when volume selection changes - auto-loads files
     */
    _onVolumeChange: async function() {
        const catalog = document.getElementById('ucFileDialogCatalog').value;
        const schema = document.getElementById('ucFileDialogSchema').value;
        const volume = document.getElementById('ucFileDialogVolume').value;
        const loadFilesBtn = document.getElementById('ucFileDialogLoadFiles');
        
        if (catalog && schema && volume) {
            loadFilesBtn.disabled = false;
            // Automatically load files when volume is selected
            await this.loadFiles();
        } else {
            loadFilesBtn.disabled = true;
            // Reset files list
            document.getElementById('ucFileDialogFilesList').innerHTML = `
                <div class="text-muted text-center py-3">
                    <i class="bi bi-info-circle"></i> Select a volume to browse files
                </div>
            `;
            document.getElementById('ucFileDialogInfo').textContent = '';
        }
    },
    
    loadFiles: async function() {
        const catalogEl = document.getElementById('ucFileDialogCatalog');
        const schemaEl = document.getElementById('ucFileDialogSchema');
        const volumeEl = document.getElementById('ucFileDialogVolume');
        const container = document.getElementById('ucFileDialogFilesList');
        const info = document.getElementById('ucFileDialogInfo');
        
        const catalog = catalogEl ? catalogEl.value : '';
        const schema = schemaEl ? schemaEl.value : '';
        const volume = volumeEl ? volumeEl.value : '';
        
        // Validate all fields are selected
        if (!catalog || !schema || !volume) {
            container.innerHTML = `
                <div class="text-muted text-center py-3">
                    <i class="bi bi-info-circle"></i> Please select catalog, schema, and volume first
                </div>
            `;
            info.textContent = '';
            return;
        }
        
        container.innerHTML = '<div class="text-center py-3"><i class="bi bi-hourglass-split"></i> Loading files...</div>';
        info.textContent = '';
        
        try {
            const response = await fetch('/browse-volume', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ 
                    catalog: catalog,
                    schema: schema,
                    volume: volume
                }),
                credentials: 'same-origin'
            });
            
            const result = await response.json();
            
            if (result.success && result.files.length > 0) {
                // Filter by extensions
                const filteredFiles = result.files.filter(file => {
                    if (file.is_directory) return true;
                    return this.extensions.some(ext => 
                        file.name.toLowerCase().endsWith(ext.toLowerCase())
                    );
                });
                
                if (filteredFiles.length > 0) {
                    container.innerHTML = '';
                    info.textContent = `Found ${filteredFiles.length} file(s)`;
                    
                    filteredFiles.forEach(file => {
                        const card = document.createElement('div');
                        card.className = 'card mb-2';
                        const icon = file.is_directory ? 'folder' : 'file-earmark-code';
                        const iconColor = file.is_directory ? 'text-warning' : 'text-primary';
                        
                        let actionButton = '';
                        if (!file.is_directory) {
                            if (this.mode === 'load') {
                                actionButton = `
                                    <button class="btn btn-sm btn-primary" onclick="UCFileDialog.selectFile('${catalog}', '${schema}', '${volume}', '${file.name}', '${file.path}')">
                                        <i class="bi bi-check-circle"></i> Select
                                    </button>
                                `;
                            } else {
                                // In save mode, clicking a file sets the filename
                                actionButton = `
                                    <button class="btn btn-sm btn-outline-secondary" onclick="UCFileDialog.setFilename('${file.name}')">
                                        <i class="bi bi-pencil"></i> Use Name
                                    </button>
                                `;
                            }
                        }
                        
                        card.innerHTML = `
                            <div class="card-body p-2">
                                <div class="d-flex justify-content-between align-items-center">
                                    <div>
                                        <i class="bi bi-${icon} ${iconColor}"></i>
                                        <strong>${file.name}</strong>
                                        ${file.size ? `<small class="text-muted ms-2">${this._formatFileSize(file.size)}</small>` : ''}
                                    </div>
                                    ${actionButton}
                                </div>
                            </div>
                        `;
                        container.appendChild(card);
                    });
                } else {
                    container.innerHTML = `<div class="alert alert-info">No matching files found (looking for ${this.extensions.join(', ')})</div>`;
                }
            } else if (result.success) {
                container.innerHTML = '<div class="alert alert-info">No files found in this volume</div>';
            } else {
                container.innerHTML = `<div class="alert alert-danger">Error: ${result.message}</div>`;
            }
        } catch (error) {
            container.innerHTML = `<div class="alert alert-danger">Error loading files: ${error.message}</div>`;
        }
    },
    
    selectFile: async function(catalog, schema, volume, filename, filepath) {
        // Read the file content (load mode)
        console.log('UCFileDialog: Selecting file', filepath);
        
        try {
            const response = await fetch('/read-volume-file', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_path: filepath }),
                credentials: 'same-origin'
            });
            
            const result = await response.json();
            console.log('UCFileDialog: Read result', result.success, result.message || '');
            
            if (result.success) {
                // Close modal first
                this._closeModal();
                
                // Call callback with file info
                if (this.callback) {
                    console.log('UCFileDialog: Calling callback with content length:', result.content?.length || 0);
                    try {
                        await this.callback({
                            catalog,
                            schema,
                            volume,
                            filename,
                            path: filepath,
                            content: result.content
                        });
                    } catch (callbackError) {
                        console.error('UCFileDialog: Callback error:', callbackError);
                        if (typeof showNotification === 'function') {
                            showNotification('Error processing file: ' + callbackError.message, 'error');
                        }
                    }
                } else {
                    console.warn('UCFileDialog: No callback defined');
                }
            } else {
                console.error('UCFileDialog: Read failed:', result.message);
                if (typeof showNotification === 'function') {
                    showNotification('Error reading file: ' + result.message, 'error');
                }
            }
        } catch (error) {
            console.error('UCFileDialog: Error:', error);
            if (typeof showNotification === 'function') {
                showNotification('Error: ' + error.message, 'error');
            }
        }
    },
    
    setFilename: function(filename) {
        document.getElementById('ucFileDialogFilename').value = filename;
    },
    
    confirmSave: function() {
        const catalog = document.getElementById('ucFileDialogCatalog').value;
        const schema = document.getElementById('ucFileDialogSchema').value;
        const volume = document.getElementById('ucFileDialogVolume').value;
        const filename = document.getElementById('ucFileDialogFilename').value.trim();
        
        if (!catalog || !schema || !volume) {
            if (typeof showNotification === 'function') {
                showNotification('Please select catalog, schema, and volume', 'warning');
            }
            return;
        }
        
        if (!filename) {
            if (typeof showNotification === 'function') {
                showNotification('Please enter a filename', 'warning');
            }
            return;
        }
        
        const path = `/Volumes/${catalog}/${schema}/${volume}/${filename}`;
        
        // Close modal
        this._closeModal();
        
        // Call callback with location info
        if (this.callback) {
            this.callback({
                catalog,
                schema,
                volume,
                filename,
                path
            });
        }
    },
    
    _closeModal: function() {
        const modal = bootstrap.Modal.getInstance(document.getElementById('ucFileDialogModal'));
        if (modal) modal.hide();
    },
    
    _formatFileSize: function(bytes) {
        if (!bytes) return '';
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }
};

// Initialize event listeners when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    const warehouseSelect = document.getElementById('ucFileDialogWarehouse');
    const refreshWarehousesBtn = document.getElementById('ucFileDialogRefreshWarehouses');
    const catalogSelect = document.getElementById('ucFileDialogCatalog');
    const schemaSelect = document.getElementById('ucFileDialogSchema');
    const volumeSelect = document.getElementById('ucFileDialogVolume');
    const loadBtn = document.getElementById('ucFileDialogLoadFiles');
    const saveBtn = document.getElementById('ucFileDialogConfirmSave');
    
    if (warehouseSelect) {
        warehouseSelect.addEventListener('change', async function() {
            await UCFileDialog._onWarehouseChange(this.value);
        });
    }
    
    if (refreshWarehousesBtn) {
        refreshWarehousesBtn.addEventListener('click', async function() {
            await UCFileDialog._loadWarehouses();
        });
    }
    
    if (catalogSelect) {
        catalogSelect.addEventListener('change', async function() {
            if (this.value) {
                await UCFileDialog._loadSchemas(this.value);
            }
        });
    }
    
    if (schemaSelect) {
        schemaSelect.addEventListener('change', async function() {
            const catalog = catalogSelect?.value;
            if (catalog && this.value) {
                await UCFileDialog._loadVolumes(catalog, this.value);
            }
        });
    }
    
    if (volumeSelect) {
        volumeSelect.addEventListener('change', async function() {
            await UCFileDialog._onVolumeChange();
        });
    }
    
    if (loadBtn) {
        loadBtn.addEventListener('click', () => UCFileDialog.loadFiles());
    }
    
    if (saveBtn) {
        saveBtn.addEventListener('click', () => UCFileDialog.confirmSave());
    }
});

