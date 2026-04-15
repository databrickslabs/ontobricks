/**
 * OntoViz - Entity-Relationship Visual Editor
 * A lightweight library for creating and managing ER diagrams
 * 
 * @version 1.0.0
 * @license MIT
 */

(function(global) {
    'use strict';

    // ==========================================
    // Utility Functions
    // ==========================================
    const Utils = {
        generateId() {
            return 'ovz_' + Math.random().toString(36).substr(2, 9);
        },

        clamp(value, min, max) {
            return Math.min(Math.max(value, min), max);
        },

        getMousePosition(e, container) {
            const rect = container.getBoundingClientRect();
            return {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top
            };
        },

        distance(p1, p2) {
            return Math.sqrt(Math.pow(p2.x - p1.x, 2) + Math.pow(p2.y - p1.y, 2));
        }
    };

    // ==========================================
    // Entity Class
    // ==========================================
    class Entity {
        constructor(options = {}) {
            this.id = options.id || Utils.generateId();
            this.name = options.name || 'NewEntity';
            this.icon = options.icon || '📦'; // Default emoji icon
            this.description = options.description || '';
            this.x = options.x ?? 100;
            this.y = options.y ?? 100;
            // Ensure each property has an id
            this.properties = (options.properties || []).map(prop => ({
                id: prop.id || Utils.generateId(),
                name: prop.name || 'property',
                type: prop.type || 'string',
                isPrimaryKey: prop.isPrimaryKey || false,
                isRequired: prop.isRequired || false
            }));
            this.color = options.color || null; // Use default if null
            this.element = null;
        }

        addProperty(property = {}) {
            const prop = {
                id: Utils.generateId(),
                name: property.name || 'property',
                type: property.type || 'string',
                isPrimaryKey: property.isPrimaryKey || false,
                isRequired: property.isRequired || false
            };
            this.properties.push(prop);
            return prop;
        }

        removeProperty(propertyId) {
            this.properties = this.properties.filter(p => p.id !== propertyId);
        }

        updateProperty(propertyId, updates) {
            const prop = this.properties.find(p => p.id === propertyId);
            if (prop) {
                Object.assign(prop, updates);
            }
            return prop;
        }

        toJSON() {
            return {
                id: this.id,
                name: this.name,
                icon: this.icon,
                description: this.description,
                x: this.x,
                y: this.y,
                properties: this.properties,
                color: this.color
            };
        }

        static fromJSON(json) {
            return new Entity(json);
        }
    }

    // ==========================================
    // Relationship Class
    // ==========================================
    class Relationship {
        constructor(options = {}) {
            this.id = options.id || Utils.generateId();
            this.name = options.name || 'relates_to';
            this.sourceEntityId = options.sourceEntityId || null;
            this.targetEntityId = options.targetEntityId || null;
            this.sourceCardinality = options.sourceCardinality || '1';
            this.targetCardinality = options.targetCardinality || '*';
            this.direction = options.direction || 'forward'; // 'forward' or 'reverse' only
            // Note: Relationship attributes are not supported
            this.properties = [];
            this.labelOffsetX = options.labelOffsetX ?? 0;
            this.labelOffsetY = options.labelOffsetY ?? 0;
            // Anchor points: 'auto', 'top', 'bottom', 'left', 'right' (Line 28: can be changed by dragging)
            this.sourceAnchor = options.sourceAnchor || 'auto';
            this.targetAnchor = options.targetAnchor || 'auto';
            this.pathElement = null;
            this.labelElement = null;
        }

        /**
         * Toggle between forward and reverse direction
         */
        cycleDirection() {
            // Only toggle between forward and reverse
            // For bidirectional, user should create two separate relationships
            this.direction = this.direction === 'forward' ? 'reverse' : 'forward';
            return this.direction;
        }

        toJSON() {
            return {
                id: this.id,
                name: this.name,
                sourceEntityId: this.sourceEntityId,
                targetEntityId: this.targetEntityId,
                sourceCardinality: this.sourceCardinality,
                targetCardinality: this.targetCardinality,
                direction: this.direction,
                properties: this.properties,
                labelOffsetX: this.labelOffsetX,
                labelOffsetY: this.labelOffsetY,
                sourceAnchor: this.sourceAnchor,  // Line 36: Keep in memory the layout
                targetAnchor: this.targetAnchor
            };
        }

        static fromJSON(json) {
            return new Relationship(json);
        }
    }

    // ==========================================
    // Inheritance Class
    // ==========================================
    class Inheritance {
        constructor(options = {}) {
            this.id = options.id || Utils.generateId();
            this.sourceEntityId = options.sourceEntityId || null; // Parent entity
            this.targetEntityId = options.targetEntityId || null; // Child entity (inherits from parent)
            this.direction = options.direction || 'forward'; // 'forward' = source->target, 'reverse' = target->source
            // DOM references
            this.pathElement = null;
            this.arrowElement = null;
        }

        /**
         * Toggle direction (clicking on arrow changes direction)
         */
        cycleDirection() {
            this.direction = this.direction === 'forward' ? 'reverse' : 'forward';
            return this.direction;
        }

        /**
         * Get the parent entity ID (the one being inherited FROM)
         */
        getParentEntityId() {
            return this.direction === 'forward' ? this.sourceEntityId : this.targetEntityId;
        }

        /**
         * Get the child entity ID (the one inheriting)
         */
        getChildEntityId() {
            return this.direction === 'forward' ? this.targetEntityId : this.sourceEntityId;
        }

        toJSON() {
            return {
                id: this.id,
                sourceEntityId: this.sourceEntityId,
                targetEntityId: this.targetEntityId,
                direction: this.direction
            };
        }

        static fromJSON(json) {
            return new Inheritance(json);
        }
    }

    // ==========================================
    // Main OntoViz Class
    // ==========================================
    class OntoViz {
        constructor(container, options = {}) {
            this.container = typeof container === 'string' 
                ? document.querySelector(container) 
                : container;

            if (!this.container) {
                throw new Error('OntoViz: Container element not found');
            }

            // Options
            this.options = {
                showToolbar: options.showToolbar !== false,
                showMinimap: options.showMinimap !== false,
                showStatusBar: options.showStatusBar !== false,
                showPropertyTypes: options.showPropertyTypes !== false,
                showPropertyKeys: options.showPropertyKeys !== false,
                gridSize: options.gridSize || 24,
                snapToGrid: options.snapToGrid !== false,
                viewOnly: options.viewOnly || false,  // View-only mode: allows layout but not content editing
                onEntityCreate: options.onEntityCreate || null,
                onEntityUpdate: options.onEntityUpdate || null,
                onEntityDelete: options.onEntityDelete || null,
                onRelationshipCreate: options.onRelationshipCreate || null,
                onRelationshipUpdate: options.onRelationshipUpdate || null,
                onVisibilityChange: options.onVisibilityChange || null,
                onRelationshipDelete: options.onRelationshipDelete || null,
                onInheritanceCreate: options.onInheritanceCreate || null,
                onInheritanceUpdate: options.onInheritanceUpdate || null,
                onInheritanceDelete: options.onInheritanceDelete || null,
                onSelectionChange: options.onSelectionChange || null,
                onViewModeChange: options.onViewModeChange || null,
                onLayoutChange: options.onLayoutChange || null
            };

            // State
            this.entities = new Map();
            this.relationships = new Map();
            this.inheritances = new Map();
            this.selectedEntity = null;
            this.selectedRelationship = null;
            this.selectedInheritance = null;
            this.isDragging = false;
            this.isConnecting = false;
            this.connectionSource = null;
            this.tempLine = null;
            this.isInheritanceMode = false; // Toggle mode for creating inheritance links
            
            // Canvas panning state
            this.isPanning = false;
            this.panStart = { x: 0, y: 0 };

            // Initialize
            this._init();
        }

        // ==========================================
        // Initialization
        // ==========================================
        _init() {
            this._createCanvas();
            this._createSVGLayer();
            if (this.options.showToolbar) this._createToolbar();
            if (this.options.showStatusBar) this._createStatusBar();
            this._bindEvents();
        }

        _createCanvas() {
            this.container.innerHTML = '';
            this.container.classList.add('ovz-canvas');
            
            // Add view-only class if viewOnly option is set
            if (this.options.viewOnly) {
                this.container.classList.add('ovz-view-only');
            }
            
            // Initialize visibility state (needed for _renderRelationship and _renderInheritance)
            // This must be initialized here since _createToolbar may not be called
            if (!this.visibilityState) {
                this.visibilityState = {
                    entities: new Map(),
                    relationships: new Map(),
                    inheritances: new Map()
                };
            }

            this.canvasInner = document.createElement('div');
            this.canvasInner.className = 'ovz-canvas-inner';
            this.container.appendChild(this.canvasInner);
        }

        _createSVGLayer() {
            this.svgLayer = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            this.svgLayer.classList.add('ovz-svg-layer');
            this.svgLayer.innerHTML = `
                <defs>
                    <marker id="ovz-arrow" markerWidth="12" markerHeight="12" 
                            refX="10" refY="6" orient="auto" markerUnits="userSpaceOnUse">
                        <path d="M0,0 L0,12 L12,6 z" class="ovz-relationship-arrow"/>
                    </marker>
                    <marker id="ovz-arrow-start" markerWidth="12" markerHeight="12" 
                            refX="2" refY="6" orient="auto" markerUnits="userSpaceOnUse">
                        <path d="M12,0 L12,12 L0,6 z" class="ovz-relationship-arrow"/>
                    </marker>
                    <marker id="ovz-inheritance-arrow" markerWidth="16" markerHeight="16" 
                            refX="14" refY="8" orient="auto" markerUnits="userSpaceOnUse">
                        <path d="M0,0 L0,16 L16,8 z" class="ovz-inheritance-arrow"/>
                    </marker>
                    <marker id="ovz-inheritance-arrow-start" markerWidth="16" markerHeight="16" 
                            refX="2" refY="8" orient="auto" markerUnits="userSpaceOnUse">
                        <path d="M16,0 L16,16 L0,8 z" class="ovz-inheritance-arrow"/>
                    </marker>
                </defs>
            `;
            this.canvasInner.appendChild(this.svgLayer);
        }

        _createToolbar() {
            const toolbar = document.createElement('div');
            toolbar.className = 'ovz-toolbar';
            toolbar.innerHTML = `
                <button class="ovz-toolbar-btn ovz-primary" data-action="add-entity" title="Add Entity">
                    <span>+</span>
                </button>
                <button class="ovz-toolbar-btn" data-action="add-inheritance" title="Add Inheritance Link">
                    <span>△</span>
                </button>
                <div class="ovz-toolbar-divider"></div>
                <button class="ovz-toolbar-btn" data-action="center" title="Center Diagram">
                    <span>◎</span>
                </button>
                <div class="ovz-toolbar-divider"></div>
                <button class="ovz-toolbar-btn" data-action="zoom-in" title="Zoom In">
                    <span>⊕</span>
                </button>
                <button class="ovz-toolbar-btn" data-action="zoom-out" title="Zoom Out">
                    <span>⊖</span>
                </button>
                <div class="ovz-toolbar-divider"></div>
                <button class="ovz-toolbar-btn" data-action="palette" title="Show/Hide Elements">
                    <span>👁</span>
                </button>
            `;
            this.container.appendChild(toolbar);
            this.toolbar = toolbar;

            // Initialize visibility state
            this.visibilityState = {
                entities: new Map(),      // entity.id -> boolean (visible)
                relationships: new Map(), // relationship.id -> boolean (visible)
                inheritances: new Map()   // inheritance.id -> boolean (visible)
            };

            // Bind toolbar events
            toolbar.addEventListener('click', (e) => {
                const btn = e.target.closest('.ovz-toolbar-btn');
                if (!btn) return;

                const action = btn.dataset.action;
                switch (action) {
                    case 'add-entity':
                        this.addEntity({ x: 150, y: 150 });
                        break;
                    case 'add-inheritance':
                        this._toggleInheritanceMode();
                        break;
                    case 'center':
                        this.centerDiagram();
                        break;
                    case 'zoom-in':
                        this._zoom(1.2);
                        break;
                    case 'zoom-out':
                        this._zoom(0.8);
                        break;
                    case 'palette':
                        this._togglePalettePopup(btn);
                        break;
                }
            });
        }

        // ==========================================
        // Palette/Visibility Popup
        // ==========================================
        _togglePalettePopup(btn) {
            // Close existing popup if open
            const existingPopup = document.querySelector('.ovz-palette-popup');
            if (existingPopup) {
                existingPopup.remove();
                btn.classList.remove('ovz-active');
                return;
            }

            btn.classList.add('ovz-active');

            // Create popup
            const popup = document.createElement('div');
            popup.className = 'ovz-palette-popup';
            
            // Build entities HTML
            let entitiesHtml = '';
            this.entities.forEach((entity, id) => {
                const isVisible = this.visibilityState.entities.get(id) !== false;
                const icon = entity.icon || '📦';
                entitiesHtml += `
                    <label class="ovz-palette-item">
                        <input type="checkbox" data-type="entity" data-id="${id}" ${isVisible ? 'checked' : ''}>
                        <span class="ovz-palette-icon">${icon}</span>
                        <span class="ovz-palette-name">${entity.name}</span>
                    </label>
                `;
            });

            // Determine orphan entities (no relationships or inheritances)
            const connectedEntities = new Set();
            this.relationships.forEach(rel => {
                connectedEntities.add(rel.sourceEntityId);
                connectedEntities.add(rel.targetEntityId);
            });
            this.inheritances.forEach(inh => {
                connectedEntities.add(inh.sourceEntityId);
                connectedEntities.add(inh.targetEntityId);
            });
            
            // Debug: log entity and connection info
            const allEntityIds = Array.from(this.entities.keys());
            const orphanIds = allEntityIds.filter(id => !connectedEntities.has(id));
            console.log('[Palette] Entities:', allEntityIds.length, allEntityIds);
            console.log('[Palette] Relationships:', this.relationships.size);
            console.log('[Palette] Inheritances:', this.inheritances.size);
            console.log('[Palette] Connected entity IDs:', Array.from(connectedEntities));
            console.log('[Palette] Orphan entity IDs:', orphanIds);
            
            const orphanCount = orphanIds.length;

            // Check if all relationships/inheritances are currently visible
            let allRelationshipsVisible = true;
            this.relationships.forEach((rel, id) => {
                if (this.visibilityState.relationships.get(id) === false) {
                    allRelationshipsVisible = false;
                }
            });
            let allInheritancesVisible = true;
            this.inheritances.forEach((inh, id) => {
                if (this.visibilityState.inheritances.get(id) === false) {
                    allInheritancesVisible = false;
                }
            });

            popup.innerHTML = `
                <div class="ovz-palette-header">
                    <span>Show/Hide Elements</span>
                    <button class="ovz-palette-close" title="Close">×</button>
                </div>
                <div class="ovz-palette-actions">
                    <button class="ovz-palette-action-btn" data-action="show-all-entities">Show All</button>
                    <button class="ovz-palette-action-btn" data-action="hide-all-entities">Hide All</button>
                    <button class="ovz-palette-action-btn" data-action="hide-orphans" title="Hide entities without relationships or inheritance" ${orphanCount === 0 ? 'disabled' : ''}>Hide Orphans (${orphanCount})</button>
                </div>
                <div class="ovz-palette-content">
                    <div class="ovz-palette-section">
                        <div class="ovz-palette-section-title">Entities (${this.entities.size})</div>
                        <div class="ovz-palette-items" data-section="entities">
                            ${entitiesHtml || '<div class="ovz-palette-empty">No entities</div>'}
                        </div>
                    </div>
                    <div class="ovz-palette-section">
                        <div class="ovz-palette-section-title">Relationships (${this.relationships.size})</div>
                        <div class="ovz-palette-toggles">
                            <label class="ovz-palette-toggle-item">
                                <input type="checkbox" data-action="toggle-all-relationships" ${this.relationships.size === 0 || allRelationshipsVisible ? 'checked' : ''} ${this.relationships.size === 0 ? 'disabled' : ''}>
                                <span class="ovz-palette-icon">↔</span>
                                <span class="ovz-palette-name">Show/Hide All Relationships</span>
                            </label>
                        </div>
                    </div>
                    <div class="ovz-palette-section">
                        <div class="ovz-palette-section-title">Inheritance Links (${this.inheritances.size})</div>
                        <div class="ovz-palette-toggles">
                            <label class="ovz-palette-toggle-item">
                                <input type="checkbox" data-action="toggle-all-inheritances" ${this.inheritances.size === 0 || allInheritancesVisible ? 'checked' : ''} ${this.inheritances.size === 0 ? 'disabled' : ''}>
                                <span class="ovz-palette-icon">△</span>
                                <span class="ovz-palette-name">Show/Hide All Inheritance Links</span>
                            </label>
                        </div>
                </div>
            `;

            // Position popup using fixed positioning (relative to viewport)
            const btnRect = btn.getBoundingClientRect();
            popup.style.position = 'fixed';
            popup.style.top = (btnRect.bottom + 8) + 'px';
            popup.style.right = (window.innerWidth - btnRect.right) + 'px';

            // Append to body for proper fixed positioning
            document.body.appendChild(popup);

            // Bind events
            popup.querySelector('.ovz-palette-close').addEventListener('click', () => {
                popup.remove();
                btn.classList.remove('ovz-active');
            });

            // Show All Entities button
            popup.querySelector('[data-action="show-all-entities"]').addEventListener('click', () => {
                popup.querySelectorAll('input[data-type="entity"]').forEach(cb => {
                    cb.checked = true;
                    this._toggleItemVisibility('entity', cb.dataset.id, true);
                });
            });

            // Hide All Entities button
            popup.querySelector('[data-action="hide-all-entities"]').addEventListener('click', () => {
                popup.querySelectorAll('input[data-type="entity"]').forEach(cb => {
                    cb.checked = false;
                    this._toggleItemVisibility('entity', cb.dataset.id, false);
                });
            });

            // Hide Orphans button
            const hideOrphansBtn = popup.querySelector('[data-action="hide-orphans"]');
            if (hideOrphansBtn) {
                hideOrphansBtn.addEventListener('click', () => {
                    // Find and hide orphan entities
                    this.entities.forEach((entity, id) => {
                        if (!connectedEntities.has(id)) {
                            this._toggleItemVisibility('entity', id, false);
                            // Update checkbox in popup
                            const cb = popup.querySelector(`input[data-type="entity"][data-id="${id}"]`);
                            if (cb) cb.checked = false;
                        }
                    });
                });
            }

            // Toggle all relationships
            const toggleRelationshipsCheckbox = popup.querySelector('[data-action="toggle-all-relationships"]');
            if (toggleRelationshipsCheckbox) {
                toggleRelationshipsCheckbox.addEventListener('change', (e) => {
                    const visible = e.target.checked;
                    this.relationships.forEach((rel, id) => {
                        this.visibilityState.relationships.set(id, visible);
                        this._updateRelationshipVisibility(id);
                    });
                    // Fire visibility change callback
                    if (this.options.onVisibilityChange) {
                        this.options.onVisibilityChange('relationships', 'all', visible);
                    }
                });
            }

            // Toggle all inheritances
            const toggleInheritancesCheckbox = popup.querySelector('[data-action="toggle-all-inheritances"]');
            if (toggleInheritancesCheckbox) {
                toggleInheritancesCheckbox.addEventListener('change', (e) => {
                    const visible = e.target.checked;
                    this.inheritances.forEach((inh, id) => {
                        this.visibilityState.inheritances.set(id, visible);
                        this._updateInheritanceVisibility(id);
                    });
                    // Fire visibility change callback
                    if (this.options.onVisibilityChange) {
                        this.options.onVisibilityChange('inheritances', 'all', visible);
                    }
                });
            }

            // Entity checkbox change events
            popup.querySelectorAll('input[data-type="entity"]').forEach(cb => {
                cb.addEventListener('change', (e) => {
                    this._toggleItemVisibility('entity', e.target.dataset.id, e.target.checked);
                });
            });

            // Close on outside click
            const closeOnOutside = (e) => {
                if (!popup.contains(e.target) && !btn.contains(e.target)) {
                    popup.remove();
                    btn.classList.remove('ovz-active');
                    document.removeEventListener('mousedown', closeOnOutside);
                }
            };
            setTimeout(() => {
                document.addEventListener('mousedown', closeOnOutside);
            }, 100);
        }

        _toggleItemVisibility(type, id, visible, skipCallback = false) {
            console.log(`[Palette] Toggle ${type} ${id} -> ${visible}`);
            
            if (type === 'entity') {
                this.visibilityState.entities.set(id, visible);
                const entity = this.entities.get(id);
                console.log(`[Palette] Entity found:`, entity ? entity.name : 'NOT FOUND');
                
                if (entity && entity.element) {
                    entity.element.style.display = visible ? '' : 'none';
                }
                // Always update all connected relationships and inheritances
                // NOTE: Do NOT call _updateRelationshipPaths() here as it re-renders and loses visibility
                this._updateAllConnectionsVisibility();
            } else if (type === 'relationship') {
                this.visibilityState.relationships.set(id, visible);
                // Update this specific relationship considering entity visibility
                this._updateRelationshipVisibility(id);
            } else if (type === 'inheritance') {
                this.visibilityState.inheritances.set(id, visible);
                // Update this specific inheritance considering entity visibility
                this._updateInheritanceVisibility(id);
            }
            // NOTE: Removed _updateRelationshipPaths() call - it was re-rendering elements
            // and losing visibility settings. Visibility toggling doesn't require path re-rendering.
            
            // Fire visibility change callback (for auto-save)
            if (!skipCallback && this.options.onVisibilityChange) {
                this.options.onVisibilityChange(type, id, visible);
            }
        }

        _updateInheritanceVisibility(inhId) {
            const inh = this.inheritances.get(inhId);
            if (!inh) return;
            
            const sourceVisible = this.visibilityState.entities.get(inh.sourceEntityId) !== false;
            const targetVisible = this.visibilityState.entities.get(inh.targetEntityId) !== false;
            const inhExplicitlyVisible = this.visibilityState.inheritances.get(inhId) !== false;
            
            // Inheritance is visible only if both connected entities are visible AND inheritance itself is visible
            const shouldBeVisible = sourceVisible && targetVisible && inhExplicitlyVisible;
            
            // Use visibility for SVG elements
            const visValue = shouldBeVisible ? 'visible' : 'hidden';
            if (inh.pathElement) inh.pathElement.style.visibility = visValue;
            if (inh.arrowElement) inh.arrowElement.style.visibility = visValue;
        }

        _updateRelationshipVisibility(relId) {
            const rel = this.relationships.get(relId);
            if (!rel) return;
            
            const sourceVisible = this.visibilityState.entities.get(rel.sourceEntityId) !== false;
            const targetVisible = this.visibilityState.entities.get(rel.targetEntityId) !== false;
            const relExplicitlyVisible = this.visibilityState.relationships.get(relId) !== false;
            
            // Relationship is visible only if both connected entities are visible AND relationship itself is visible
            const shouldBeVisible = sourceVisible && targetVisible && relExplicitlyVisible;
            
            // Use visibility for SVG elements
            const visValue = shouldBeVisible ? 'visible' : 'hidden';
            if (rel.pathElement) rel.pathElement.style.visibility = visValue;
            if (rel.pathElement2) rel.pathElement2.style.visibility = visValue;
            if (rel.labelElement) rel.labelElement.style.visibility = visValue;
        }

        _updateAllConnectionsVisibility() {
            console.log(`[Palette] Updating all connections. Relationships: ${this.relationships.size}, Inheritances: ${this.inheritances.size}`);
            console.log(`[Palette] Entity visibility state:`, Object.fromEntries(this.visibilityState.entities));
            
            // Update all relationships based on entity visibility
            this.relationships.forEach((rel, relId) => {
                const sourceVisible = this.visibilityState.entities.get(rel.sourceEntityId) !== false;
                const targetVisible = this.visibilityState.entities.get(rel.targetEntityId) !== false;
                const relExplicitlyVisible = this.visibilityState.relationships.get(relId) !== false;
                
                // Relationship is visible only if both connected entities are visible AND relationship itself is visible
                const shouldBeVisible = sourceVisible && targetVisible && relExplicitlyVisible;
                
                console.log(`[Palette] Rel "${rel.name}": source=${rel.sourceEntityId}(${sourceVisible}), target=${rel.targetEntityId}(${targetVisible}) -> ${shouldBeVisible}`);
                console.log(`[Palette] Rel "${rel.name}" elements: path1=${!!rel.pathElement}, path2=${!!rel.pathElement2}, label=${!!rel.labelElement}`);
                
                // Use visibility for SVG elements (more reliable than display)
                const visValue = shouldBeVisible ? 'visible' : 'hidden';
                if (rel.pathElement) {
                    rel.pathElement.style.visibility = visValue;
                    console.log(`[Palette] Setting path1 visibility to: ${visValue}`);
                }
                if (rel.pathElement2) {
                    rel.pathElement2.style.visibility = visValue;
                    console.log(`[Palette] Setting path2 visibility to: ${visValue}`);
                }
                if (rel.labelElement) {
                    rel.labelElement.style.visibility = visValue;
                    console.log(`[Palette] Setting label visibility to: ${visValue}`);
                }
            });

            // Also update all inheritances
            this.inheritances.forEach((inh, inhId) => {
                const sourceVisible = this.visibilityState.entities.get(inh.sourceEntityId) !== false;
                const targetVisible = this.visibilityState.entities.get(inh.targetEntityId) !== false;
                const inhExplicitlyVisible = this.visibilityState.inheritances.get(inhId) !== false;
                
                // Inheritance is visible only if both connected entities are visible AND inheritance itself is visible
                const shouldBeVisible = sourceVisible && targetVisible && inhExplicitlyVisible;
                
                console.log(`[Palette] Inh: source=${inh.sourceEntityId}(${sourceVisible}), target=${inh.targetEntityId}(${targetVisible}) -> ${shouldBeVisible}`);
                console.log(`[Palette] Inh elements: path=${!!inh.pathElement}, arrow=${!!inh.arrowElement}`);
                
                // Use visibility for SVG elements (more reliable than display)
                const inhVisValue = shouldBeVisible ? 'visible' : 'hidden';
                if (inh.pathElement) {
                    inh.pathElement.style.visibility = inhVisValue;
                    console.log(`[Palette] Setting inheritance path visibility to: ${inhVisValue}`);
                }
                if (inh.arrowElement) {
                    inh.arrowElement.style.visibility = inhVisValue;
                    console.log(`[Palette] Setting inheritance arrow visibility to: ${inhVisValue}`);
                }
            });
        }

        _createStatusBar() {
            this.statusBar = document.createElement('div');
            this.statusBar.className = 'ovz-status';
            this._updateStatusBar();
            this.container.appendChild(this.statusBar);
        }

        _updateStatusBar() {
            if (!this.statusBar) return;
            this.statusBar.innerHTML = `
                <div class="ovz-status-item">
                    <span class="ovz-status-dot"></span>
                    <span>Entities: ${this.entities.size}</span>
                </div>
                <div class="ovz-status-item">
                    <span>Relationships: ${this.relationships.size}</span>
                </div>
                <div class="ovz-status-item">
                    <span>Inheritances: ${this.inheritances.size}</span>
                </div>
            `;
        }

        // ==========================================
        // Event Binding
        // ==========================================
        _bindEvents() {
            // Canvas click - deselect
            this.container.addEventListener('click', (e) => {
                if (e.target === this.container || e.target === this.canvasInner) {
                    this._clearSelection();
                }
            });

            // Context menu
            this.container.addEventListener('contextmenu', (e) => {
                e.preventDefault();
                this._showContextMenu(e);
            });

            // Canvas panning - mousedown on background
            this.canvasInner.addEventListener('mousedown', (e) => {
                // Only start panning if clicking on the canvas background (not on entities/relationships)
                if (e.target === this.canvasInner || e.target === this.svgLayer || e.target.tagName === 'svg') {
                    this.isPanning = true;
                    this.panStart = { x: e.clientX, y: e.clientY };
                    this.canvasInner.style.cursor = 'grabbing';
                    e.preventDefault();
                }
            });

            // Mouse move for connection line AND canvas panning
            this.container.addEventListener('mousemove', (e) => {
                // Handle connection line drawing
                if (this.isConnecting && this.tempLine) {
                    const pos = Utils.getMousePosition(e, this.canvasInner);
                    this.tempLine.setAttribute('x2', pos.x);
                    this.tempLine.setAttribute('y2', pos.y);
                }
                
                // Handle canvas panning
                if (this.isPanning && !this.isDragging) {
                    const deltaX = e.clientX - this.panStart.x;
                    const deltaY = e.clientY - this.panStart.y;
                    
                    // Move all entities
                    this.entities.forEach(entity => {
                        entity.x += deltaX;
                        entity.y += deltaY;
                        if (entity.element) {
                            entity.element.style.left = entity.x + 'px';
                            entity.element.style.top = entity.y + 'px';
                        }
                    });
                    
                    // Move all relationship labels too (for panning, everything moves together)
                    this.relationships.forEach(relationship => {
                        const label = relationship.labelElement;
                        if (label && label._baseMidPoint) {
                            label._baseMidPoint.x += deltaX;
                            label._baseMidPoint.y += deltaY;
                            const newX = label._baseMidPoint.x + (relationship.labelOffsetX || 0);
                            const newY = label._baseMidPoint.y + (relationship.labelOffsetY || 0);
                            label.style.left = newX + 'px';
                            label.style.top = newY + 'px';
                        }
                    });
                    
                    // Update relationship paths only (labels already moved)
                    this._updateRelationshipPathsOnly();
                    
                    // Update pan start for next move
                    this.panStart = { x: e.clientX, y: e.clientY };
                }
            });

            // Mouse up - end connection AND panning
            this.container.addEventListener('mouseup', () => {
                if (this.isConnecting) {
                    this._endConnection(null);
                }
                
                // End panning — notify that entity positions changed
                if (this.isPanning) {
                    this.isPanning = false;
                    this.canvasInner.style.cursor = '';
                    if (this.options.onLayoutChange) {
                        this.options.onLayoutChange('pan');
                    }
                }
            });
            
            // Also handle mouse leaving the container
            this.container.addEventListener('mouseleave', () => {
                if (this.isPanning) {
                    this.isPanning = false;
                    this.canvasInner.style.cursor = '';
                }
            });

            // Keyboard shortcuts
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Delete' || e.key === 'Backspace') {
                    if (this.selectedEntity && document.activeElement.tagName !== 'INPUT') {
                        this._showDeleteOrHideDialog('entity', this.selectedEntity.id, this.selectedEntity.name);
                    } else if (this.selectedRelationship && document.activeElement.tagName !== 'INPUT') {
                        this._showDeleteOrHideDialog('relationship', this.selectedRelationship.id, this.selectedRelationship.name);
                    } else if (this.selectedInheritance && document.activeElement.tagName !== 'INPUT') {
                        // For inheritance, build a name from source -> target
                        const sourceEntity = this.entities.get(this.selectedInheritance.sourceEntityId);
                        const targetEntity = this.entities.get(this.selectedInheritance.targetEntityId);
                        const inhName = `${sourceEntity?.name || '?'} → ${targetEntity?.name || '?'}`;
                        this._showDeleteOrHideDialog('inheritance', this.selectedInheritance.id, inhName);
                    }
                }
                if (e.key === 'Escape') {
                    this._clearSelection();
                    this._hideContextMenu();
                    if (this.isConnecting) {
                        this._endConnection(null);
                    }
                    if (this.isInheritanceMode) {
                        this._exitInheritanceMode();
                    }
                }
            });

            // Click outside context menu
            document.addEventListener('click', () => {
                this._hideContextMenu();
            });
        }

        // ==========================================
        // Entity Management
        // ==========================================
        addEntity(options = {}) {
            const entity = new Entity(options);
            this.entities.set(entity.id, entity);
            this._renderEntity(entity);
            this._updateStatusBar();

            if (this.options.onEntityCreate) {
                this.options.onEntityCreate(entity);
            }

            return entity;
        }

        removeEntity(entityId) {
            const entity = this.entities.get(entityId);
            if (!entity) return;

            // Remove related relationships
            this.relationships.forEach((rel, id) => {
                if (rel.sourceEntityId === entityId || rel.targetEntityId === entityId) {
                    this.removeRelationship(id);
                }
            });

            // Remove related inheritances
            this.inheritances.forEach((inh, id) => {
                if (inh.sourceEntityId === entityId || inh.targetEntityId === entityId) {
                    this.removeInheritance(id);
                }
            });

            // Remove DOM element
            if (entity.element) {
                entity.element.remove();
            }

            this.entities.delete(entityId);

            if (this.selectedEntity?.id === entityId) {
                this.selectedEntity = null;
            }

            this._updateStatusBar();

            if (this.options.onEntityDelete) {
                this.options.onEntityDelete(entity);
            }
        }

        updateEntity(entityId, updates) {
            const entity = this.entities.get(entityId);
            if (!entity) return;

            Object.assign(entity, updates);
            this._renderEntity(entity);
            this._updateRelationshipPaths();

            if (this.options.onEntityUpdate) {
                this.options.onEntityUpdate(entity);
            }

            return entity;
        }

        getEntity(entityId) {
            return this.entities.get(entityId);
        }

        /**
         * Set view-only mode
         * In view-only mode: layout changes (dragging) are allowed, but content editing is disabled
         * @param {boolean} viewOnly - true for view-only mode, false for edit mode
         */
        setViewOnly(viewOnly) {
            this.options.viewOnly = viewOnly;
            
            // Add/remove CSS class on container for styling
            if (viewOnly) {
                this.container.classList.add('ovz-view-only');
            } else {
                this.container.classList.remove('ovz-view-only');
            }
            
            // Re-render all entities to update their UI
            this.entities.forEach(entity => {
                this._renderEntity(entity);
            });
            
            // Re-render all relationships to update their UI
            this._updateRelationshipPaths();
            
            // Restore visibility state after re-rendering
            // Re-apply hidden state to entities
            this.visibilityState.entities.forEach((visible, id) => {
                if (visible === false) {
                    const entity = this.entities.get(id);
                    if (entity && entity.element) {
                        entity.element.style.display = 'none';
                    }
                }
            });
            
            // Update all connections visibility (relationships and inheritances)
            this._updateAllConnectionsVisibility();
            
            // Fire callback if provided
            if (this.options.onViewModeChange) {
                this.options.onViewModeChange(viewOnly);
            }
        }

        /**
         * Check if in view-only mode
         * @returns {boolean}
         */
        isViewOnly() {
            return this.options.viewOnly === true;
        }

        _renderEntity(entity) {
            // Remove existing element if re-rendering
            if (entity.element) {
                entity.element.remove();
            }

            const el = document.createElement('div');
            el.className = 'ovz-entity';
            el.dataset.entityId = entity.id;
            el.style.left = entity.x + 'px';
            el.style.top = entity.y + 'px';

            // Check if in view-only mode
            const isViewOnly = this.options.viewOnly === true;

            // Build properties HTML
            const showTypes = this.options.showPropertyTypes;
            const showKeys = this.options.showPropertyKeys;
            
            let propertiesHTML = entity.properties.map(prop => `
                <div class="ovz-property" data-property-id="${prop.id}">
                    ${showKeys ? `
                    <span class="ovz-property-icon ${prop.isPrimaryKey ? 'ovz-key' : ''}">
                        ${prop.isPrimaryKey ? '🔑' : '○'}
                    </span>
                    ` : `<span class="ovz-property-icon">○</span>`}
                    <span class="ovz-property-name">
                        ${isViewOnly 
                            ? `<span class="ovz-property-name-text">${this._escapeHtml(prop.name)}</span>`
                            : `<input type="text" value="${this._escapeHtml(prop.name)}" data-field="name" spellcheck="false">`
                        }
                    </span>
                    ${showTypes ? `<span class="ovz-property-type">${prop.type}</span>` : ''}
                    ${!isViewOnly ? `
                    <div class="ovz-property-actions">
                        ${showKeys ? `<button class="ovz-property-btn" data-action="toggle-key" title="Toggle Primary Key">🔑</button>` : ''}
                        <button class="ovz-property-btn ovz-delete" data-action="delete" title="Delete">×</button>
                    </div>
                    ` : ''}
                </div>
            `).join('');

            // Build inherited properties HTML (read-only - same in both modes)
            const inheritedProps = this.getInheritedProperties(entity.id);
            let inheritedPropertiesHTML = '';
            if (inheritedProps.length > 0) {
                inheritedPropertiesHTML = `
                    <div class="ovz-inherited-section">
                        <div class="ovz-inherited-header">Inherited Properties</div>
                        ${inheritedProps.map(prop => `
                            <div class="ovz-property ovz-property-inherited" title="Inherited from ${this._escapeHtml(prop.inheritedFrom)}">
                                <span class="ovz-property-icon ovz-inherited">↳</span>
                                <span class="ovz-property-name">
                                    <span class="ovz-inherited-name">${this._escapeHtml(prop.name)}</span>
                                </span>
                                ${showTypes ? `<span class="ovz-property-type">${prop.type}</span>` : ''}
                                <span class="ovz-inherited-from">${this._escapeHtml(prop.inheritedFrom)}</span>
                            </div>
                        `).join('')}
                    </div>
                `;
            }

            // Entity header - hide action buttons in view-only mode
            const entityActionsHTML = isViewOnly ? '' : `
                <div class="ovz-entity-actions">
                    <button class="ovz-entity-btn" data-action="edit-icon" title="Change Icon">🎨</button>
                    <button class="ovz-entity-btn" data-action="edit-description" title="Edit Description">📝</button>
                    <button class="ovz-entity-btn" data-action="add-property" title="Add Property">+</button>
                    <button class="ovz-entity-btn" data-action="delete" title="Delete Entity">×</button>
                </div>
            `;

            // Entity title - use text in view-only, input in edit mode
            const entityTitleHTML = isViewOnly 
                ? `<span class="ovz-entity-title-text">${this._escapeHtml(entity.name)}</span>`
                : `<span class="ovz-entity-title"><input type="text" value="${this._escapeHtml(entity.name)}" data-field="name" spellcheck="false"></span>`;

            // Icon - clickable only in edit mode
            const iconAttr = isViewOnly ? '' : 'data-action="edit-icon" title="Click to change icon"';

            el.innerHTML = `
                <div class="ovz-entity-header">
                    <span class="ovz-entity-icon" ${iconAttr}>${entity.icon || '📦'}</span>
                    ${entityTitleHTML}
                    ${entityActionsHTML}
                </div>
                ${entity.description ? `<div class="ovz-entity-description" title="${this._escapeHtml(entity.description)}">${this._escapeHtml(entity.description)}</div>` : ''}
                <div class="ovz-entity-body">
                    ${propertiesHTML}
                </div>
                ${inheritedPropertiesHTML}
                ${!isViewOnly ? `
                <div class="ovz-add-property" data-action="add-property">
                    <span>+ Add Property</span>
                </div>
                ` : ''}
                <div class="ovz-connector ovz-connector-left" data-connector="left"></div>
                <div class="ovz-connector ovz-connector-right" data-connector="right"></div>
                <div class="ovz-connector ovz-connector-top" data-connector="top"></div>
                <div class="ovz-connector ovz-connector-bottom" data-connector="bottom"></div>
            `;

            entity.element = el;
            this.canvasInner.appendChild(el);

            // Bind entity events
            this._bindEntityEvents(entity, el);
        }

        _bindEntityEvents(entity, el) {
            // Drag functionality
            let dragOffset = { x: 0, y: 0 };
            let isDragging = false;

            const header = el.querySelector('.ovz-entity-header');

            header.addEventListener('mousedown', (e) => {
                if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON') return;
                
                isDragging = true;
                el.classList.add('ovz-dragging');
                dragOffset = {
                    x: e.clientX - entity.x,
                    y: e.clientY - entity.y
                };

                const onMouseMove = (e) => {
                    if (!isDragging) return;

                    let newX = e.clientX - dragOffset.x;
                    let newY = e.clientY - dragOffset.y;

                    // Snap to grid
                    if (this.options.snapToGrid) {
                        newX = Math.round(newX / this.options.gridSize) * this.options.gridSize;
                        newY = Math.round(newY / this.options.gridSize) * this.options.gridSize;
                    }

                    // Clamp to canvas bounds
                    newX = Math.max(0, newX);
                    newY = Math.max(0, newY);

                    entity.x = newX;
                    entity.y = newY;
                    el.style.left = newX + 'px';
                    el.style.top = newY + 'px';

                    // Only update paths, keep relationship labels in place
                    this._updateRelationshipPathsOnly();
                };

                const onMouseUp = () => {
                    isDragging = false;
                    el.classList.remove('ovz-dragging');
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);

                    if (this.options.onEntityUpdate) {
                        this.options.onEntityUpdate(entity);
                    }
                };

                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });

            // Select entity
            el.addEventListener('click', (e) => {
                if (e.target.tagName !== 'INPUT') {
                    this._selectEntity(entity);
                }
            });

            // Entity name input
            const nameInput = el.querySelector('.ovz-entity-header input');
            if (nameInput) {
                this._restrictToValidChars(nameInput);
                nameInput.addEventListener('change', () => {
                    entity.name = nameInput.value;
                    if (this.options.onEntityUpdate) {
                        this.options.onEntityUpdate(entity);
                    }
                });
                nameInput.addEventListener('click', (e) => e.stopPropagation());
            }

            // Add property buttons (only in edit mode)
            el.querySelectorAll('[data-action="add-property"]').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    entity.addProperty({ name: 'new_property' });
                    this._renderEntity(entity);
                    this._updateRelationshipPaths();
                    // Re-render any child entities that inherit from this entity
                    this._rerenderChildEntities(entity.id);
                    if (this.options.onEntityUpdate) {
                        this.options.onEntityUpdate(entity);
                    }
                });
            });

            // Delete entity button (only exists in edit mode)
            const deleteBtn = el.querySelector('[data-action="delete"]');
            if (deleteBtn) {
                deleteBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this._showDeleteOrHideDialog('entity', entity.id, entity.name);
                });
            }

            // Edit icon buttons (only exist in edit mode)
            el.querySelectorAll('[data-action="edit-icon"]').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this._showIconPicker(entity);
                });
            });

            // Edit description button
            el.querySelector('[data-action="edit-description"]')?.addEventListener('click', (e) => {
                e.stopPropagation();
                this._showDescriptionEditor(entity);
            });

            // Property events
            el.querySelectorAll('.ovz-property').forEach(propEl => {
                const propId = propEl.dataset.propertyId;

                // Property name change (skip inherited properties which don't have inputs)
                const propInput = propEl.querySelector('input[data-field="name"]');
                if (propInput) {
                    this._restrictToValidChars(propInput);
                    propInput.addEventListener('change', () => {
                        entity.updateProperty(propId, { name: propInput.value });
                        // Re-render any child entities that inherit from this entity
                        this._rerenderChildEntities(entity.id);
                        if (this.options.onEntityUpdate) {
                            this.options.onEntityUpdate(entity);
                        }
                    });
                    propInput.addEventListener('click', (e) => e.stopPropagation());
                }

                // Toggle primary key
                propEl.querySelector('[data-action="toggle-key"]')?.addEventListener('click', (e) => {
                    e.stopPropagation();
                    const prop = entity.properties.find(p => p.id === propId);
                    if (prop) {
                        prop.isPrimaryKey = !prop.isPrimaryKey;
                        this._renderEntity(entity);
                        if (this.options.onEntityUpdate) {
                            this.options.onEntityUpdate(entity);
                        }
                    }
                });

                // Delete property
                propEl.querySelector('[data-action="delete"]')?.addEventListener('click', (e) => {
                    e.stopPropagation();
                    entity.removeProperty(propId);
                    this._renderEntity(entity);
                    this._updateRelationshipPaths();
                    // Re-render any child entities that inherit from this entity
                    this._rerenderChildEntities(entity.id);
                    if (this.options.onEntityUpdate) {
                        this.options.onEntityUpdate(entity);
                    }
                });
            });

            // Connector events (for creating relationships)
            el.querySelectorAll('.ovz-connector').forEach(connector => {
                connector.addEventListener('mousedown', (e) => {
                    e.stopPropagation();
                    this._startConnection(entity, connector.dataset.connector, e);
                });

                connector.addEventListener('mouseup', (e) => {
                    e.stopPropagation();
                    // Allow self-relationships (entity can have relationship with itself)
                    if (this.isConnecting) {
                        this._endConnection(entity);
                    }
                });

                connector.addEventListener('mouseenter', () => {
                    if (this.isConnecting) {
                        // For inheritance mode, don't allow self-loops
                        // For relationship mode, allow self-loops
                        const allowSelfLoop = !this.isInheritanceMode;
                        if (this.connectionSource.entity.id !== entity.id || allowSelfLoop) {
                            connector.classList.add('ovz-active');
                        }
                    }
                });

                connector.addEventListener('mouseleave', () => {
                    connector.classList.remove('ovz-active');
                });
            });
        }

        _selectEntity(entity) {
            this._clearSelection();
            this.selectedEntity = entity;
            entity.element.classList.add('ovz-selected');

            if (this.options.onSelectionChange) {
                this.options.onSelectionChange({ type: 'entity', item: entity });
            }
        }

        // ==========================================
        // Icon and Description Editors
        // ==========================================
        _showIconPicker(entity) {
            const commonIcons = (typeof EmojiPicker !== 'undefined')
                ? EmojiPicker.allEmojis()
                : ['📦','👤','🏢','📄','🔧','💡','📊','🎯','🏠','🚗','📱','💻','🌐','📁','⚙️','🔗'];

            // Remove existing picker if any
            this._closeModal();

            // Create modal
            const modal = document.createElement('div');
            modal.className = 'ovz-modal';
            modal.innerHTML = `
                <div class="ovz-modal-content ovz-icon-picker">
                    <div class="ovz-modal-header">
                        <h3>Choose Icon</h3>
                        <button class="ovz-modal-close" data-action="close">×</button>
                    </div>
                    <div class="ovz-modal-body">
                        <div class="ovz-icon-grid">
                            ${commonIcons.map(icon => `
                                <button class="ovz-icon-option ${entity.icon === icon ? 'ovz-selected' : ''}" 
                                        data-icon="${icon}">${icon}</button>
                            `).join('')}
                        </div>
                        <div class="ovz-icon-custom">
                            <label>Or enter custom emoji:</label>
                            <input type="text" id="ovz-custom-icon" class="ovz-custom-emoji-input" value="${entity.icon || ''}" 
                                   maxlength="4" placeholder="🔹">
                        </div>
                    </div>
                    <div class="ovz-modal-footer">
                        <button class="ovz-btn ovz-btn-secondary" data-action="close">Close</button>
                        <button class="ovz-btn ovz-btn-primary" data-action="save">Save</button>
                    </div>
                </div>
            `;

            this.container.appendChild(modal);

            // Event handlers - close buttons (both X and Close button)
            modal.querySelectorAll('[data-action="close"]').forEach(btn => {
                btn.addEventListener('click', () => this._closeModal());
            });
            
            modal.querySelectorAll('.ovz-icon-option').forEach(btn => {
                btn.addEventListener('click', () => {
                    modal.querySelectorAll('.ovz-icon-option').forEach(b => b.classList.remove('ovz-selected'));
                    btn.classList.add('ovz-selected');
                    modal.querySelector('#ovz-custom-icon').value = btn.dataset.icon;
                });
            });

            modal.querySelector('[data-action="save"]').addEventListener('click', () => {
                const newIcon = modal.querySelector('#ovz-custom-icon').value.trim() || '📦';
                entity.icon = newIcon;
                this._renderEntity(entity);
                this._updateRelationshipPaths();
                this._closeModal();
                if (this.options.onEntityUpdate) {
                    this.options.onEntityUpdate(entity);
                }
            });

            // Close on backdrop click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this._closeModal();
            });
        }

        _showDescriptionEditor(entity) {
            // Remove existing modal if any
            this._closeModal();

            // Create modal
            const modal = document.createElement('div');
            modal.className = 'ovz-modal';
            modal.innerHTML = `
                <div class="ovz-modal-content ovz-description-editor">
                    <div class="ovz-modal-header">
                        <h3>Edit Description</h3>
                        <button class="ovz-modal-close" data-action="close">×</button>
                    </div>
                    <div class="ovz-modal-body">
                        <label>Description for "${entity.name}":</label>
                        <textarea id="ovz-description-input" rows="4" 
                                  placeholder="Enter a description for this entity...">${this._escapeHtml(entity.description || '')}</textarea>
                    </div>
                    <div class="ovz-modal-footer">
                        <button class="ovz-btn ovz-btn-secondary" data-action="close">Cancel</button>
                        <button class="ovz-btn ovz-btn-primary" data-action="save">Save</button>
                    </div>
                </div>
            `;

            this.container.appendChild(modal);

            // Focus the textarea
            const textarea = modal.querySelector('#ovz-description-input');
            textarea.focus();
            textarea.setSelectionRange(textarea.value.length, textarea.value.length);

            // Event handlers - close buttons (both X and Cancel button)
            modal.querySelectorAll('[data-action="close"]').forEach(btn => {
                btn.addEventListener('click', () => this._closeModal());
            });
            
            modal.querySelector('[data-action="save"]').addEventListener('click', () => {
                entity.description = textarea.value.trim();
                this._renderEntity(entity);
                this._updateRelationshipPaths();
                this._closeModal();
                if (this.options.onEntityUpdate) {
                    this.options.onEntityUpdate(entity);
                }
            });

            // Close on backdrop click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this._closeModal();
            });

            // Save on Ctrl+Enter
            textarea.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                    modal.querySelector('[data-action="save"]').click();
                }
            });
        }

        _closeModal() {
            const modal = this.container.querySelector('.ovz-modal');
            if (modal) {
                modal.remove();
            }
        }

        /**
         * Show a dialog asking whether to delete, hide, or cancel for an entity/relationship/inheritance
         * @param {string} type - 'entity', 'relationship', or 'inheritance'
         * @param {string} id - The ID of the item
         * @param {string} name - Display name for the item
         */
        _showDeleteOrHideDialog(type, id, name) {
            // Remove existing modal if any
            this._closeModal();

            // Determine icon and type label
            let typeLabel, icon;
            if (type === 'entity') {
                const entity = this.entities.get(id);
                icon = entity?.icon || '📦';
                typeLabel = 'Entity';
            } else if (type === 'relationship') {
                icon = '🔗';
                typeLabel = 'Relationship';
            } else if (type === 'inheritance') {
                icon = '↗️';
                typeLabel = 'Inheritance';
            }

            // Create modal
            const modal = document.createElement('div');
            modal.className = 'ovz-modal';
            modal.innerHTML = `
                <div class="ovz-modal-content ovz-delete-dialog">
                    <div class="ovz-modal-header">
                        <h3>${icon} ${typeLabel}: ${this._escapeHtml(name)}</h3>
                        <button class="ovz-modal-close" data-action="cancel">×</button>
                    </div>
                    <div class="ovz-modal-body" style="padding: 20px; text-align: center;">
                        <p style="margin-bottom: 20px; font-size: 14px;">What would you like to do with this ${typeLabel.toLowerCase()}?</p>
                        <div style="display: flex; gap: 10px; justify-content: center; flex-wrap: wrap;">
                            <button class="ovz-btn ovz-btn-danger" data-action="delete" style="min-width: 100px;">
                                <span style="margin-right: 5px;">🗑️</span> Delete
                            </button>
                            <button class="ovz-btn ovz-btn-secondary" data-action="hide" style="min-width: 100px;">
                                <span style="margin-right: 5px;">👁️</span> Hide
                            </button>
                            <button class="ovz-btn ovz-btn-outline" data-action="cancel" style="min-width: 100px;">
                                Cancel
                            </button>
                        </div>
                        <p style="margin-top: 15px; font-size: 12px; color: #666;">
                            <strong>Delete</strong> permanently removes the item.<br>
                            <strong>Hide</strong> keeps it but hides from view (use palette to show again).
                        </p>
                    </div>
                </div>
            `;

            this.container.appendChild(modal);

            // Event handlers
            modal.querySelector('[data-action="delete"]').addEventListener('click', () => {
                this._closeModal();
                if (type === 'entity') {
                    this.removeEntity(id);
                } else if (type === 'relationship') {
                    this.removeRelationship(id);
                } else if (type === 'inheritance') {
                    this.removeInheritance(id);
                }
            });

            modal.querySelector('[data-action="hide"]').addEventListener('click', () => {
                this._closeModal();
                this._toggleItemVisibility(type, id, false);
                // Fire visibility change callback
                if (this.options.onVisibilityChange) {
                    this.options.onVisibilityChange(type, id, false);
                }
            });

            modal.querySelectorAll('[data-action="cancel"]').forEach(btn => {
                btn.addEventListener('click', () => this._closeModal());
            });

            // Close on backdrop click
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this._closeModal();
            });

            // Close on Escape key
            const escHandler = (e) => {
                if (e.key === 'Escape') {
                    this._closeModal();
                    document.removeEventListener('keydown', escHandler);
                }
            };
            document.addEventListener('keydown', escHandler);
        }

        // ==========================================
        // Relationship Management
        // ==========================================
        addRelationship(options = {}) {
            if (!options.sourceEntityId || !options.targetEntityId) {
                console.error('OntoViz: sourceEntityId and targetEntityId are required');
                return null;
            }

            const relationship = new Relationship(options);
            this.relationships.set(relationship.id, relationship);
            this._renderRelationship(relationship);
            this._updateStatusBar();

            if (this.options.onRelationshipCreate) {
                this.options.onRelationshipCreate(relationship);
            }

            return relationship;
        }

        removeRelationship(relationshipId) {
            const relationship = this.relationships.get(relationshipId);
            if (!relationship) return;

            if (relationship.pathElement) {
                relationship.pathElement.remove();
            }
            if (relationship.pathElement2) {
                relationship.pathElement2.remove();
            }
            if (relationship.labelElement) {
                relationship.labelElement.remove();
            }

            this.relationships.delete(relationshipId);

            if (this.selectedRelationship?.id === relationshipId) {
                this.selectedRelationship = null;
            }

            this._updateStatusBar();

            if (this.options.onRelationshipDelete) {
                this.options.onRelationshipDelete(relationship);
            }
        }

        updateRelationship(relationshipId, updates) {
            const relationship = this.relationships.get(relationshipId);
            if (!relationship) return;

            Object.assign(relationship, updates);
            this._renderRelationship(relationship);

            if (this.options.onRelationshipUpdate) {
                this.options.onRelationshipUpdate(relationship);
            }

            return relationship;
        }

        // ==========================================
        // Inheritance Management
        // ==========================================
        
        /**
         * Add an inheritance link between two entities
         * No self-loops allowed for inheritance
         * @param {Object} options - { sourceEntityId, targetEntityId, direction }
         */
        addInheritance(options = {}) {
            if (!options.sourceEntityId || !options.targetEntityId) {
                console.error('OntoViz: sourceEntityId and targetEntityId are required for inheritance');
                return null;
            }

            // No self-loops for inheritance
            if (options.sourceEntityId === options.targetEntityId) {
                console.error('OntoViz: Inheritance cannot link an entity to itself');
                return null;
            }

            const inheritance = new Inheritance(options);
            this.inheritances.set(inheritance.id, inheritance);
            this._renderInheritance(inheritance);
            
            // Re-render the child entity to show inherited properties
            this._updateEntityWithInheritance(inheritance.getChildEntityId());
            
            this._updateStatusBar();

            if (this.options.onInheritanceCreate) {
                this.options.onInheritanceCreate(inheritance);
            }

            return inheritance;
        }

        removeInheritance(inheritanceId) {
            const inheritance = this.inheritances.get(inheritanceId);
            if (!inheritance) return;

            const childEntityId = inheritance.getChildEntityId();

            if (inheritance.pathElement) {
                inheritance.pathElement.remove();
            }
            if (inheritance.arrowElement) {
                inheritance.arrowElement.remove();
            }

            this.inheritances.delete(inheritanceId);

            if (this.selectedInheritance?.id === inheritanceId) {
                this.selectedInheritance = null;
            }

            // Re-render the child entity to remove inherited properties
            this._updateEntityWithInheritance(childEntityId);

            this._updateStatusBar();

            if (this.options.onInheritanceDelete) {
                this.options.onInheritanceDelete(inheritance);
            }
        }

        updateInheritance(inheritanceId, updates) {
            const inheritance = this.inheritances.get(inheritanceId);
            if (!inheritance) return;

            const oldChildEntityId = inheritance.getChildEntityId();
            
            Object.assign(inheritance, updates);
            this._renderInheritance(inheritance);
            
            const newChildEntityId = inheritance.getChildEntityId();
            
            // Re-render affected entities
            this._updateEntityWithInheritance(oldChildEntityId);
            if (oldChildEntityId !== newChildEntityId) {
                this._updateEntityWithInheritance(newChildEntityId);
            }

            if (this.options.onInheritanceUpdate) {
                this.options.onInheritanceUpdate(inheritance);
            }

            return inheritance;
        }

        /**
         * Get all inherited properties for an entity
         * @param {string} entityId 
         * @returns {Array} Array of inherited properties with source info
         */
        getInheritedProperties(entityId) {
            const inheritedProps = [];
            
            this.inheritances.forEach(inheritance => {
                if (inheritance.getChildEntityId() === entityId) {
                    const parentEntity = this.entities.get(inheritance.getParentEntityId());
                    if (parentEntity && parentEntity.properties) {
                        parentEntity.properties.forEach(prop => {
                            inheritedProps.push({
                                ...prop,
                                inherited: true,
                                inheritedFrom: parentEntity.name,
                                inheritedFromId: parentEntity.id
                            });
                        });
                    }
                }
            });
            
            return inheritedProps;
        }

        /**
         * Update entity display to show/hide inherited properties
         */
        _updateEntityWithInheritance(entityId) {
            const entity = this.entities.get(entityId);
            if (entity) {
                this._renderEntity(entity);
            }
        }

        /**
         * Re-render all child entities that inherit from a given parent entity
         * Called when a parent's properties change (add/remove/update)
         * @param {string} parentEntityId - The ID of the parent entity that changed
         */
        _rerenderChildEntities(parentEntityId) {
            // Find all inheritances where this entity is the parent
            this.inheritances.forEach(inheritance => {
                if (inheritance.getParentEntityId() === parentEntityId) {
                    const childEntityId = inheritance.getChildEntityId();
                    this._updateEntityWithInheritance(childEntityId);
                }
            });
        }

        /**
         * Render inheritance as a dotted straight line with arrow
         */
        _renderInheritance(inheritance) {
            // Remove existing elements
            if (inheritance.pathElement) {
                inheritance.pathElement.remove();
            }
            if (inheritance.arrowElement) {
                inheritance.arrowElement.remove();
            }

            const sourceEntity = this.entities.get(inheritance.sourceEntityId);
            const targetEntity = this.entities.get(inheritance.targetEntityId);

            if (!sourceEntity || !targetEntity) return;

            // Calculate optimal anchors for straight line
            const sourceRect = {
                x: sourceEntity.x,
                y: sourceEntity.y,
                width: sourceEntity.element?.offsetWidth || 220,
                height: sourceEntity.element?.offsetHeight || 100
            };
            const targetRect = {
                x: targetEntity.x,
                y: targetEntity.y,
                width: targetEntity.element?.offsetWidth || 220,
                height: targetEntity.element?.offsetHeight || 100
            };

            // Calculate centers
            const sourceCenter = {
                x: sourceRect.x + sourceRect.width / 2,
                y: sourceRect.y + sourceRect.height / 2
            };
            const targetCenter = {
                x: targetRect.x + targetRect.width / 2,
                y: targetRect.y + targetRect.height / 2
            };

            // Determine optimal anchors based on relative positions
            const dx = targetCenter.x - sourceCenter.x;
            const dy = targetCenter.y - sourceCenter.y;

            let sourceAnchor, targetAnchor;
            if (Math.abs(dx) > Math.abs(dy)) {
                // Horizontal connection
                if (dx > 0) {
                    sourceAnchor = 'right';
                    targetAnchor = 'left';
                } else {
                    sourceAnchor = 'left';
                    targetAnchor = 'right';
                }
            } else {
                // Vertical connection
                if (dy > 0) {
                    sourceAnchor = 'bottom';
                    targetAnchor = 'top';
                } else {
                    sourceAnchor = 'top';
                    targetAnchor = 'bottom';
                }
            }

            // Get anchor points
            const sourcePoint = this._getEntityAnchorPoint(sourceEntity, sourceAnchor);
            const targetPoint = this._getEntityAnchorPoint(targetEntity, targetAnchor);

            // Determine which end gets the arrow based on direction
            // Arrow points TO the child (the one inheriting)
            const arrowAtEnd = inheritance.direction === 'forward';

            // Create SVG path (dotted line)
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.classList.add('ovz-inheritance');
            path.dataset.inheritanceId = inheritance.id;
            
            // Create path that routes around unrelated entities
            const skip = new Set([inheritance.sourceEntityId, inheritance.targetEntityId]);
            const pathD = this._createSmoothCurve(sourcePoint, targetPoint, 'horizontal', 'entry', skip);
            path.setAttribute('d', pathD);
            
            // Add arrow marker
            if (arrowAtEnd) {
                path.setAttribute('marker-end', 'url(#ovz-inheritance-arrow)');
            } else {
                path.setAttribute('marker-start', 'url(#ovz-inheritance-arrow-start)');
            }

            inheritance.pathElement = path;
            this.svgLayer.appendChild(path);

            // Bind click event to toggle direction
            path.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectInheritance(inheritance);
            });
            
            // Double-click to toggle direction
            path.addEventListener('dblclick', (e) => {
                e.stopPropagation();
                const oldChildId = inheritance.getChildEntityId();
                inheritance.cycleDirection();
                const newChildId = inheritance.getChildEntityId();
                
                this._renderInheritance(inheritance);
                
                // Update both entities' inherited properties
                this._updateEntityWithInheritance(oldChildId);
                this._updateEntityWithInheritance(newChildId);
                
                if (this.options.onInheritanceUpdate) {
                    this.options.onInheritanceUpdate(inheritance);
                }
            });
            
            // Apply visibility state if inheritance should be hidden
            // This ensures visibility is preserved when inheritances are re-rendered
            const sourceVisible = this.visibilityState.entities.get(inheritance.sourceEntityId) !== false;
            const targetVisible = this.visibilityState.entities.get(inheritance.targetEntityId) !== false;
            const inhExplicitlyVisible = this.visibilityState.inheritances.get(inheritance.id) !== false;
            const shouldBeVisible = sourceVisible && targetVisible && inhExplicitlyVisible;
            
            if (!shouldBeVisible) {
                if (inheritance.pathElement) inheritance.pathElement.style.visibility = 'hidden';
                if (inheritance.arrowElement) inheritance.arrowElement.style.visibility = 'hidden';
            }
        }

        _selectInheritance(inheritance) {
            // Clear previous selection
            this._clearSelection();
            
            this.selectedInheritance = inheritance;
            
            if (inheritance.pathElement) {
                inheritance.pathElement.classList.add('ovz-selected');
            }

            if (this.options.onSelectionChange) {
                this.options.onSelectionChange({ type: 'inheritance', item: inheritance });
            }
        }

        /**
         * Update all inheritance paths (called when entities move)
         */
        _updateInheritancePaths() {
            this.inheritances.forEach(inheritance => {
                this._renderInheritance(inheritance);
            });
        }

        /**
         * Toggle inheritance mode on/off
         * When active, dragging from entity connector creates inheritance instead of relationship
         */
        _toggleInheritanceMode() {
            if (this.entities.size < 2) {
                // Use showNotification if available, otherwise console.warn
                if (typeof window.showNotification === 'function') {
                    window.showNotification('You need at least 2 entities to create an inheritance link.', 'warning');
                } else {
                    console.warn('You need at least 2 entities to create an inheritance link.');
                }
                return;
            }

            this.isInheritanceMode = !this.isInheritanceMode;
            
            // Update toolbar button state
            const inheritanceBtn = this.container.querySelector('[data-action="add-inheritance"]');
            if (inheritanceBtn) {
                inheritanceBtn.classList.toggle('ovz-active', this.isInheritanceMode);
            }
            
            // Update canvas state
            this.container.classList.toggle('ovz-inheritance-mode', this.isInheritanceMode);
            
            if (this.isInheritanceMode) {
                this._showInheritanceInstructions('Drag from PARENT entity connector to CHILD entity to create inheritance');
            } else {
                this._hideInheritanceInstructions();
            }
        }

        _showInheritanceInstructions(message) {
            // Remove existing instructions
            this._hideInheritanceInstructions();
            
            const instructions = document.createElement('div');
            instructions.className = 'ovz-inheritance-instructions';
            instructions.innerHTML = `
                <span>${message}</span>
                <button class="ovz-inheritance-cancel">Exit Mode (Esc)</button>
            `;
            instructions.querySelector('.ovz-inheritance-cancel').addEventListener('click', () => {
                this._toggleInheritanceMode();
            });
            this.container.appendChild(instructions);
        }

        _hideInheritanceInstructions() {
            const existing = this.container.querySelector('.ovz-inheritance-instructions');
            if (existing) {
                existing.remove();
            }
        }

        _exitInheritanceMode() {
            if (this.isInheritanceMode) {
                this._toggleInheritanceMode();
            }
        }

        _renderRelationship(relationship) {
            // Remove existing elements
            if (relationship.pathElement) {
                relationship.pathElement.remove();
            }
            if (relationship.pathElement2) {
                relationship.pathElement2.remove();
            }
            if (relationship.labelElement) {
                relationship.labelElement.remove();
            }

            const sourceEntity = this.entities.get(relationship.sourceEntityId);
            const targetEntity = this.entities.get(relationship.targetEntityId);

            if (!sourceEntity || !targetEntity) return;

            // Calculate connection points
            // Pass relationship to use explicit anchor settings if set (Line 28)
            const points = this._calculateConnectionPoints(sourceEntity, targetEntity, relationship);

            // Calculate offset for multiple relationships between same entities
            const relInfo = this._getRelationshipIndex(relationship);
            let offset = 0;
            if (relInfo.total > 1) {
                // Spread relationships with 30px spacing, centered around 0
                const spacing = 30;
                offset = (relInfo.index - (relInfo.total - 1) / 2) * spacing;
            }

            // Calculate base midpoint for label position
            let baseMidPoint;
            
            if (points.connectionType === 'self') {
                // For self-loops, use stored label position or calculate initial position
                // Initial position: top-right of entity center
                const entityWidth = sourceEntity.element?.offsetWidth || 220;
                const entityHeight = sourceEntity.element?.offsetHeight || 100;
                const entityCenterX = sourceEntity.x + entityWidth / 2;
                const entityCenterY = sourceEntity.y + entityHeight / 2;
                const loopSize = 80 + Math.abs(offset) * 0.5;
                
                baseMidPoint = {
                    x: entityCenterX + loopSize * 0.7 + offset * 0.5,
                    y: entityCenterY - loopSize * 0.7
                };
            } else {
                const dx = points.target.x - points.source.x;
                const dy = points.target.y - points.source.y;
                const distance = Math.sqrt(dx * dx + dy * dy) || 1;
                const perpX = -dy / distance * offset;
                const perpY = dx / distance * offset;
                
                // Calculate base midpoint (for offset calculations)
                baseMidPoint = {
                    x: (points.source.x + points.target.x) / 2 + perpX,
                    y: (points.source.y + points.target.y) / 2 + perpY
                };
            }
            
            // Apply custom label offset if set
            const midPoint = {
                x: baseMidPoint.x + (relationship.labelOffsetX || 0),
                y: baseMidPoint.y + (relationship.labelOffsetY || 0)
            };
            
            // Rule 17: "A relationship is represented by 2 lines and a relationship descriptive box"
            // Calculate optimal anchors based on positions to avoid hiding lines behind boxes
            const { entryAnchor, exitAnchor } = this._calculateOptimalAnchors(
                sourceEntity, targetEntity, midPoint, points.connectionType
            );
            
            // Calculate optimal entity anchors based on relationship box position
            // For self-loops, calculate two different optimal anchors for source and target
            let sourceEntityAnchor, targetEntityAnchor;
            if (points.connectionType === 'self') {
                const selfAnchors = this._calculateSelfLoopAnchors(sourceEntity, midPoint);
                sourceEntityAnchor = selfAnchors.sourceAnchor;
                targetEntityAnchor = selfAnchors.targetAnchor;
            } else {
                sourceEntityAnchor = this._calculateOptimalEntityAnchor(sourceEntity, midPoint);
                targetEntityAnchor = this._calculateOptimalEntityAnchor(targetEntity, midPoint);
            }
            
            // Get actual entity anchor points
            const sourcePoint = this._getEntityAnchorPoint(sourceEntity, sourceEntityAnchor);
            const targetPoint = this._getEntityAnchorPoint(targetEntity, targetEntityAnchor);
            
            // Store connection info for path updates
            relationship._entryAnchor = entryAnchor;
            relationship._exitAnchor = exitAnchor;
            relationship._connectionType = points.connectionType;
            relationship._sourceEntityAnchor = sourceEntityAnchor;
            relationship._targetEntityAnchor = targetEntityAnchor;
            relationship._sourcePoint = sourcePoint;
            relationship._targetPoint = targetPoint;

            // Create both SVG paths (will be updated after label is rendered)
            const path1 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path1.classList.add('ovz-relationship');
            path1.dataset.relationshipId = relationship.id;
            
            const path2 = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path2.classList.add('ovz-relationship');
            path2.dataset.relationshipId = relationship.id;
            
            // Set arrow markers based on direction
            if (relationship.direction === 'forward') {
                path2.setAttribute('marker-end', 'url(#ovz-arrow)');
            } else if (relationship.direction === 'reverse') {
                path1.setAttribute('marker-start', 'url(#ovz-arrow-start)');
            }

            // Store both paths
            relationship.pathElement = path1;
            relationship.pathElement2 = path2;
            this.svgLayer.appendChild(path1);
            this.svgLayer.appendChild(path2);

            const label = document.createElement('div');
            label.className = 'ovz-relationship-label ovz-draggable';
            label.dataset.relationshipId = relationship.id;
            label.style.left = midPoint.x + 'px';
            label.style.top = midPoint.y + 'px';
            label.style.transform = 'translate(-50%, -50%)';
            label.style.cursor = 'move';
            
            // Note: Relationship attributes are not supported
            
            // Check if in view-only mode
            const isViewOnly = this.options.viewOnly === true;
            
            // Build direction indicator with entity names
            const sourceName = sourceEntity.name;
            const targetName = targetEntity.name;
            let directionIcon, directionTitle;
            if (relationship.direction === 'reverse') {
                directionIcon = '←';
                directionTitle = `${targetName} → ${sourceName}`;
            } else {
                // Default to forward
                directionIcon = '→';
                directionTitle = `${sourceName} → ${targetName}`;
            }
            // Note: For bidirectional relationships, create two separate arrows
            
            // Relationship name - text in view-only, input in edit mode
            const relNameHTML = isViewOnly 
                ? `<span class="ovz-rel-name-text">${this._escapeHtml(relationship.name)}</span>`
                : `<input type="text" value="${this._escapeHtml(relationship.name)}" spellcheck="false" class="ovz-rel-name-input">`;
            
            // Action buttons - only shown in edit mode
            const relActionsHTML = isViewOnly ? '' : `
                <button class="ovz-rel-direction" data-action="toggle-direction" title="${directionTitle}">${directionIcon}</button>
                <button class="ovz-rel-delete" data-action="delete-rel" title="Delete Relationship">×</button>
            `;
            
            label.innerHTML = `
                <div class="ovz-rel-header">
                    ${relNameHTML}
                    ${relActionsHTML}
                </div>
                <div class="ovz-rel-direction-label" ${!isViewOnly ? 'data-action="toggle-direction"' : ''} title="${isViewOnly ? '' : 'Click to change direction'}">
                    ${directionTitle}
                </div>
                <div class="ovz-rel-anchor ovz-rel-anchor-left" data-anchor="left"></div>
                <div class="ovz-rel-anchor ovz-rel-anchor-right" data-anchor="right"></div>
                <div class="ovz-rel-anchor ovz-rel-anchor-top" data-anchor="top"></div>
                <div class="ovz-rel-anchor ovz-rel-anchor-bottom" data-anchor="bottom"></div>
            `;

            relationship.labelElement = label;
            this.canvasInner.appendChild(label);

            // Store base midpoint for drag offset calculation
            label._baseMidPoint = { ...baseMidPoint };
            
            // Highlight active anchors based on calculated optimal anchors
            // Lines enter from one side and exit from another
            this._updateRelationshipAnchors(label, points.connectionType, entryAnchor, exitAnchor);
            
            // Now that label is rendered, update paths to connect to actual anchor positions
            // Use requestAnimationFrame to ensure label dimensions are calculated
            requestAnimationFrame(() => {
                this._updatePathsToAnchors(relationship, midPoint);
            });
            
            // Bind events for both paths (Rule 17: 2 lines)
            path1.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectRelationship(relationship);
            });
            
            path2.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectRelationship(relationship);
            });

            label.addEventListener('click', (e) => {
                e.stopPropagation();
                this._selectRelationship(relationship);
            });
            
            // Drag functionality for relationship label
            // Rule: Always try to place the Relationship box between the 2 entities (Line 25)
            let isDragging = false;
            let dragStartX = 0;
            let dragStartY = 0;
            let initialOffsetX = relationship.labelOffsetX || 0;
            let initialOffsetY = relationship.labelOffsetY || 0;
            
            label.addEventListener('mousedown', (e) => {
                // Don't start drag on inputs or buttons
                if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON') return;
                
                isDragging = true;
                dragStartX = e.clientX;
                dragStartY = e.clientY;
                initialOffsetX = relationship.labelOffsetX || 0;
                initialOffsetY = relationship.labelOffsetY || 0;
                label.classList.add('ovz-dragging');
                e.preventDefault();
                
                const onMouseMove = (moveEvent) => {
                    if (!isDragging) return;
                    
                    const deltaX = moveEvent.clientX - dragStartX;
                    const deltaY = moveEvent.clientY - dragStartY;
                    
                    // Calculate proposed new position
                    let newOffsetX = initialOffsetX + deltaX;
                    let newOffsetY = initialOffsetY + deltaY;
                    
                    // Rule (Line 25): Constrain label to stay between the two entities
                    const sourceEntity = this.entities.get(relationship.sourceId);
                    const targetEntity = this.entities.get(relationship.targetId);
                    
                    if (sourceEntity && targetEntity) {
                        const labelWidth = label.offsetWidth || 120;
                        const labelHeight = label.offsetHeight || 60;
                        const padding = 20; // Padding from entities
                        
                        // Calculate bounding box between entities
                        const minX = Math.min(sourceEntity.x, targetEntity.x) - padding;
                        const maxX = Math.max(sourceEntity.x + 150, targetEntity.x + 150) + padding;
                        const minY = Math.min(sourceEntity.y, targetEntity.y) - padding;
                        const maxY = Math.max(sourceEntity.y + 80, targetEntity.y + 80) + padding;
                        
                        // Calculate new label center position
                        const labelCenterX = label._baseMidPoint.x + newOffsetX;
                        const labelCenterY = label._baseMidPoint.y + newOffsetY;
                        
                        // Constrain to bounding box
                        const constrainedX = Math.max(minX, Math.min(maxX, labelCenterX));
                        const constrainedY = Math.max(minY, Math.min(maxY, labelCenterY));
                        
                        // Recalculate offset from constrained position
                        newOffsetX = constrainedX - label._baseMidPoint.x;
                        newOffsetY = constrainedY - label._baseMidPoint.y;
                    }
                    
                    // Update offset
                    relationship.labelOffsetX = newOffsetX;
                    relationship.labelOffsetY = newOffsetY;
                    
                    // Update label position directly
                    const newX = label._baseMidPoint.x + relationship.labelOffsetX;
                    const newY = label._baseMidPoint.y + relationship.labelOffsetY;
                    label.style.left = newX + 'px';
                    label.style.top = newY + 'px';
                    
                    // Update both paths to follow label (Rule 17: 2 lines)
                    this._updateRelationshipPathsForDrag(relationship, points, { x: newX, y: newY });
                };
                
                const onMouseUp = () => {
                    if (isDragging) {
                        isDragging = false;
                        label.classList.remove('ovz-dragging');
                        
                        // Trigger update callback
                        if (this.options.onRelationshipUpdate) {
                            this.options.onRelationshipUpdate(relationship);
                        }
                    }
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);
                };
                
                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });

            const labelInput = label.querySelector('.ovz-rel-name-input');
            if (labelInput) {
                this._restrictToValidChars(labelInput);
                // Track original name for rename detection
                let originalName = relationship.name;
                labelInput.addEventListener('focus', () => {
                    originalName = relationship.name;
                });
                labelInput.addEventListener('change', () => {
                    const newName = labelInput.value;
                    const wasRenamed = originalName !== newName;
                    relationship.name = newName;
                    if (this.options.onRelationshipUpdate) {
                        // Pass rename info to callback
                        this.options.onRelationshipUpdate(relationship, { 
                            renamed: wasRenamed, 
                            oldName: wasRenamed ? originalName : null 
                        });
                    }
                    originalName = newName;
                });
                labelInput.addEventListener('click', (e) => e.stopPropagation());
            }
            
            // Toggle direction button and label
            label.querySelectorAll('[data-action="toggle-direction"]').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    relationship.cycleDirection();
                    this._renderRelationship(relationship);
                    if (this.options.onRelationshipUpdate) {
                        this.options.onRelationshipUpdate(relationship);
                    }
                });
            });
            
            // Note: Relationship attributes are not supported
            
            // Delete relationship button
            const deleteRelBtn = label.querySelector('[data-action="delete-rel"]');
            if (deleteRelBtn) {
                deleteRelBtn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this._showDeleteOrHideDialog('relationship', relationship.id, relationship.name);
                });
            }
            
            // Apply visibility state if relationship should be hidden
            // This ensures visibility is preserved when relationships are re-rendered
            const sourceVisible = this.visibilityState.entities.get(relationship.sourceEntityId) !== false;
            const targetVisible = this.visibilityState.entities.get(relationship.targetEntityId) !== false;
            const relExplicitlyVisible = this.visibilityState.relationships.get(relationship.id) !== false;
            const shouldBeVisible = sourceVisible && targetVisible && relExplicitlyVisible;
            
            if (!shouldBeVisible) {
                const visValue = 'hidden';
                if (relationship.pathElement) relationship.pathElement.style.visibility = visValue;
                if (relationship.pathElement2) relationship.pathElement2.style.visibility = visValue;
                if (relationship.labelElement) relationship.labelElement.style.visibility = visValue;
            }
        }

        /**
         * Calculate connection points between two entities
         * Rule (Line 28): it is possible to drag and change the origin of a line from an anchor to another
         * @param {Entity} sourceEntity 
         * @param {Entity} targetEntity 
         * @param {Relationship} relationship - Optional, used to get explicit anchor settings
         */
        _calculateConnectionPoints(sourceEntity, targetEntity, relationship = null) {
            // Anchor positions are at the CENTER of the connector circles
            // Connectors are 14px circles positioned at -7px from edge, so center is AT the edge
            
            const sourceRect = {
                x: sourceEntity.x,
                y: sourceEntity.y,
                width: sourceEntity.element?.offsetWidth || 220,
                height: sourceEntity.element?.offsetHeight || 100
            };

            const targetRect = {
                x: targetEntity.x,
                y: targetEntity.y,
                width: targetEntity.element?.offsetWidth || 220,
                height: targetEntity.element?.offsetHeight || 100
            };

            // Helper to get anchor point for an entity (at center of connector circle = entity edge)
            const getAnchorPoint = (rect, anchor, center) => {
                switch (anchor) {
                    case 'top':
                        return { x: center.x, y: rect.y };
                    case 'bottom':
                        return { x: center.x, y: rect.y + rect.height };
                    case 'left':
                        return { x: rect.x, y: center.y };
                    case 'right':
                        return { x: rect.x + rect.width, y: center.y };
                    default:
                        return null; // 'auto'
                }
            };

            // Handle self-relationship (entity connected to itself)
            if (sourceEntity.id === targetEntity.id) {
                const center = { x: sourceRect.x + sourceRect.width / 2, y: sourceRect.y + sourceRect.height / 2 };
                
                // Get stored anchors - these are dynamically calculated based on label position
                let sourceAnchor = relationship?._sourceEntityAnchor || relationship?.sourceAnchor;
                let targetAnchor = relationship?._targetEntityAnchor || relationship?.targetAnchor;
                
                // If no stored anchors, calculate based on label offset or use top-right default
                if (!sourceAnchor || sourceAnchor === 'auto' || !targetAnchor || targetAnchor === 'auto') {
                    // Calculate label position to determine initial anchors
                    const labelOffsetX = relationship?.labelOffsetX || 0;
                    const labelOffsetY = relationship?.labelOffsetY || 0;
                    // Default label position is top-right of entity center
                    const defaultLabelX = center.x + 80;
                    const defaultLabelY = center.y - 80;
                    const labelX = defaultLabelX + labelOffsetX;
                    const labelY = defaultLabelY + labelOffsetY;
                    
                    // Calculate direction from entity to label
                    const dx = labelX - center.x;
                    const dy = labelY - center.y;
                    const absDx = Math.abs(dx);
                    const absDy = Math.abs(dy);
                    
                    // Choose adjacent anchors facing toward the label
                    if (absDx > absDy) {
                        if (dx > 0) {
                            sourceAnchor = dy < 0 ? 'right' : 'bottom';
                            targetAnchor = dy < 0 ? 'top' : 'right';
                        } else {
                            sourceAnchor = dy < 0 ? 'top' : 'left';
                            targetAnchor = dy < 0 ? 'left' : 'bottom';
                        }
                    } else {
                        if (dy < 0) {
                            sourceAnchor = dx > 0 ? 'right' : 'top';
                            targetAnchor = dx > 0 ? 'top' : 'left';
                        } else {
                            sourceAnchor = dx > 0 ? 'bottom' : 'left';
                            targetAnchor = dx > 0 ? 'right' : 'bottom';
                        }
                    }
                }
                
                const sourcePoint = getAnchorPoint(sourceRect, sourceAnchor, center);
                const targetPoint = getAnchorPoint(sourceRect, targetAnchor, center);
                
                return { source: sourcePoint, target: targetPoint, connectionType: 'self' };
            }

            // Calculate centers
            const sourceCenter = {
                x: sourceRect.x + sourceRect.width / 2,
                y: sourceRect.y + sourceRect.height / 2
            };

            const targetCenter = {
                x: targetRect.x + targetRect.width / 2,
                y: targetRect.y + targetRect.height / 2
            };

            // Check for explicit anchor settings (Line 28)
            const sourceAnchor = relationship?.sourceAnchor || 'auto';
            const targetAnchor = relationship?.targetAnchor || 'auto';
            
            let sourcePoint, targetPoint;
            let connectionType; // 'horizontal' or 'vertical'

            // If explicit anchors are set, use them
            if (sourceAnchor !== 'auto' && targetAnchor !== 'auto') {
                sourcePoint = getAnchorPoint(sourceRect, sourceAnchor, sourceCenter);
                targetPoint = getAnchorPoint(targetRect, targetAnchor, targetCenter);
                
                // Determine connection type based on anchors
                const isHorizontalSource = sourceAnchor === 'left' || sourceAnchor === 'right';
                const isHorizontalTarget = targetAnchor === 'left' || targetAnchor === 'right';
                connectionType = (isHorizontalSource && isHorizontalTarget) ? 'horizontal' : 'vertical';
                
                return { source: sourcePoint, target: targetPoint, connectionType };
            }

            // Auto-determine best connection sides
            const dx = targetCenter.x - sourceCenter.x;
            const dy = targetCenter.y - sourceCenter.y;

            if (Math.abs(dx) > Math.abs(dy)) {
                // Horizontal connection - connect to left/right connector centers (at entity edge)
                connectionType = 'horizontal';
                if (dx > 0) {
                    // Source right connector to target left connector
                    sourcePoint = sourceAnchor === 'auto' 
                        ? { x: sourceRect.x + sourceRect.width, y: sourceCenter.y }
                        : getAnchorPoint(sourceRect, sourceAnchor, sourceCenter);
                    targetPoint = targetAnchor === 'auto'
                        ? { x: targetRect.x, y: targetCenter.y }
                        : getAnchorPoint(targetRect, targetAnchor, targetCenter);
                } else {
                    // Source left connector to target right connector
                    sourcePoint = sourceAnchor === 'auto'
                        ? { x: sourceRect.x, y: sourceCenter.y }
                        : getAnchorPoint(sourceRect, sourceAnchor, sourceCenter);
                    targetPoint = targetAnchor === 'auto'
                        ? { x: targetRect.x + targetRect.width, y: targetCenter.y }
                        : getAnchorPoint(targetRect, targetAnchor, targetCenter);
                }
            } else {
                // Vertical connection - connect to top/bottom connector centers (at entity edge)
                connectionType = 'vertical';
                if (dy > 0) {
                    // Source bottom connector to target top connector
                    sourcePoint = sourceAnchor === 'auto'
                        ? { x: sourceCenter.x, y: sourceRect.y + sourceRect.height }
                        : getAnchorPoint(sourceRect, sourceAnchor, sourceCenter);
                    targetPoint = targetAnchor === 'auto'
                        ? { x: targetCenter.x, y: targetRect.y }
                        : getAnchorPoint(targetRect, targetAnchor, targetCenter);
                } else {
                    // Source top connector to target bottom connector
                    sourcePoint = sourceAnchor === 'auto'
                        ? { x: sourceCenter.x, y: sourceRect.y }
                        : getAnchorPoint(sourceRect, sourceAnchor, sourceCenter);
                    targetPoint = targetAnchor === 'auto'
                        ? { x: targetCenter.x, y: targetRect.y + targetRect.height }
                        : getAnchorPoint(targetRect, targetAnchor, targetCenter);
                }
            }

            return { source: sourcePoint, target: targetPoint, connectionType };
        }

        /**
         * Get index and count of relationships between two entities
         * Used for offsetting multiple relationships between the same entities
         */
        _getRelationshipIndex(relationship) {
            const sourceId = relationship.sourceEntityId;
            const targetId = relationship.targetEntityId;
            
            // Get all relationships between these two entities (in either direction)
            const relsBetween = [];
            this.relationships.forEach(rel => {
                if ((rel.sourceEntityId === sourceId && rel.targetEntityId === targetId) ||
                    (rel.sourceEntityId === targetId && rel.targetEntityId === sourceId)) {
                    relsBetween.push(rel);
                }
            });
            
            // Sort by id for consistent ordering
            relsBetween.sort((a, b) => a.id.localeCompare(b.id));
            
            const index = relsBetween.findIndex(r => r.id === relationship.id);
            return { index, total: relsBetween.length };
        }

        _createCurvePath(source, target, offset = 0, labelOffset = null, connectionType = 'horizontal') {
            // Rule 23: "lines can be represented as curves (The lines should have no corners)"
            // Use smooth bezier curves only - no L (line) commands
            
            // Handle self-loop (entity connected to itself) - Rule 26
            if (connectionType === 'self') {
                const loopSize = 60 + Math.abs(offset) * 0.5;
                const loopOffsetX = offset * 0.5;
                
                // Calculate label center point for self-loop
                const midOffsetX = labelOffset ? labelOffset.x : 0;
                const midOffsetY = labelOffset ? labelOffset.y : 0;
                const labelX = source.x + loopSize * 0.5 + loopOffsetX + midOffsetX;
                const labelY = source.y - loopSize * 0.6 + midOffsetY;
                
                // Smooth loop using quadratic beziers
                const cp1 = { x: source.x + loopSize, y: source.y };
                const cp2 = { x: target.x, y: target.y - loopSize };
                
                return `M ${source.x} ${source.y} Q ${cp1.x} ${cp1.y}, ${labelX} ${labelY} Q ${cp2.x} ${cp2.y}, ${target.x} ${target.y}`;
            }

            // Calculate perpendicular offset for multiple relationships
            let perpX, perpY;
            if (connectionType === 'horizontal') {
                perpX = 0;
                perpY = offset;
            } else {
                perpX = offset;
                perpY = 0;
            }

            // Apply offset to source and target points
            const offsetSource = { x: source.x + perpX, y: source.y + perpY };
            const offsetTarget = { x: target.x + perpX, y: target.y + perpY };

            // Calculate the label center point (the path MUST pass through this point)
            // Rule 29: The Relationship box cannot be disconnected from the lines
            const labelX = (offsetSource.x + offsetTarget.x) / 2 + (labelOffset ? labelOffset.x : 0);
            const labelY = (offsetSource.y + offsetTarget.y) / 2 + (labelOffset ? labelOffset.y : 0);

            // Create smooth S-curve using two quadratic bezier curves
            // The path flows: Source → Control1 → Label → Control2 → Target
            // No corners - smooth curves throughout (Rule 23)
            
            if (connectionType === 'horizontal') {
                // Horizontal connection: smooth S-curve
                // Layout priority (Rules 32-33): left→right, curves flow naturally
                //
                //   Source ⟋                    
                //           ⟍                    
                //            ⟋ [Label] ⟍        
                //                        ⟋      
                //                         Target
                
                // Control points for smooth S-curve through label
                // First curve: source to label (horizontal exit, smooth approach)
                const cp1 = {
                    x: labelX,
                    y: offsetSource.y
                };
                
                // Second curve: label to target (smooth exit, horizontal entry)
                const cp2 = {
                    x: labelX,
                    y: offsetTarget.y
                };
                
                // Two quadratic beziers meeting at the label point
                return `M ${offsetSource.x} ${offsetSource.y} ` +
                       `Q ${cp1.x} ${cp1.y}, ${labelX} ${labelY} ` +
                       `Q ${cp2.x} ${cp2.y}, ${offsetTarget.x} ${offsetTarget.y}`;
                       
            } else {
                // Vertical connection: smooth S-curve
                // Layout priority (Rules 32-33): top→bottom, curves flow naturally
                //
                //      Source
                //        ⟍
                //         ⟋ [Label] 
                //        ⟍
                //      Target
                
                // Control points for smooth S-curve through label
                // First curve: source to label (vertical exit, smooth approach)
                const cp1 = {
                    x: offsetSource.x,
                    y: labelY
                };
                
                // Second curve: label to target (smooth exit, vertical entry)
                const cp2 = {
                    x: offsetTarget.x,
                    y: labelY
                };
                
                // Two quadratic beziers meeting at the label point
                return `M ${offsetSource.x} ${offsetSource.y} ` +
                       `Q ${cp1.x} ${cp1.y}, ${labelX} ${labelY} ` +
                       `Q ${cp2.x} ${cp2.y}, ${offsetTarget.x} ${offsetTarget.y}`;
            }
        }

        /**
         * Calculate optimal anchors to avoid lines being hidden by boxes
         * @param {Entity} sourceEntity 
         * @param {Entity} targetEntity 
         * @param {Object} labelCenter - Center of the relationship box
         * @param {string} connectionType - 'horizontal', 'vertical', or 'self'
         * @returns {Object} { entryAnchor, exitAnchor }
         */
        _calculateOptimalAnchors(sourceEntity, targetEntity, labelCenter, connectionType) {
            // Self-loop uses dynamic anchors based on label position
            if (connectionType === 'self') {
                const selfAnchors = this._calculateSelfLoopAnchors(sourceEntity, labelCenter);
                // Use pre-calculated relationship box anchors
                return {
                    entryAnchor: selfAnchors.entryAnchor,
                    exitAnchor: selfAnchors.exitAnchor
                };
            }
            
            // Get entity centers
            const sourceCenter = {
                x: sourceEntity.x + (sourceEntity.element?.offsetWidth || 220) / 2,
                y: sourceEntity.y + (sourceEntity.element?.offsetHeight || 100) / 2
            };
            const targetCenter = {
                x: targetEntity.x + (targetEntity.element?.offsetWidth || 220) / 2,
                y: targetEntity.y + (targetEntity.element?.offsetHeight || 100) / 2
            };
            
            // Calculate direction from source to label (for entry anchor on relationship box)
            const dxEntry = labelCenter.x - sourceCenter.x;
            const dyEntry = labelCenter.y - sourceCenter.y;
            
            // Calculate direction from label to target (for exit anchor on relationship box)
            const dxExit = targetCenter.x - labelCenter.x;
            const dyExit = targetCenter.y - labelCenter.y;
            
            // Determine entry anchor: which side of the relationship box faces the source?
            let entryAnchor;
            if (Math.abs(dxEntry) > Math.abs(dyEntry)) {
                // Source is primarily horizontal from label
                entryAnchor = dxEntry > 0 ? 'left' : 'right';
            } else {
                // Source is primarily vertical from label
                entryAnchor = dyEntry > 0 ? 'top' : 'bottom';
            }
            
            // Determine exit anchor: which side of the relationship box faces the target?
            let exitAnchor;
            if (Math.abs(dxExit) > Math.abs(dyExit)) {
                // Target is primarily horizontal from label
                exitAnchor = dxExit > 0 ? 'right' : 'left';
            } else {
                // Target is primarily vertical from label
                exitAnchor = dyExit > 0 ? 'bottom' : 'top';
            }
            
            // Avoid using the same anchor for entry and exit
            if (entryAnchor === exitAnchor) {
                // Choose alternative based on secondary direction
                if (entryAnchor === 'left' || entryAnchor === 'right') {
                    // Use vertical anchors as alternatives
                    exitAnchor = dyExit > 0 ? 'bottom' : 'top';
                } else {
                    // Use horizontal anchors as alternatives
                    exitAnchor = dxExit > 0 ? 'right' : 'left';
                }
            }
            
            return { entryAnchor, exitAnchor };
        }

        /**
         * Get the opposite anchor name
         * @param {string} anchor - 'left', 'right', 'top', or 'bottom'
         * @returns {string} - The opposite anchor
         */
        _getOppositeAnchor(anchor) {
            const opposites = {
                'left': 'right',
                'right': 'left',
                'top': 'bottom',
                'bottom': 'top'
            };
            return opposites[anchor] || anchor;
        }

        /**
         * Calculate optimal entity anchor based on relationship box position
         * @param {Entity} entity - The entity
         * @param {Object} labelCenter - Center of the relationship box
         * @returns {string} - Optimal anchor name ('left', 'right', 'top', 'bottom')
         */
        _calculateOptimalEntityAnchor(entity, labelCenter) {
            // Get entity center
            const entityCenter = {
                x: entity.x + (entity.element?.offsetWidth || 220) / 2,
                y: entity.y + (entity.element?.offsetHeight || 100) / 2
            };
            
            // Calculate direction from entity to relationship box
            const dx = labelCenter.x - entityCenter.x;
            const dy = labelCenter.y - entityCenter.y;
            
            // Choose anchor on the side facing the relationship box
            if (Math.abs(dx) > Math.abs(dy)) {
                // Relationship box is primarily horizontal from entity
                return dx > 0 ? 'right' : 'left';
            } else {
                // Relationship box is primarily vertical from entity
                return dy > 0 ? 'bottom' : 'top';
            }
        }

        /**
         * Calculate optimal anchors for a self-loop relationship
         * Returns entity anchors and relationship box anchors with correct pairing
         * @param {Entity} entity - The entity that connects to itself
         * @param {Object} labelCenter - Center point of the relationship label
         * @returns {Object} - { sourceAnchor, targetAnchor, entryAnchor, exitAnchor }
         */
        _calculateSelfLoopAnchors(entity, labelCenter) {
            // Get entity bounds
            const entityWidth = entity.element?.offsetWidth || 220;
            const entityHeight = entity.element?.offsetHeight || 100;
            const entityCenter = {
                x: entity.x + entityWidth / 2,
                y: entity.y + entityHeight / 2
            };
            
            // Calculate direction from entity center to label
            const dx = labelCenter.x - entityCenter.x;
            const dy = labelCenter.y - entityCenter.y;
            
            // Determine quadrant and return all four anchors:
            // - Entity anchors (sourceAnchor, targetAnchor): adjacent sides facing the label
            // - Relationship box anchors (entryAnchor, exitAnchor): adjacent sides facing entity
            // - Pairing: horizontal entity anchor → vertical rel anchor, horizontal rel anchor → vertical entity anchor
            
            if (dx >= 0 && dy < 0) {
                // Top-right quadrant
                // Entity: right (horizontal) + top (vertical)
                // Rel box: bottom (vertical) + left (horizontal) - facing entity
                // Flow: entity right → rel bottom, rel left → entity top
                return { sourceAnchor: 'right', targetAnchor: 'top', entryAnchor: 'bottom', exitAnchor: 'left' };
            } else if (dx < 0 && dy < 0) {
                // Top-left quadrant
                // Entity: left (horizontal) + top (vertical)
                // Rel box: bottom (vertical) + right (horizontal) - facing entity
                // Flow: entity left → rel bottom, rel right → entity top
                return { sourceAnchor: 'left', targetAnchor: 'top', entryAnchor: 'bottom', exitAnchor: 'right' };
            } else if (dx >= 0 && dy >= 0) {
                // Bottom-right quadrant
                // Entity: right (horizontal) + bottom (vertical)
                // Rel box: top (vertical) + left (horizontal) - facing entity
                // Flow: entity right → rel top, rel left → entity bottom
                return { sourceAnchor: 'right', targetAnchor: 'bottom', entryAnchor: 'top', exitAnchor: 'left' };
            } else {
                // Bottom-left quadrant (dx < 0 && dy >= 0)
                // Entity: left (horizontal) + bottom (vertical)
                // Rel box: top (vertical) + right (horizontal) - facing entity
                // Flow: entity left → rel top, rel right → entity bottom
                return { sourceAnchor: 'left', targetAnchor: 'bottom', entryAnchor: 'top', exitAnchor: 'right' };
            }
        }

        /**
         * Get anchor point position for an entity
         * @param {Entity} entity 
         * @param {string} anchorName - 'left', 'right', 'top', 'bottom'
         * @returns {Object} - {x, y} position
         */
        _getEntityAnchorPoint(entity, anchorName) {
            const rect = {
                x: entity.x,
                y: entity.y,
                width: entity.element?.offsetWidth || 220,
                height: entity.element?.offsetHeight || 100
            };
            const center = {
                x: rect.x + rect.width / 2,
                y: rect.y + rect.height / 2
            };
            
            switch (anchorName) {
                case 'top':
                    return { x: center.x, y: rect.y };
                case 'bottom':
                    return { x: center.x, y: rect.y + rect.height };
                case 'left':
                    return { x: rect.x, y: center.y };
                case 'right':
                    return { x: rect.x + rect.width, y: center.y };
                default:
                    return center;
            }
        }

        /**
         * Update paths to connect exactly to anchor positions
         * Rule: EVERY line links exactly 2 anchors (Entity <-> Relationship)
         * @param {Relationship} relationship 
         * @param {Object} labelCenter - Center point of the label
         */
        _updatePathsToAnchors(relationship, labelCenter) {
            const label = relationship.labelElement;
            if (!label) return;
            
            // Recalculate optimal anchors based on actual positions
            const sourceEntity = this.entities.get(relationship.sourceEntityId);
            const targetEntity = this.entities.get(relationship.targetEntityId);
            
            if (sourceEntity && targetEntity) {
                if (relationship._connectionType === 'self') {
                    // Self-loops use dynamic anchors based on label position
                    const selfAnchors = this._calculateSelfLoopAnchors(sourceEntity, labelCenter);
                    relationship._sourceEntityAnchor = selfAnchors.sourceAnchor;
                    relationship._targetEntityAnchor = selfAnchors.targetAnchor;
                    
                    // Use pre-calculated relationship box anchors
                    relationship._entryAnchor = selfAnchors.entryAnchor;
                    relationship._exitAnchor = selfAnchors.exitAnchor;
                    
                    // Update source and target points
                    relationship._sourcePoint = this._getEntityAnchorPoint(sourceEntity, selfAnchors.sourceAnchor);
                    relationship._targetPoint = this._getEntityAnchorPoint(sourceEntity, selfAnchors.targetAnchor);
                    
                    // Update anchor visualization - use relationship box anchors (entryAnchor, exitAnchor)
                    this._updateRelationshipAnchors(label, 'self', selfAnchors.entryAnchor, selfAnchors.exitAnchor);
                } else {
                    // Calculate optimal relationship box anchors
                    const { entryAnchor, exitAnchor } = this._calculateOptimalAnchors(
                        sourceEntity, targetEntity, labelCenter, relationship._connectionType
                    );
                    relationship._entryAnchor = entryAnchor;
                    relationship._exitAnchor = exitAnchor;
                    
                    // Calculate optimal entity anchors based on relationship box position
                    relationship._sourceEntityAnchor = this._calculateOptimalEntityAnchor(sourceEntity, labelCenter);
                    relationship._targetEntityAnchor = this._calculateOptimalEntityAnchor(targetEntity, labelCenter);
                    
                    // Update source and target points to use optimal entity anchors
                    relationship._sourcePoint = this._getEntityAnchorPoint(sourceEntity, relationship._sourceEntityAnchor);
                    relationship._targetPoint = this._getEntityAnchorPoint(targetEntity, relationship._targetEntityAnchor);
                    
                    // Update anchor visualization
                    this._updateRelationshipAnchors(label, relationship._connectionType, entryAnchor, exitAnchor);
                }
            }
            
            // Get actual label dimensions
            const labelWidth = label.offsetWidth || 120;
            const labelHeight = label.offsetHeight || 60;
            
            // Calculate exact anchor positions (center of anchor circles on label edge)
            const anchorPositions = {
                left: { x: labelCenter.x - labelWidth / 2, y: labelCenter.y },
                right: { x: labelCenter.x + labelWidth / 2, y: labelCenter.y },
                top: { x: labelCenter.x, y: labelCenter.y - labelHeight / 2 },
                bottom: { x: labelCenter.x, y: labelCenter.y + labelHeight / 2 }
            };
            
            const entryPoint = anchorPositions[relationship._entryAnchor];
            const exitPoint = anchorPositions[relationship._exitAnchor];
            
            // Build skip set so the line only avoids unrelated entities
            const skip = new Set([relationship.sourceEntityId, relationship.targetEntityId]);

            // Update path 1: Entity source anchor → Relationship entry anchor
            if (relationship.pathElement && relationship._sourcePoint) {
                relationship.pathElement.setAttribute('d', 
                    this._createSmoothCurve(relationship._sourcePoint, entryPoint, relationship._connectionType, 'entry', skip));
            }
            
            // Update path 2: Relationship exit anchor → Entity target anchor
            if (relationship.pathElement2 && relationship._targetPoint) {
                relationship.pathElement2.setAttribute('d', 
                    this._createSmoothCurve(exitPoint, relationship._targetPoint, relationship._connectionType, 'exit', skip));
            }
        }

        /**
         * Create a path between two points.
         * Uses straight lines for regular relationships, quarter-circle arcs for self-loops.
         * When skipEntityIds is provided, the path curves around any entity boxes that
         * the straight line would cross.
         * @param {Object} start - Start point {x, y}
         * @param {Object} end - End point {x, y}
         * @param {string} connectionType - 'horizontal', 'vertical', or 'self'
         * @param {string} segment - 'entry' or 'exit'
         * @param {Set|null} skipEntityIds - Entity IDs whose boxes should NOT be treated as obstacles
         */
        _createSmoothCurve(start, end, connectionType, segment, skipEntityIds = null) {
            if (connectionType === 'self') {
                const dx = end.x - start.x;
                const dy = end.y - start.y;
                const distance = Math.sqrt(dx * dx + dy * dy) || 50;
                const radius = distance;
                const sameSign = (dx >= 0 && dy >= 0) || (dx < 0 && dy < 0);
                const sweepFlag = sameSign ? 1 : 0;
                return `M ${start.x} ${start.y} A ${radius} ${radius} 0 0 ${sweepFlag} ${end.x} ${end.y}`;
            }

            if (skipEntityIds) {
                const obstacles = this._findAllObstacles(start, end, skipEntityIds);
                if (obstacles.length === 1) {
                    const cp = this._computeDetourControlPoint(start, end, obstacles[0]);
                    return `M ${start.x} ${start.y} Q ${cp.x} ${cp.y} ${end.x} ${end.y}`;
                }
                if (obstacles.length > 1) {
                    const merged = this._mergeObstacleBounds(obstacles);
                    const cp = this._computeDetourControlPoint(start, end, merged);
                    const candidatePath = { cp, start, end };
                    const stillBlocked = this._pathIntersectsAnyObstacle(
                        start, cp, end, skipEntityIds
                    );
                    if (!stillBlocked) {
                        return `M ${start.x} ${start.y} Q ${cp.x} ${cp.y} ${end.x} ${end.y}`;
                    }
                    return this._buildMultiSegmentDetour(start, end, obstacles, skipEntityIds);
                }
            }

            return `M ${start.x} ${start.y} L ${end.x} ${end.y}`;
        }

        /**
         * Find ALL entity bounding boxes that intersect the straight line
         * from start to end, sorted by distance from start.
         */
        _findAllObstacles(start, end, skipIds) {
            const pad = 6;
            const results = [];

            this.entities.forEach(entity => {
                if (skipIds.has(entity.id)) return;
                if (this.visibilityState.entities.get(entity.id) === false) return;
                const el = entity.element;
                if (!el) return;

                const rect = {
                    x: entity.x - pad,
                    y: entity.y - pad,
                    w: (el.offsetWidth || 220) + pad * 2,
                    h: (el.offsetHeight || 100) + pad * 2
                };

                if (this._segmentIntersectsRect(start, end, rect)) {
                    const cx = rect.x + rect.w / 2;
                    const cy = rect.y + rect.h / 2;
                    const dx = cx - start.x;
                    const dy = cy - start.y;
                    results.push({ rect, dist: dx * dx + dy * dy });
                }
            });
            results.sort((a, b) => a.dist - b.dist);
            return results.map(r => r.rect);
        }

        /**
         * Merge multiple obstacle rects into one bounding box.
         */
        _mergeObstacleBounds(rects) {
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            for (const r of rects) {
                minX = Math.min(minX, r.x);
                minY = Math.min(minY, r.y);
                maxX = Math.max(maxX, r.x + r.w);
                maxY = Math.max(maxY, r.y + r.h);
            }
            return { x: minX, y: minY, w: maxX - minX, h: maxY - minY };
        }

        /**
         * Check whether a quadratic Bezier (start → cp → end) intersects any
         * visible entity box (excluding skipIds).  Samples the curve at intervals.
         */
        _pathIntersectsAnyObstacle(start, cp, end, skipIds) {
            const pad = 6;
            const steps = 10;
            for (let i = 1; i < steps; i++) {
                const t = i / steps;
                const inv = 1 - t;
                const px = inv * inv * start.x + 2 * inv * t * cp.x + t * t * end.x;
                const py = inv * inv * start.y + 2 * inv * t * cp.y + t * t * end.y;

                let hit = false;
                this.entities.forEach(entity => {
                    if (hit) return;
                    if (skipIds.has(entity.id)) return;
                    if (this.visibilityState.entities.get(entity.id) === false) return;
                    const el = entity.element;
                    if (!el) return;
                    const rx = entity.x - pad;
                    const ry = entity.y - pad;
                    const rw = (el.offsetWidth || 220) + pad * 2;
                    const rh = (el.offsetHeight || 100) + pad * 2;
                    if (px >= rx && px <= rx + rw && py >= ry && py <= ry + rh) {
                        hit = true;
                    }
                });
                if (hit) return true;
            }
            return false;
        }

        /**
         * Build a multi-waypoint SVG path that threads between obstacles
         * using cubic Bezier curves.  Entity boxes are NEVER moved.
         */
        _buildMultiSegmentDetour(start, end, obstacles, skipIds) {
            const dx = end.x - start.x;
            const dy = end.y - start.y;
            const len = Math.sqrt(dx * dx + dy * dy) || 1;
            const perpX = -dy / len;
            const perpY = dx / len;

            const merged = this._mergeObstacleBounds(obstacles);
            const cx = merged.x + merged.w / 2;
            const cy = merged.y + merged.h / 2;
            const midX = (start.x + end.x) / 2;
            const midY = (start.y + end.y) / 2;
            const side = perpX * (cx - midX) + perpY * (cy - midY);
            const dir = side >= 0 ? -1 : 1;

            const margin = 40;
            const corners = [
                { x: merged.x - margin, y: merged.y - margin },
                { x: merged.x + merged.w + margin, y: merged.y - margin },
                { x: merged.x + merged.w + margin, y: merged.y + merged.h + margin },
                { x: merged.x - margin, y: merged.y + merged.h + margin }
            ];

            let bestPair = null;
            let bestCost = Infinity;
            for (let i = 0; i < corners.length; i++) {
                const c1 = corners[i];
                const c2 = corners[(i + 1) % corners.length];
                const dot = (c1.x - cx) * perpX * dir + (c1.y - cy) * perpY * dir;
                if (dot <= 0) continue;
                const cost = this._dist(start, c1) + this._dist(c1, c2) + this._dist(c2, end);
                if (cost < bestCost) {
                    bestCost = cost;
                    bestPair = [c1, c2];
                }
            }

            if (!bestPair) {
                const cp = this._computeDetourControlPoint(start, end, merged);
                return `M ${start.x} ${start.y} Q ${cp.x} ${cp.y} ${end.x} ${end.y}`;
            }

            const [wp1, wp2] = bestPair;
            return `M ${start.x} ${start.y} `
                 + `C ${start.x + (wp1.x - start.x) * 0.5} ${start.y + (wp1.y - start.y) * 0.5}, `
                 + `${wp1.x} ${wp1.y}, ${wp1.x} ${wp1.y} `
                 + `L ${wp2.x} ${wp2.y} `
                 + `C ${wp2.x} ${wp2.y}, `
                 + `${end.x + (wp2.x - end.x) * 0.5} ${end.y + (wp2.y - end.y) * 0.5}, `
                 + `${end.x} ${end.y}`;
        }

        _dist(a, b) {
            const dx = b.x - a.x;
            const dy = b.y - a.y;
            return Math.sqrt(dx * dx + dy * dy);
        }

        /**
         * @deprecated Use _findAllObstacles instead.
         */
        _findFirstObstacle(start, end, skipIds) {
            const all = this._findAllObstacles(start, end, skipIds);
            return all.length > 0 ? all[0] : null;
        }

        /**
         * Test whether a line segment from p1 to p2 intersects an axis-aligned rectangle.
         */
        _segmentIntersectsRect(p1, p2, rect) {
            const rx = rect.x, ry = rect.y, rw = rect.w, rh = rect.h;

            const inside = (px, py) => px >= rx && px <= rx + rw && py >= ry && py <= ry + rh;
            if (inside(p1.x, p1.y) || inside(p2.x, p2.y)) return true;

            const edges = [
                { a: { x: rx, y: ry },         b: { x: rx + rw, y: ry } },
                { a: { x: rx + rw, y: ry },     b: { x: rx + rw, y: ry + rh } },
                { a: { x: rx + rw, y: ry + rh }, b: { x: rx, y: ry + rh } },
                { a: { x: rx, y: ry + rh },     b: { x: rx, y: ry } }
            ];
            for (const e of edges) {
                if (this._segmentsIntersect(p1, p2, e.a, e.b)) return true;
            }
            return false;
        }

        _segmentsIntersect(a, b, c, d) {
            const cross = (o, p, q) => (p.x - o.x) * (q.y - o.y) - (p.y - o.y) * (q.x - o.x);
            const d1 = cross(c, d, a), d2 = cross(c, d, b);
            const d3 = cross(a, b, c), d4 = cross(a, b, d);
            if (((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) &&
                ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))) return true;
            return false;
        }

        /**
         * Compute a quadratic Bezier control point that detours around the obstacle rect.
         * The control point is placed at the closest corner of the obstacle, offset outward.
         */
        _computeDetourControlPoint(start, end, rect) {
            const cx = rect.x + rect.w / 2;
            const cy = rect.y + rect.h / 2;

            const dx = end.x - start.x;
            const dy = end.y - start.y;
            const len = Math.sqrt(dx * dx + dy * dy) || 1;
            const perpX = -dy / len;
            const perpY = dx / len;

            const midX = (start.x + end.x) / 2;
            const midY = (start.y + end.y) / 2;
            const side = perpX * (cx - midX) + perpY * (cy - midY);

            const corners = [
                { x: rect.x, y: rect.y },
                { x: rect.x + rect.w, y: rect.y },
                { x: rect.x + rect.w, y: rect.y + rect.h },
                { x: rect.x, y: rect.y + rect.h }
            ];

            const awayDir = side >= 0 ? -1 : 1;
            let best = null;
            let bestDot = -Infinity;
            for (const c of corners) {
                const dot = (c.x - cx) * perpX * awayDir + (c.y - cy) * perpY * awayDir;
                if (dot > bestDot) {
                    bestDot = dot;
                    best = c;
                }
            }

            const margin = 30;
            return {
                x: best.x + perpX * awayDir * margin,
                y: best.y + perpY * awayDir * margin
            };
        }

        /**
         * Update both relationship paths during drag
         * Rule 17: 2 lines connected through relationship box anchors
         * Rule (Line 36-37): Self-loops use right and top anchors with curved lines
         */
        _updateRelationshipPathsForDrag(relationship, points, labelCenter) {
            // Recalculate optimal anchors based on new label position
            const sourceEntity = this.entities.get(relationship.sourceEntityId);
            const targetEntity = this.entities.get(relationship.targetEntityId);
            
            let sourcePoint = points.source;
            let targetPoint = points.target;
            const isSelfLoop = points.connectionType === 'self';
            
            if (sourceEntity && targetEntity) {
                let entryAnchor, exitAnchor;
                
                if (isSelfLoop) {
                    // Self-loops use dynamic anchors based on label position
                    const selfAnchors = this._calculateSelfLoopAnchors(sourceEntity, labelCenter);
                    relationship._sourceEntityAnchor = selfAnchors.sourceAnchor;
                    relationship._targetEntityAnchor = selfAnchors.targetAnchor;
                    // Use pre-calculated relationship box anchors
                    entryAnchor = selfAnchors.entryAnchor;
                    exitAnchor = selfAnchors.exitAnchor;
                } else {
                    // Calculate optimal relationship box anchors
                    const optimalAnchors = this._calculateOptimalAnchors(
                        sourceEntity, targetEntity, labelCenter, points.connectionType
                    );
                    entryAnchor = optimalAnchors.entryAnchor;
                    exitAnchor = optimalAnchors.exitAnchor;
                    
                    // Calculate optimal entity anchors based on relationship box position
                    relationship._sourceEntityAnchor = this._calculateOptimalEntityAnchor(sourceEntity, labelCenter);
                    relationship._targetEntityAnchor = this._calculateOptimalEntityAnchor(targetEntity, labelCenter);
                }
                
                relationship._entryAnchor = entryAnchor;
                relationship._exitAnchor = exitAnchor;
                
                // Update source and target points to use entity anchors
                sourcePoint = this._getEntityAnchorPoint(sourceEntity, relationship._sourceEntityAnchor);
                targetPoint = this._getEntityAnchorPoint(targetEntity, relationship._targetEntityAnchor);
                
                relationship._sourcePoint = sourcePoint;
                relationship._targetPoint = targetPoint;
                
                // Update anchor visualization
                if (relationship.labelElement) {
                    this._updateRelationshipAnchors(relationship.labelElement, points.connectionType, entryAnchor, exitAnchor);
                }
            }
            
            const finalEntryAnchor = relationship._entryAnchor || 'left';
            const finalExitAnchor = relationship._exitAnchor || 'right';
            
            // Get actual label dimensions
            const label = relationship.labelElement;
            const labelWidth = label?.offsetWidth || 120;
            const labelHeight = label?.offsetHeight || 60;
            
            // Calculate anchor positions on the label box (at edge where anchor circles are centered)
            const anchorPositions = {
                left: { x: labelCenter.x - labelWidth / 2, y: labelCenter.y },
                right: { x: labelCenter.x + labelWidth / 2, y: labelCenter.y },
                top: { x: labelCenter.x, y: labelCenter.y - labelHeight / 2 },
                bottom: { x: labelCenter.x, y: labelCenter.y + labelHeight / 2 }
            };
            
            const entryPoint = anchorPositions[finalEntryAnchor];
            const exitPoint = anchorPositions[finalExitAnchor];
            
            // Update both paths to connect entity anchors ↔ relationship box anchors
            const skip = new Set([relationship.sourceEntityId, relationship.targetEntityId]);
            if (relationship.pathElement) {
                relationship.pathElement.setAttribute('d', 
                    this._createSmoothCurve(sourcePoint, entryPoint, points.connectionType, 'entry', skip));
            }
            if (relationship.pathElement2) {
                relationship.pathElement2.setAttribute('d', 
                    this._createSmoothCurve(exitPoint, targetPoint, points.connectionType, 'exit', skip));
            }
        }

        /**
         * Update only the SVG path lines without moving or recreating labels.
         * Used when entities are moved - labels stay in their user-defined positions.
         */
        _updateRelationshipPathsOnly() {
            this.relationships.forEach(relationship => {
                const label = relationship.labelElement;
                if (!label) return;
                
                const sourceEntity = this.entities.get(relationship.sourceEntityId);
                const targetEntity = this.entities.get(relationship.targetEntityId);
                if (!sourceEntity || !targetEntity) return;
                
                // Get current label center position (keep it fixed)
                const labelRect = label.getBoundingClientRect();
                const containerRect = this.container.getBoundingClientRect();
                const labelCenter = {
                    x: parseFloat(label.style.left) || 0,
                    y: parseFloat(label.style.top) || 0
                };
                
                // Determine connection type (self-loop or normal)
                const isSelfLoop = sourceEntity.id === targetEntity.id;
                const connectionType = isSelfLoop ? 'self' : relationship._connectionType || 'horizontal';
                
                // Calculate optimal entity anchors based on where the label is
                let sourceEntityAnchor, targetEntityAnchor, entryAnchor, exitAnchor;
                if (isSelfLoop) {
                    // Self-loops use dynamic anchors based on label position
                    const selfAnchors = this._calculateSelfLoopAnchors(sourceEntity, labelCenter);
                    sourceEntityAnchor = selfAnchors.sourceAnchor;
                    targetEntityAnchor = selfAnchors.targetAnchor;
                    // Use pre-calculated relationship box anchors
                    entryAnchor = selfAnchors.entryAnchor;
                    exitAnchor = selfAnchors.exitAnchor;
                } else {
                    sourceEntityAnchor = this._calculateOptimalEntityAnchor(sourceEntity, labelCenter);
                    targetEntityAnchor = this._calculateOptimalEntityAnchor(targetEntity, labelCenter);
                    // Calculate optimal relationship box anchors
                    const optimalAnchors = this._calculateOptimalAnchors(sourceEntity, targetEntity, labelCenter, connectionType);
                    entryAnchor = optimalAnchors.entryAnchor;
                    exitAnchor = optimalAnchors.exitAnchor;
                }
                
                // Get entity anchor points
                const sourcePoint = this._getEntityAnchorPoint(sourceEntity, sourceEntityAnchor);
                const targetPoint = this._getEntityAnchorPoint(targetEntity, targetEntityAnchor);
                
                // Store updated values
                relationship._sourceEntityAnchor = sourceEntityAnchor;
                relationship._targetEntityAnchor = targetEntityAnchor;
                relationship._sourcePoint = sourcePoint;
                relationship._targetPoint = targetPoint;
                relationship._entryAnchor = entryAnchor;
                relationship._exitAnchor = exitAnchor;
                relationship._connectionType = connectionType;
                
                // Get label dimensions
                const labelWidth = label.offsetWidth || 120;
                const labelHeight = label.offsetHeight || 60;
                
                // Calculate anchor positions on the label box
                const anchorPositions = {
                    left: { x: labelCenter.x - labelWidth / 2, y: labelCenter.y },
                    right: { x: labelCenter.x + labelWidth / 2, y: labelCenter.y },
                    top: { x: labelCenter.x, y: labelCenter.y - labelHeight / 2 },
                    bottom: { x: labelCenter.x, y: labelCenter.y + labelHeight / 2 }
                };
                
                const entryPoint = anchorPositions[entryAnchor];
                const exitPoint = anchorPositions[exitAnchor];
                
                // Update SVG paths (route around unrelated entities)
                const skip = new Set([relationship.sourceEntityId, relationship.targetEntityId]);
                if (relationship.pathElement) {
                    relationship.pathElement.setAttribute('d', 
                        this._createSmoothCurve(sourcePoint, entryPoint, connectionType, 'entry', skip));
                }
                if (relationship.pathElement2) {
                    relationship.pathElement2.setAttribute('d', 
                        this._createSmoothCurve(exitPoint, targetPoint, connectionType, 'exit', skip));
                }
                
                // Update anchor visualization
                this._updateRelationshipAnchors(label, connectionType, entryAnchor, exitAnchor);
            });
            
            // Also update inheritance paths
            this._updateInheritancePaths();
        }

        /**
         * Fully re-render all relationships (paths and labels).
         * Used when relationships are created, deleted, or need complete refresh.
         */
        _updateRelationshipPaths() {
            this.relationships.forEach(relationship => {
                this._renderRelationship(relationship);
            });
            // Also update inheritance paths
            this._updateInheritancePaths();
        }

        _selectRelationship(relationship) {
            this._clearSelection();
            this.selectedRelationship = relationship;
            relationship.pathElement?.classList.add('ovz-selected');
            relationship.pathElement2?.classList.add('ovz-selected');
            relationship.labelElement?.classList.add('ovz-selected');
            
            // Rule (Line 29-30): Show endpoint handles for dragging to different anchors
            this._showEndpointHandles(relationship);

            if (this.options.onSelectionChange) {
                this.options.onSelectionChange({ type: 'relationship', item: relationship });
            }
        }
        
        /**
         * Show draggable endpoint handles when a relationship is selected
         * Rule (Line 29-30): User can select the line and drag'n move it to another anchor
         */
        _showEndpointHandles(relationship) {
            // Remove any existing handles
            this._hideEndpointHandles();
            
            const sourceEntity = this.entities.get(relationship.sourceEntityId);
            const targetEntity = this.entities.get(relationship.targetEntityId);
            if (!sourceEntity || !targetEntity) return;
            
            const points = this._calculateConnectionPoints(sourceEntity, targetEntity, relationship);
            
            // Create source endpoint handle
            const sourceHandle = this._createEndpointHandle(relationship, points.source, 'source', sourceEntity);
            const targetHandle = this._createEndpointHandle(relationship, points.target, 'target', targetEntity);
            
            this.svgLayer.appendChild(sourceHandle);
            this.svgLayer.appendChild(targetHandle);
            
            // Store handles for cleanup
            this._endpointHandles = [sourceHandle, targetHandle];
        }
        
        /**
         * Create a draggable endpoint handle
         */
        _createEndpointHandle(relationship, point, type, entity) {
            const handle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            handle.classList.add('ovz-endpoint-handle');
            handle.setAttribute('cx', point.x);
            handle.setAttribute('cy', point.y);
            handle.setAttribute('r', 8);
            handle.dataset.type = type;
            
            let isDragging = false;
            
            handle.addEventListener('mousedown', (e) => {
                e.stopPropagation();
                isDragging = true;
                handle.classList.add('ovz-dragging');
                
                // Show all anchors on the entity
                this._highlightEntityAnchors(entity, true);
                
                const onMouseMove = (moveEvent) => {
                    if (!isDragging) return;
                    
                    const canvas = this.container;
                    const rect = canvas.getBoundingClientRect();
                    const mouseX = (moveEvent.clientX - rect.left - this.panX) / this.scale;
                    const mouseY = (moveEvent.clientY - rect.top - this.panY) / this.scale;
                    
                    // Update handle position
                    handle.setAttribute('cx', mouseX);
                    handle.setAttribute('cy', mouseY);
                    
                    // Find and highlight closest anchor
                    const closest = this._findClosestAnchor(entity, mouseX, mouseY);
                    if (closest) {
                        this._highlightSingleAnchor(entity, closest);
                    }
                };
                
                const onMouseUp = (upEvent) => {
                    if (isDragging) {
                        isDragging = false;
                        handle.classList.remove('ovz-dragging');
                        
                        const canvas = this.container;
                        const rect = canvas.getBoundingClientRect();
                        const mouseX = (upEvent.clientX - rect.left - this.panX) / this.scale;
                        const mouseY = (upEvent.clientY - rect.top - this.panY) / this.scale;
                        
                        // Find closest anchor and update relationship
                        const closest = this._findClosestAnchor(entity, mouseX, mouseY);
                        if (closest) {
                            if (type === 'source') {
                                relationship.sourceAnchor = closest;
                            } else {
                                relationship.targetAnchor = closest;
                            }
                            
                            // Re-render relationships
                            this._updateRelationshipPaths();
                            
                            // Show handles again for new positions
                            this._showEndpointHandles(relationship);
                            
                            if (this.options.onRelationshipUpdate) {
                                this.options.onRelationshipUpdate(relationship);
                            }
                        }
                        
                        // Remove anchor highlights
                        this._highlightEntityAnchors(entity, false);
                    }
                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);
                };
                
                document.addEventListener('mousemove', onMouseMove);
                document.addEventListener('mouseup', onMouseUp);
            });
            
            return handle;
        }
        
        /**
         * Hide endpoint handles
         */
        _hideEndpointHandles() {
            if (this._endpointHandles) {
                this._endpointHandles.forEach(h => h.remove());
                this._endpointHandles = null;
            }
        }
        
        /**
         * Highlight all anchors on an entity
         */
        _highlightEntityAnchors(entity, show) {
            if (!entity?.element) return;
            entity.element.querySelectorAll('.ovz-connector').forEach(c => {
                if (show) {
                    c.classList.add('ovz-anchor-available');
                } else {
                    c.classList.remove('ovz-anchor-available', 'ovz-anchor-closest');
                }
            });
        }
        
        /**
         * Highlight only the closest anchor
         */
        _highlightSingleAnchor(entity, anchorName) {
            if (!entity?.element) return;
            entity.element.querySelectorAll('.ovz-connector').forEach(c => {
                c.classList.remove('ovz-anchor-closest');
            });
            const connector = entity.element.querySelector(`.ovz-connector[data-connector="${anchorName}"]`);
            if (connector) {
                connector.classList.add('ovz-anchor-closest');
            }
        }
        
        /**
         * Find the closest anchor to a mouse position
         */
        _findClosestAnchor(entity, mouseX, mouseY) {
            if (!entity?.element) return null;
            
            const rect = {
                x: entity.x,
                y: entity.y,
                width: entity.element.offsetWidth || 220,
                height: entity.element.offsetHeight || 100
            };
            
            const anchors = {
                top: { x: rect.x + rect.width / 2, y: rect.y },
                bottom: { x: rect.x + rect.width / 2, y: rect.y + rect.height },
                left: { x: rect.x, y: rect.y + rect.height / 2 },
                right: { x: rect.x + rect.width, y: rect.y + rect.height / 2 }
            };
            
            let closest = null;
            let minDist = Infinity;
            const snapDistance = 60;
            
            for (const [name, pos] of Object.entries(anchors)) {
                const dist = Math.sqrt(Math.pow(pos.x - mouseX, 2) + Math.pow(pos.y - mouseY, 2));
                if (dist < minDist && dist < snapDistance) {
                    minDist = dist;
                    closest = name;
                }
            }
            
            return closest;
        }

        // ==========================================
        // Connection (Drag to Connect)
        // ==========================================
        _startConnection(entity, connectorSide, e) {
            this.isConnecting = true;
            this.connectionSource = { entity, side: connectorSide };

            // Get connector position
            const entityRect = entity.element.getBoundingClientRect();
            const canvasRect = this.canvasInner.getBoundingClientRect();
            
            let startX, startY;
            const entityWidth = entity.element.offsetWidth;
            const entityHeight = entity.element.offsetHeight;

            switch (connectorSide) {
                case 'left':
                    startX = entity.x;
                    startY = entity.y + entityHeight / 2;
                    break;
                case 'right':
                    startX = entity.x + entityWidth;
                    startY = entity.y + entityHeight / 2;
                    break;
                case 'top':
                    startX = entity.x + entityWidth / 2;
                    startY = entity.y;
                    break;
                case 'bottom':
                    startX = entity.x + entityWidth / 2;
                    startY = entity.y + entityHeight;
                    break;
            }

            // Create temporary line
            this.tempLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
            this.tempLine.classList.add('ovz-temp-line');
            
            // Apply different styling for inheritance mode
            if (this.isInheritanceMode) {
                this.tempLine.classList.add('ovz-temp-line-inheritance');
            }
            
            this.tempLine.setAttribute('x1', startX);
            this.tempLine.setAttribute('y1', startY);
            this.tempLine.setAttribute('x2', startX);
            this.tempLine.setAttribute('y2', startY);
            this.svgLayer.appendChild(this.tempLine);
        }

        _endConnection(targetEntity) {
            if (this.tempLine) {
                this.tempLine.remove();
                this.tempLine = null;
            }

            if (targetEntity && this.connectionSource) {
                const sourceEntityId = this.connectionSource.entity.id;
                const targetEntityId = targetEntity.id;
                
                if (this.isInheritanceMode) {
                    // Create inheritance (no self-loops allowed)
                    if (sourceEntityId !== targetEntityId) {
                        this.addInheritance({
                            sourceEntityId: sourceEntityId,
                            targetEntityId: targetEntityId,
                            direction: 'forward'
                        });
                    }
                } else {
                    // Create relationship (self-loops allowed)
                    this.addRelationship({
                        sourceEntityId: sourceEntityId,
                        targetEntityId: targetEntityId,
                        name: 'relates_to'
                    });
                }
            }

            this.isConnecting = false;
            this.connectionSource = null;

            // Clear active connector states
            this.container.querySelectorAll('.ovz-connector.ovz-active').forEach(c => {
                c.classList.remove('ovz-active');
            });
        }

        // ==========================================
        // Selection Management
        // ==========================================
        _clearSelection() {
            if (this.selectedEntity) {
                this.selectedEntity.element?.classList.remove('ovz-selected');
                this.selectedEntity = null;
            }
            if (this.selectedRelationship) {
                this.selectedRelationship.pathElement?.classList.remove('ovz-selected');
                this.selectedRelationship.pathElement2?.classList.remove('ovz-selected');
                this.selectedRelationship.labelElement?.classList.remove('ovz-selected');
                this.selectedRelationship = null;
            }
            if (this.selectedInheritance) {
                this.selectedInheritance.pathElement?.classList.remove('ovz-selected');
                this.selectedInheritance = null;
            }
            
            // Hide endpoint handles when clearing selection
            this._hideEndpointHandles();

            if (this.options.onSelectionChange) {
                this.options.onSelectionChange(null);
            }
        }

        // ==========================================
        // Context Menu
        // ==========================================
        _showContextMenu(e) {
            this._hideContextMenu();

            // In view-only mode, don't show context menu for editing actions
            const isViewOnly = this.options.viewOnly === true;

            const menu = document.createElement('div');
            menu.className = 'ovz-context-menu';
            menu.style.left = e.clientX + 'px';
            menu.style.top = e.clientY + 'px';

            let menuItems = '';

            if (this.selectedEntity) {
                if (isViewOnly) {
                    // View-only: no editing options
                    return; // Don't show context menu
                }
                menuItems = `
                    <div class="ovz-context-item" data-action="add-property">
                        <span class="ovz-context-icon">+</span>
                        <span>Add Property</span>
                    </div>
                    <div class="ovz-context-item" data-action="duplicate">
                        <span class="ovz-context-icon">⧉</span>
                        <span>Duplicate</span>
                    </div>
                    <div class="ovz-context-divider"></div>
                    <div class="ovz-context-item ovz-danger" data-action="delete">
                        <span class="ovz-context-icon">×</span>
                        <span>Delete Entity</span>
                    </div>
                `;
            } else if (this.selectedRelationship) {
                if (isViewOnly) {
                    // View-only: no editing options
                    return; // Don't show context menu
                }
                menuItems = `
                    <div class="ovz-context-item ovz-danger" data-action="delete-rel">
                        <span class="ovz-context-icon">×</span>
                        <span>Delete Relationship</span>
                    </div>
                `;
            } else {
                if (isViewOnly) {
                    // View-only: no add entity option
                    return; // Don't show context menu
                }
                menuItems = `
                    <div class="ovz-context-item" data-action="add-entity-here">
                        <span class="ovz-context-icon">+</span>
                        <span>Add Entity Here</span>
                    </div>
                `;
            }

            menu.innerHTML = menuItems;
            document.body.appendChild(menu);
            this.contextMenu = menu;

            // Bind menu actions
            menu.addEventListener('click', (e) => {
                const item = e.target.closest('.ovz-context-item');
                if (!item) return;

                const action = item.dataset.action;
                const pos = Utils.getMousePosition({ clientX: parseInt(menu.style.left), clientY: parseInt(menu.style.top) }, this.canvasInner);

                switch (action) {
                    case 'add-entity-here':
                        this.addEntity({ x: pos.x, y: pos.y });
                        break;
                    case 'add-property':
                        if (this.selectedEntity) {
                            this.selectedEntity.addProperty({ name: 'new_property' });
                            this._renderEntity(this.selectedEntity);
                            // Re-render any child entities that inherit from this entity
                            this._rerenderChildEntities(this.selectedEntity.id);
                            if (this.options.onEntityUpdate) {
                                this.options.onEntityUpdate(this.selectedEntity);
                            }
                        }
                        break;
                    case 'duplicate':
                        if (this.selectedEntity) {
                            const ent = this.selectedEntity;
                            this.addEntity({
                                name: ent.name + '_copy',
                                x: ent.x + 30,
                                y: ent.y + 30,
                                properties: JSON.parse(JSON.stringify(ent.properties))
                            });
                        }
                        break;
                    case 'delete':
                        if (this.selectedEntity) {
                            this._showDeleteOrHideDialog('entity', this.selectedEntity.id, this.selectedEntity.name);
                        }
                        break;
                    case 'delete-rel':
                        if (this.selectedRelationship) {
                            this._showDeleteOrHideDialog('relationship', this.selectedRelationship.id, this.selectedRelationship.name);
                        }
                        break;
                }

                this._hideContextMenu();
            });
        }

        _hideContextMenu() {
            if (this.contextMenu) {
                this.contextMenu.remove();
                this.contextMenu = null;
            }
        }

        // ==========================================
        // Zoom & Pan
        // ==========================================
        _zoom(factor) {
            // Simple zoom implementation - can be enhanced
            const currentScale = this.canvasInner.style.transform 
                ? parseFloat(this.canvasInner.style.transform.replace('scale(', '').replace(')', '')) 
                : 1;
            const newScale = Utils.clamp(currentScale * factor, 0.25, 2);
            this.canvasInner.style.transform = `scale(${newScale})`;
        }

        /**
         * Show or hide loading overlay
         * Uses the global showOntologyDesignerLoading or showMappingDesignerLoading functions if available
         */
        _showLoading(show) {
            // Try ontology designer loading function first
            if (typeof showOntologyDesignerLoading === 'function') {
                showOntologyDesignerLoading(show);
                return;
            }
            // Try mapping designer loading function
            if (typeof showMappingDesignerLoading === 'function') {
                showMappingDesignerLoading(show);
                return;
            }
            // Fallback: find loading overlay by common class
            const loadingEl = this.container.closest('.card-body')?.querySelector('.designer-loading-overlay');
            if (loadingEl) {
                loadingEl.style.display = show ? 'flex' : 'none';
            }
        }

        /**
         * Center all entities in the visible canvas area
         * @param {Object} options
         * @param {number} options.padding - Padding from canvas edges (default: 50)
         * @param {boolean} options.animate - Animate the movement (default: true)
         */
        centerDiagram(options = {}) {
            const entityList = Array.from(this.entities.values());
            if (entityList.length === 0) return;

            const {
                padding = 50,
                animate = true
            } = options;

            // Get canvas dimensions
            const canvasWidth = this.container.offsetWidth || 800;
            const canvasHeight = this.container.offsetHeight || 600;

            // Calculate the bounding box of all entities
            let minX = Infinity, minY = Infinity;
            let maxX = -Infinity, maxY = -Infinity;

            entityList.forEach(entity => {
                const bounds = this._getEntityBounds(entity);
                minX = Math.min(minX, bounds.x);
                minY = Math.min(minY, bounds.y);
                maxX = Math.max(maxX, bounds.right);
                maxY = Math.max(maxY, bounds.bottom);
            });

            // Calculate diagram dimensions
            const diagramWidth = maxX - minX;
            const diagramHeight = maxY - minY;

            // Calculate the offset needed to center
            const targetCenterX = (canvasWidth - diagramWidth) / 2;
            const targetCenterY = (canvasHeight - diagramHeight) / 2;

            // Ensure minimum padding from edges
            const offsetX = Math.max(padding, targetCenterX) - minX;
            const offsetY = Math.max(padding, targetCenterY) - minY;

            // Apply offset to all entities
            entityList.forEach(entity => {
                entity.x += offsetX;
                entity.y += offsetY;
            });

            // Update DOM
            this._applyEntityPositions(animate, 400);

            // Update relationship paths
            if (animate) {
                setTimeout(() => this._updateRelationshipPaths(), 400);
            } else {
                this._updateRelationshipPaths();
            }
        }

        // ==========================================
        // Auto-Layout & Overlap Prevention
        // ==========================================
        
        /**
         * Get the bounding box of an entity
         * @param {Entity} entity 
         * @returns {Object} { id, type, x, y, width, height, right, bottom, centerX, centerY }
         */
        _getEntityBounds(entity) {
            const width = entity.element?.offsetWidth || 220;
            const height = entity.element?.offsetHeight || 100;
            return {
                id: entity.id,
                type: 'entity',
                x: entity.x,
                y: entity.y,
                width: width,
                height: height,
                right: entity.x + width,
                bottom: entity.y + height,
                centerX: entity.x + width / 2,
                centerY: entity.y + height / 2
            };
        }

        /**
         * Get the bounding box of a relationship label
         * @param {Relationship} relationship 
         * @returns {Object|null} { id, type, x, y, width, height, right, bottom, centerX, centerY }
         */
        _getRelationshipLabelBounds(relationship) {
            if (!relationship.labelElement) return null;
            
            const rect = relationship.labelElement.getBoundingClientRect();
            const canvasRect = this.canvasInner.getBoundingClientRect();
            
            // Get position from style (transform translate(-50%, -50%) applied)
            const left = parseFloat(relationship.labelElement.style.left) || 0;
            const top = parseFloat(relationship.labelElement.style.top) || 0;
            const width = relationship.labelElement.offsetWidth || 100;
            const height = relationship.labelElement.offsetHeight || 30;
            
            // Adjust for transform: translate(-50%, -50%)
            const x = left - width / 2;
            const y = top - height / 2;
            
            return {
                id: relationship.id,
                type: 'relationship',
                x: x,
                y: y,
                width: width,
                height: height,
                right: x + width,
                bottom: y + height,
                centerX: left,
                centerY: top
            };
        }

        /**
         * Get all objects (entities + relationship labels) with their bounds
         * @returns {Array} Array of bounds objects
         */
        _getAllObjectBounds() {
            const objects = [];
            
            // Add entities
            this.entities.forEach(entity => {
                objects.push(this._getEntityBounds(entity));
            });
            
            // Add relationship labels
            this.relationships.forEach(rel => {
                const bounds = this._getRelationshipLabelBounds(rel);
                if (bounds) {
                    objects.push(bounds);
                }
            });
            
            return objects;
        }

        /**
         * Check if two bounding boxes overlap
         * @param {Object} boundsA 
         * @param {Object} boundsB 
         * @param {number} padding - Extra padding between objects
         * @returns {boolean}
         */
        _boundsOverlap(boundsA, boundsB, padding = 20) {
            return !(
                boundsA.right + padding < boundsB.x ||
                boundsB.right + padding < boundsA.x ||
                boundsA.bottom + padding < boundsB.y ||
                boundsB.bottom + padding < boundsA.y
            );
        }

        /**
         * Check if two entities overlap
         * @param {Entity} entityA 
         * @param {Entity} entityB 
         * @param {number} padding - Extra padding between entities
         * @returns {boolean}
         */
        _entitiesOverlap(entityA, entityB, padding = 20) {
            const a = this._getEntityBounds(entityA);
            const b = this._getEntityBounds(entityB);
            return this._boundsOverlap(a, b, padding);
        }

        /**
         * Get all entities that overlap with the given entity
         * @param {Entity} entity 
         * @param {number} padding 
         * @returns {Entity[]}
         */
        getOverlappingEntities(entity, padding = 20) {
            const overlapping = [];
            this.entities.forEach((other) => {
                if (other.id !== entity.id && this._entitiesOverlap(entity, other, padding)) {
                    overlapping.push(other);
                }
            });
            return overlapping;
        }

        /**
         * Check if any objects (entities or relationship labels) overlap in the diagram
         * @param {number} padding 
         * @returns {boolean}
         */
        hasOverlaps(padding = 20) {
            const objects = this._getAllObjectBounds();
            for (let i = 0; i < objects.length; i++) {
                for (let j = i + 1; j < objects.length; j++) {
                    if (this._boundsOverlap(objects[i], objects[j], padding)) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Resolve overlaps by pushing entities apart
         * Considers both entities AND relationship labels to ensure no overlaps
         * @param {Object} options
         * @param {number} options.padding - Minimum space between all objects (default: 40)
         * @param {number} options.iterations - Max iterations to resolve (default: 100)
         * @param {number} options.stepSize - How much to move per iteration (default: 15)
         * @param {boolean} options.animate - Animate the movement (default: true)
         */
        resolveOverlaps(options = {}) {
            const {
                padding = 40,
                iterations = 100,
                stepSize = 15,
                animate = true
            } = options;

            const entityList = Array.from(this.entities.values());
            if (entityList.length < 1) return;

            // Multiple passes - first resolve entity-entity, then entity-relationship
            for (let iter = 0; iter < iterations; iter++) {
                let hasOverlap = false;
                
                // Update relationship paths to get current label positions
                this._updateRelationshipPaths();
                
                // Get all current bounds
                const entityBounds = entityList.map(e => ({ entity: e, bounds: this._getEntityBounds(e) }));
                const relBounds = [];
                this.relationships.forEach(rel => {
                    const bounds = this._getRelationshipLabelBounds(rel);
                    if (bounds) {
                        relBounds.push({ relationship: rel, bounds: bounds });
                    }
                });

                // Check entity-entity overlaps
                for (let i = 0; i < entityBounds.length; i++) {
                    for (let j = i + 1; j < entityBounds.length; j++) {
                        const a = entityBounds[i];
                        const b = entityBounds[j];
                        
                        if (this._boundsOverlap(a.bounds, b.bounds, padding)) {
                            hasOverlap = true;
                            this._pushApart(a.entity, b.entity, a.bounds, b.bounds, stepSize);
                        }
                    }
                }

                // Check entity-relationship label overlaps
                for (const eb of entityBounds) {
                    for (const rb of relBounds) {
                        if (this._boundsOverlap(eb.bounds, rb.bounds, padding)) {
                            hasOverlap = true;
                            // Move the entity away from the relationship label
                            this._pushEntityFromLabel(eb.entity, eb.bounds, rb.bounds, stepSize);
                        }
                    }
                }

                // Check relationship label - relationship label overlaps
                for (let i = 0; i < relBounds.length; i++) {
                    for (let j = i + 1; j < relBounds.length; j++) {
                        const a = relBounds[i];
                        const b = relBounds[j];
                        
                        if (this._boundsOverlap(a.bounds, b.bounds, padding)) {
                            hasOverlap = true;
                            // Move connected entities to separate the labels
                            this._separateRelationshipLabels(a.relationship, b.relationship, a.bounds, b.bounds, stepSize);
                        }
                    }
                }
                
                if (!hasOverlap) break;
            }
            
            // Update all entity positions in DOM
            this._applyEntityPositions(animate, 300);
            
            // Update relationship paths
            if (animate) {
                setTimeout(() => this._updateRelationshipPaths(), 300);
            } else {
                this._updateRelationshipPaths();
            }
        }

        /**
         * Push two entities apart
         */
        _pushApart(entityA, entityB, boundsA, boundsB, stepSize) {
            let dx = boundsB.centerX - boundsA.centerX;
            let dy = boundsB.centerY - boundsA.centerY;
            
            if (dx === 0 && dy === 0) {
                dx = Math.random() - 0.5;
                dy = Math.random() - 0.5;
            }
            
            const distance = Math.sqrt(dx * dx + dy * dy) || 1;
            dx /= distance;
            dy /= distance;
            
            entityA.x = Math.max(0, entityA.x - dx * stepSize / 2);
            entityA.y = Math.max(0, entityA.y - dy * stepSize / 2);
            entityB.x = Math.max(0, entityB.x + dx * stepSize / 2);
            entityB.y = Math.max(0, entityB.y + dy * stepSize / 2);
        }

        /**
         * Push entity away from a relationship label by moving the label (not the entity)
         * This preserves entity positions while relocating the relationship label
         */
        _pushEntityFromLabel(entity, entityBounds, labelBounds, stepSize, relationship) {
            // Find the relationship that owns this label and adjust its label offset
            let targetRel = null;
            this.relationships.forEach(rel => {
                if (rel.id === labelBounds.id) {
                    targetRel = rel;
                }
            });
            
            if (targetRel) {
                // Move the label away from the entity by adjusting labelOffset
                let dx = labelBounds.centerX - entityBounds.centerX;
                let dy = labelBounds.centerY - entityBounds.centerY;
                
                if (dx === 0 && dy === 0) {
                    dx = Math.random() - 0.5;
                    dy = Math.random() - 0.5;
                }
                
                const distance = Math.sqrt(dx * dx + dy * dy) || 1;
                dx /= distance;
                dy /= distance;
                
                // Adjust the relationship label offset
                targetRel.labelOffsetX = (targetRel.labelOffsetX || 0) + dx * stepSize;
                targetRel.labelOffsetY = (targetRel.labelOffsetY || 0) + dy * stepSize;
            } else {
                // Fallback: move entity away from label
                let dx = entityBounds.centerX - labelBounds.centerX;
                let dy = entityBounds.centerY - labelBounds.centerY;
                
                if (dx === 0 && dy === 0) {
                    dx = Math.random() - 0.5;
                    dy = Math.random() - 0.5;
                }
                
                const distance = Math.sqrt(dx * dx + dy * dy) || 1;
                dx /= distance;
                dy /= distance;
                
                // Move entity away from label
                entity.x = Math.max(0, entity.x + dx * stepSize);
                entity.y = Math.max(0, entity.y + dy * stepSize);
            }
        }

        /**
         * Separate two relationship labels by adjusting their label offsets
         * This preserves entity positions while relocating the labels
         */
        _separateRelationshipLabels(relA, relB, boundsA, boundsB, stepSize) {
            // Calculate direction to push apart
            let dx = boundsB.centerX - boundsA.centerX;
            let dy = boundsB.centerY - boundsA.centerY;
            
            if (dx === 0 && dy === 0) {
                dx = Math.random() - 0.5;
                dy = Math.random() - 0.5;
            }
            
            const distance = Math.sqrt(dx * dx + dy * dy) || 1;
            dx /= distance;
            dy /= distance;
            
            // Move labels by adjusting their offsets in opposite directions
            const moveAmount = stepSize / 2;
            
            // Adjust label offset for relA (move in -dx, -dy direction)
            relA.labelOffsetX = (relA.labelOffsetX || 0) - dx * moveAmount;
            relA.labelOffsetY = (relA.labelOffsetY || 0) - dy * moveAmount;
            
            // Adjust label offset for relB (move in +dx, +dy direction)
            relB.labelOffsetX = (relB.labelOffsetX || 0) + dx * moveAmount;
            relB.labelOffsetY = (relB.labelOffsetY || 0) + dy * moveAmount;
        }

        /**
         * Apply entity positions to DOM with optional animation
         */
        _applyEntityPositions(animate, duration) {
            this.entities.forEach(entity => {
                if (entity.element) {
                    if (animate) {
                        entity.element.style.transition = `left ${duration}ms ease, top ${duration}ms ease`;
                    }
                    entity.element.style.left = entity.x + 'px';
                    entity.element.style.top = entity.y + 'px';
                    
                    if (animate) {
                        setTimeout(() => {
                            entity.element.style.transition = '';
                        }, duration);
                    }
                }
            });
        }

        /**
         * Auto-layout entities to avoid all box and line overlaps, then center the diagram
         * Uses a hierarchical layout that places connected entities optimally
         * @param {Object} options
         * @param {number} options.horizontalGap - Gap between columns (default: 200)
         * @param {number} options.verticalGap - Gap between rows (default: 150)
         * @param {boolean} options.animate - Animate the movement (default: true)
         */
        autoLayoutGrid(options = {}) {
            this.autoLayoutForce(options);
        }

        /**
         * Auto-layout entities in a force-directed manner
         * Considers relationship labels as repelling objects
         * @param {Object} options
         * @param {number} options.iterations - Number of simulation steps (default: 150)
         * @param {number} options.repulsion - Repulsion force strength (default: 8000)
         * @param {number} options.attraction - Attraction force for connected entities (default: 0.05)
         * @param {number} options.damping - Velocity damping (default: 0.85)
         * @param {number} options.minDistance - Minimum distance between entities (default: 250)
         * @param {boolean} options.animate - Animate the final result (default: true)
         */
        autoLayoutForce(options = {}) {
            const entityList = Array.from(this.entities.values());
            if (entityList.length === 0) return;

            const {
                iterations = 150,
                repulsion = 8000,
                attraction = 0.05,
                damping = 0.85,
                minDistance = 250,  // Minimum distance to leave room for labels
                animate = true
            } = options;

            // Initialize velocities
            const velocities = new Map();
            entityList.forEach(entity => {
                velocities.set(entity.id, { vx: 0, vy: 0 });
            });

            // Build adjacency for relationships
            const connections = new Map();
            this.relationships.forEach(rel => {
                if (!connections.has(rel.sourceEntityId)) {
                    connections.set(rel.sourceEntityId, new Set());
                }
                if (!connections.has(rel.targetEntityId)) {
                    connections.set(rel.targetEntityId, new Set());
                }
                connections.get(rel.sourceEntityId).add(rel.targetEntityId);
                connections.get(rel.targetEntityId).add(rel.sourceEntityId);
            });

            // Run simulation
            for (let iter = 0; iter < iterations; iter++) {
                entityList.forEach(entity => {
                    const bounds = this._getEntityBounds(entity);
                    
                    let fx = 0, fy = 0;
                    
                    // Repulsion from all other entities
                    entityList.forEach(other => {
                        if (other.id === entity.id) return;
                        
                        const otherBounds = this._getEntityBounds(other);
                        
                        let dx = bounds.centerX - otherBounds.centerX;
                        let dy = bounds.centerY - otherBounds.centerY;
                        let dist = Math.sqrt(dx * dx + dy * dy);
                        
                        if (dist < 1) dist = 1;
                        
                        // Strong repulsion when too close
                        if (dist < minDistance) {
                            const force = repulsion / (dist * dist) * (1 + (minDistance - dist) / minDistance);
                            fx += (dx / dist) * force;
                            fy += (dy / dist) * force;
                        } else {
                            // Normal repulsion
                            const force = repulsion / (dist * dist);
                            fx += (dx / dist) * force;
                            fy += (dy / dist) * force;
                        }
                    });
                    
                    // Attraction to connected entities (but maintain minimum distance)
                    const connected = connections.get(entity.id);
                    if (connected) {
                        connected.forEach(otherId => {
                            const other = this.entities.get(otherId);
                            if (!other) return;
                            
                            const otherBounds = this._getEntityBounds(other);
                            
                            const dx = otherBounds.centerX - bounds.centerX;
                            const dy = otherBounds.centerY - bounds.centerY;
                            const dist = Math.sqrt(dx * dx + dy * dy);
                            
                            // Only attract if beyond optimal distance
                            const optimalDist = minDistance * 1.2;
                            if (dist > optimalDist) {
                                const attractForce = (dist - optimalDist) * attraction;
                                fx += (dx / dist) * attractForce;
                                fy += (dy / dist) * attractForce;
                            }
                        });
                    }
                    
                    // Gentle centering force to prevent drift
                    const centerX = 400;
                    const centerY = 300;
                    fx += (centerX - bounds.centerX) * 0.001;
                    fy += (centerY - bounds.centerY) * 0.001;
                    
                    // Update velocity with damping
                    const vel = velocities.get(entity.id);
                    vel.vx = (vel.vx + fx) * damping;
                    vel.vy = (vel.vy + fy) * damping;
                });
                
                // Update positions
                entityList.forEach(entity => {
                    const vel = velocities.get(entity.id);
                    entity.x = Math.max(30, entity.x + vel.vx);
                    entity.y = Math.max(30, entity.y + vel.vy);
                });
            }

            // Reset relationship label offsets for clean layout
            this.relationships.forEach(rel => {
                rel.labelOffsetX = 0;
                rel.labelOffsetY = 0;
            });

            // Apply positions
            this._applyEntityPositions(animate, 500);

            // Update relationships, resolve overlaps, and center
            const finalize = () => {
                this._updateRelationshipPaths();
                // Resolve any label overlaps
                this.resolveOverlaps({ animate: false, padding: 30, iterations: 50 });
                // Center the diagram
                this.centerDiagram({ animate });
            };

            if (animate) {
                setTimeout(finalize, 550);
            } else {
                finalize();
            }
        }

        // ==========================================
        // Import/Export
        // ==========================================
        toJSON() {
            return {
                entities: Array.from(this.entities.values()).map(e => e.toJSON()),
                relationships: Array.from(this.relationships.values()).map(r => r.toJSON()),
                inheritances: Array.from(this.inheritances.values()).map(i => i.toJSON()),
                visibility: this.getVisibilityState()
            };
        }
        
        /**
         * Get current visibility state for all items
         * @returns {Object} Visibility state object with hiddenEntities, hiddenRelationships, hiddenInheritances arrays
         */
        getVisibilityState() {
            const hiddenEntities = [];
            const hiddenRelationships = [];
            const hiddenInheritances = [];
            
            // Collect hidden entities by name (more stable than ID across sessions)
            this.visibilityState.entities.forEach((visible, id) => {
                if (visible === false) {
                    const entity = this.entities.get(id);
                    if (entity) {
                        hiddenEntities.push(entity.name);
                    }
                }
            });
            
            // Collect hidden relationships by composite key (name + source + target)
            this.visibilityState.relationships.forEach((visible, id) => {
                if (visible === false) {
                    const rel = this.relationships.get(id);
                    if (rel) {
                        const source = this.entities.get(rel.sourceEntityId);
                        const target = this.entities.get(rel.targetEntityId);
                        hiddenRelationships.push({
                            name: rel.name,
                            source: source?.name || '',
                            target: target?.name || ''
                        });
                    }
                }
            });
            
            // Collect hidden inheritances by source-target names
            this.visibilityState.inheritances.forEach((visible, id) => {
                if (visible === false) {
                    const inh = this.inheritances.get(id);
                    if (inh) {
                        const sourceEntity = this.entities.get(inh.sourceEntityId);
                        const targetEntity = this.entities.get(inh.targetEntityId);
                        if (sourceEntity && targetEntity) {
                            hiddenInheritances.push({
                                source: sourceEntity.name,
                                target: targetEntity.name
                            });
                        }
                    }
                }
            });
            
            return {
                hiddenEntities,
                hiddenRelationships,
                hiddenInheritances
            };
        }
        
        /**
         * Set visibility state from saved state object
         * @param {Object} state - Visibility state with hiddenEntities, hiddenRelationships, hiddenInheritances
         */
        setVisibilityState(state) {
            if (!state) return;
            
            const { hiddenEntities = [], hiddenRelationships = [], hiddenInheritances = [] } = state;
            
            // Build name-to-id maps
            const entityNameToId = new Map();
            this.entities.forEach((entity, id) => {
                entityNameToId.set(entity.name, id);
            });
            
            // Build composite key map for relationships (handles duplicate names)
            const relCompositeToId = new Map();
            const relNameToIds = new Map(); // name → [id, id, ...] for legacy string format
            this.relationships.forEach((rel, id) => {
                const source = this.entities.get(rel.sourceEntityId);
                const target = this.entities.get(rel.targetEntityId);
                const key = `${rel.name}|${source?.name || ''}|${target?.name || ''}`;
                relCompositeToId.set(key, id);
                if (!relNameToIds.has(rel.name)) relNameToIds.set(rel.name, []);
                relNameToIds.get(rel.name).push(id);
            });
            
            // Reset all to visible first
            this.entities.forEach((entity, id) => {
                this.visibilityState.entities.set(id, true);
            });
            this.relationships.forEach((rel, id) => {
                this.visibilityState.relationships.set(id, true);
            });
            this.inheritances.forEach((inh, id) => {
                this.visibilityState.inheritances.set(id, true);
            });
            
            // Apply hidden entities
            hiddenEntities.forEach(name => {
                const id = entityNameToId.get(name);
                if (id) {
                    this.visibilityState.entities.set(id, false);
                    const entity = this.entities.get(id);
                    if (entity && entity.element) {
                        entity.element.style.display = 'none';
                    }
                }
            });
            
            // Apply hidden relationships (supports both legacy string[] and new {name,source,target}[])
            hiddenRelationships.forEach(item => {
                if (typeof item === 'object' && item.name) {
                    const key = `${item.name}|${item.source || ''}|${item.target || ''}`;
                    const id = relCompositeToId.get(key);
                    if (id) this.visibilityState.relationships.set(id, false);
                } else {
                    // Legacy format: hide ALL relationships with this name
                    const ids = relNameToIds.get(item);
                    if (ids) ids.forEach(id => this.visibilityState.relationships.set(id, false));
                }
            });
            
            // Apply hidden inheritances
            hiddenInheritances.forEach(({ source, target }) => {
                const sourceId = entityNameToId.get(source);
                const targetId = entityNameToId.get(target);
                if (sourceId && targetId) {
                    // Find the inheritance with matching source and target
                    this.inheritances.forEach((inh, id) => {
                        if (inh.sourceEntityId === sourceId && inh.targetEntityId === targetId) {
                            this.visibilityState.inheritances.set(id, false);
                        }
                    });
                }
            });
            
            // Update all connection visibility
            this._updateAllConnectionsVisibility();
        }

        /**
         * Load diagram from JSON data
         * Preserves saved entity positions. Only applies auto-layout for fresh/new diagrams.
         * @param {Object} data - The diagram data with entities and relationships
         * @param {Object} options - Load options
         * @param {boolean} options.autoLayout - Apply auto-layout after loading (default: false)
         * @param {boolean} options.center - Center diagram after loading (default: true)
         * @param {boolean} options.animate - Animate the layout (default: false)
         */
        fromJSON(data, options = {}) {
            const {
                autoLayout = false,
                center = true,
                animate = false
            } = options;

            this.clear();
            
            // Build hidden entity names set BEFORE rendering
            const hiddenEntityNames = new Set(data.visibility?.hiddenEntities || []);
            // Build hidden relationship lookup — supports both string[] and {name,source,target}[]
            const hiddenRelRaw = data.visibility?.hiddenRelationships || [];
            const hiddenRelNameSet = new Set();
            const hiddenRelCompositeSet = new Set();
            hiddenRelRaw.forEach(item => {
                if (typeof item === 'object' && item.name) {
                    hiddenRelCompositeSet.add(`${item.name}|${item.source || ''}|${item.target || ''}`);
                    hiddenRelNameSet.add(item.name);
                } else {
                    hiddenRelNameSet.add(item);
                }
            });

            if (data.entities) {
                data.entities.forEach(entityData => {
                    const entity = Entity.fromJSON(entityData);
                    this.entities.set(entity.id, entity);
                    this._renderEntity(entity);
                    
                    // Immediately hide if in hidden list
                    if (hiddenEntityNames.has(entity.name)) {
                        this.visibilityState.entities.set(entity.id, false);
                        if (entity.element) {
                            entity.element.style.display = 'none';
                        }
                    }
                });
            }

            if (data.relationships) {
                data.relationships.forEach(relData => {
                    const relationship = Relationship.fromJSON(relData);
                    this.relationships.set(relationship.id, relationship);
                    this._renderRelationship(relationship);
                    
                    // Immediately hide if in hidden list (composite key or name fallback)
                    const srcEntity = this.entities.get(relationship.sourceEntityId);
                    const tgtEntity = this.entities.get(relationship.targetEntityId);
                    const compositeKey = `${relationship.name}|${srcEntity?.name || ''}|${tgtEntity?.name || ''}`;
                    if (hiddenRelCompositeSet.has(compositeKey) ||
                        (hiddenRelCompositeSet.size === 0 && hiddenRelNameSet.has(relationship.name))) {
                        this.visibilityState.relationships.set(relationship.id, false);
                    }
                });
            }

            if (data.inheritances) {
                data.inheritances.forEach(inhData => {
                    const inheritance = Inheritance.fromJSON(inhData);
                    this.inheritances.set(inheritance.id, inheritance);
                    this._renderInheritance(inheritance);
                });
                // Re-render entities to show inherited properties
                this.entities.forEach(entity => {
                    this._renderEntity(entity);
                    // Re-apply hidden state after re-render
                    if (hiddenEntityNames.has(entity.name)) {
                        if (entity.element) {
                            entity.element.style.display = 'none';
                        }
                    }
                });
            }
            
            // Apply full visibility state (handles inheritances and updates connections)
            if (data.visibility) {
                this.setVisibilityState(data.visibility);
            }

            this._updateStatusBar();

            // Post-load: center diagram if requested, skip layout changes to preserve saved positions
            if (this.entities.size > 0) {
                setTimeout(() => {
                    if (autoLayout) {
                        this.autoLayoutGrid({ animate });
                        setTimeout(() => {
                            this.resolveOverlaps({ animate, padding: 40, iterations: 50 });
                            if (center) {
                                setTimeout(() => {
                                    this.centerDiagram({ animate });
                                }, animate ? 350 : 50);
                            }
                        }, animate ? 450 : 50);
                    } else if (center) {
                        this.centerDiagram({ animate });
                    }
                }, 100);
            }
        }

        _exportJSON() {
            const json = JSON.stringify(this.toJSON(), null, 2);
            const blob = new Blob([json], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'ontoviz-diagram.json';
            a.click();
            URL.revokeObjectURL(url);
        }

        // ==========================================
        // Clear
        // ==========================================
        async _confirmClear() {
            // Use the global showConfirmDialog if available, otherwise fall back to native confirm
            if (typeof window.showConfirmDialog === 'function') {
                const confirmed = await window.showConfirmDialog({
                    title: 'Clear Diagram',
                    message: 'Clear all entities, relationships, and inheritances?',
                    confirmText: 'Clear All',
                    confirmClass: 'btn-danger',
                    icon: 'eraser'
                });
                if (confirmed) this.clear();
            } else {
                if (confirm('Clear all entities, relationships, and inheritances?')) {
                    this.clear();
                }
            }
        }

        clear() {
            this.inheritances.forEach((inh, id) => {
                this.removeInheritance(id);
            });
            this.relationships.forEach((rel, id) => {
                this.removeRelationship(id);
            });
            this.entities.forEach((entity, id) => {
                this.removeEntity(id);
            });
            this._clearSelection();
            this._updateStatusBar();
        }

        // ==========================================
        // Utilities
        // ==========================================
        _escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        /**
         * Update relationship label anchors to show which sides the lines use
         * Rule 18: relationship box has 4 anchors where the lines can be anchored
         * Rule 28: line must come in from one side and go out from another
         * @param {HTMLElement} label - The relationship label element
         * @param {string} connectionType - 'horizontal', 'vertical', or 'self'
         * @param {string} entryAnchorOverride - Optional specific entry anchor
         * @param {string} exitAnchorOverride - Optional specific exit anchor
         */
        _updateRelationshipAnchors(label, connectionType, entryAnchorOverride = null, exitAnchorOverride = null) {
            // Clear all active states
            label.querySelectorAll('.ovz-rel-anchor').forEach(a => {
                a.classList.remove('ovz-anchor-active');
            });
            
            // Use provided anchors or determine from connection type
            let entryAnchor = entryAnchorOverride;
            let exitAnchor = exitAnchorOverride;
            
            if (!entryAnchor || !exitAnchor) {
                if (connectionType === 'horizontal') {
                    entryAnchor = entryAnchor || 'left';
                    exitAnchor = exitAnchor || 'right';
                } else if (connectionType === 'vertical') {
                    entryAnchor = entryAnchor || 'top';
                    exitAnchor = exitAnchor || 'bottom';
                }
                // For self-loops, anchors should always be provided dynamically
                // No fixed defaults - use whatever was passed in
            }
            
            // Highlight active anchors
            if (entryAnchor) {
                const entry = label.querySelector(`.ovz-rel-anchor-${entryAnchor}`);
                if (entry) entry.classList.add('ovz-anchor-active');
            }
            if (exitAnchor) {
                const exit = label.querySelector(`.ovz-rel-anchor-${exitAnchor}`);
                if (exit) exit.classList.add('ovz-anchor-active');
            }
        }

        /**
         * Restrict input to valid identifier characters only
         * Rule (Line 44): Only letters, numbers, underscores (_), and hyphens (-) allowed
         * @param {HTMLInputElement} input 
         */
        _restrictToValidChars(input) {
            if (!input) return; // Guard against null/undefined inputs
            
            const validPattern = /^[a-zA-Z0-9_-]*$/;
            
            // Block invalid keys when typing
            input.addEventListener('keydown', (e) => {
                // Allow control keys (backspace, delete, arrows, etc.)
                if (e.ctrlKey || e.metaKey || e.altKey ||
                    ['Backspace', 'Delete', 'ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 
                     'Home', 'End', 'Tab', 'Enter', 'Escape'].includes(e.key)) {
                    return;
                }
                // Block if key is not a valid character
                if (e.key.length === 1 && !validPattern.test(e.key)) {
                    e.preventDefault();
                }
            });
            
            // Strip invalid characters on paste or any input (also handles IME)
            input.addEventListener('input', () => {
                const pos = input.selectionStart;
                const originalValue = input.value;
                // Remove any character that is not letter, number, underscore, or hyphen
                const cleanedValue = input.value.replace(/[^a-zA-Z0-9_-]/g, '');
                
                if (originalValue !== cleanedValue) {
                    input.value = cleanedValue;
                    // Restore cursor position
                    const newPos = Math.min(pos, cleanedValue.length);
                    input.setSelectionRange(newPos, newPos);
                }
            });
        }
    }

    // ==========================================
    // Export
    // ==========================================
    global.OntoViz = OntoViz;
    global.OntoVizEntity = Entity;
    global.OntoVizRelationship = Relationship;
    global.OntoVizInheritance = Inheritance;

})(typeof window !== 'undefined' ? window : this);

