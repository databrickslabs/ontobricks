# OntoBricks User Guide

## Introduction

OntoBricks is a visual tool for designing ontologies, mapping them to Databricks tables, generating R2RML mappings, synchronizing data to a triple store, and exploring your knowledge graph visually. This guide walks you through the complete workflow.

## Prerequisites

Before starting, ensure you have:
- Access to a Databricks workspace
- Personal access token from Databricks
- SQL Warehouse ID
- Catalog and schema with tables to map

## Application Workflow

OntoBricks follows a 3-step workflow:

```
1. Design Ontology → 2. Assign Data Sources → 3. Digital Twin (Sync & Explore)
```

---

## Step 1: Design Ontology

Navigate to the **Ontology** page by clicking "Ontology" in the navigation bar.

### Option A: Visual Designer (Recommended)

Click **Design** in the sidebar to use the visual drag-and-drop interface.

#### Creating Entities

1. Click the **+ Add Entity** button in the toolbar
2. A new entity box appears on the canvas
3. **Edit the entity name** by clicking on it
4. **Add attributes** using the + button on the entity
5. **Move entities** by dragging them around the canvas

#### Customizing Entities

Each entity supports:
- **Icon**: Click the icon button (🎨) to select an emoji
- **Auto-Map Icons**: In the **Model** view, click the smiley face button (😊) in the toolbar to automatically assign emoji icons to all entities that still have the default icon. This feature uses the domain's configured LLM serving endpoint to pick the most appropriate emoji for each entity name.
- **Description**: Click the description button (📝) to add notes
- **Attributes**: Add data properties directly on the entity

#### Creating Relationships

1. Click and drag from one entity's connector (○) to another
2. A relationship line is created between them
3. **Edit the relationship name** by clicking on the label
4. **Add relationship attributes** using the + button on the relationship

#### Creating Inheritance Links

Inheritance links represent class hierarchies (rdfs:subClassOf):

1. Click the **△ (Inheritance)** button in the toolbar to activate inheritance mode
2. **Drag from a parent entity's connector** to a child entity
3. A dotted line with a hollow arrow appears from parent to child
4. **Child entities automatically inherit** all attributes from the parent (displayed as read-only)
5. Click the inheritance arrow to **reverse the direction** if needed

**Example**: Create an "Employee" entity that inherits from "Person":
- Person has attributes: name, email
- Employee inherits name and email from Person, plus its own attributes: employeeId, salary

#### Relationship Direction

Each relationship has a direction button showing:
- **→** Forward: From source to target entity
- **←** Reverse: From target to source entity
- **↔** Bidirectional: Both directions

Click the direction indicator to cycle through options.

#### Canvas Controls

- **Zoom**: Use the mouse wheel or zoom buttons
- **Pan**: Click and drag the background
- **Auto Layout**: Click the grid icon to organize entities
- **Center**: Click the center button to fit all entities in view
- **Minimap**: Toggle the minimap for navigation

#### Auto-Save

All changes in the Design view are automatically saved. You'll see a brief "Saving..." indicator when changes are persisted.

### Option B: Form-Based Interface

#### Configure Basic Information

1. Click **Information** in the sidebar
2. Enter an **Ontology Name** (e.g., "MyOrganization")
   - Used in the URI and file naming
3. The **Base URI** is auto-generated from:
   - The Default Base URI Domain (from Settings)
   - The domain name
   - Format: `{domain}/{DomainName}#`
   - Example: `https://databricks-ontology.com/MyOrganization#`
   - Toggle the **Custom** switch on the Domain page to enter a custom URI

#### Add Classes (Entities)

1. Click **Entities** in the sidebar
2. Click **Add Class** button
3. Enter:
   - **Name**: Class identifier (e.g., "Person", "Department")
   - **Label**: Human-readable name (optional)
   - **Icon**: Select an emoji/icon for visualization
   - **Description**: Brief description of the entity
4. **Add Attributes**: Use the attributes section to add data properties
5. Click **Add** to save the class

**Example Classes:**
- 👤 Person (represents people in your organization)
- 🏢 Department (organizational units)
- 📋 Project (work projects)

#### Add Relationships (Object Properties)

1. Click **Relationships** in the sidebar
2. Click **Add Property** button
3. Enter:
   - **Name**: Relationship identifier (e.g., "worksIn", "manages")
   - **Domain**: Select source entity from dropdown
   - **Range**: Select target entity from dropdown
   - **Direction**: Choose Forward, Reverse, or Bidirectional
4. **Add Attributes**: Add relationship properties if needed
5. Click **Add** to save

**Example Relationships:**
- worksIn (Person → Department)
- manages (Person → Project)
- collaboratesWith (Person ↔ Person)

### Advanced Features

OntoBricks provides advanced ontology features under the **Advanced** section in the sidebar.

#### SWRL Rules (Graphical Editor)

Click **Business Rules** in the sidebar to define inference rules using SWRL (Semantic Web Rule Language). OntoBricks provides a **graphical D3-based editor** for building rules visually.

1. Click **Add Rule** to open the fullscreen rule editor
2. The editor has two panes:
   - **Left**: Interactive D3 graph showing your ontology classes and relationships
   - **Right**: Rule builder with **IF** (antecedent) and **THEN** (consequent) atom lists
3. **Building rules visually**:
   - Right-click a class or relationship on the graph to open the context menu
   - Choose **Add to IF** or **Add to THEN** to add atoms
   - Variable names (e.g., `?x`, `?y`) are assigned automatically
   - The SWRL preview updates live as you add atoms
4. **Advanced mode**: Toggle the Advanced section to edit raw antecedent/consequent text directly
5. Enter a **Rule Name** and optional **Description**
6. Click **Save Rule**

**Example SWRL Rules:**

```
Rule: InferGrandparent
Antecedent: Person(?x) ∧ hasParent(?x, ?y) ∧ hasParent(?y, ?z)
Consequent: hasGrandparent(?x, ?z)

Rule: InferManager
Antecedent: Person(?x) ∧ worksIn(?x, ?d) ∧ manages(?y, ?d)
Consequent: hasManager(?x, ?y)
```

Rules are compiled to **SQL** (Delta backend) or **Cypher** (LadybugDB backend) for execution in the Reasoning pipeline.

#### Property Constraints

Click **Constraints** in the sidebar to define cardinality and value restrictions.

1. Click **Add Constraint** button
2. Select **Constraint Type**:
   - **Cardinality**: Min/Max/Exact number of values
   - **Value Restrictions**: allValuesFrom, someValuesFrom, hasValue
   - **Property Characteristics**: Functional, Transitive, Symmetric, etc.
3. Select the **Class** and **Property** to constrain
4. Configure type-specific options
5. Click **Save Constraint**

**Example Constraints:**

| Type | Property | Class | Value |
|------|----------|-------|-------|
| Exact Cardinality | hasManager | Employee | 1 |
| Max Cardinality | hasPhone | Person | 3 |
| All Values From | worksIn | Person | Department |
| Functional | hasBirthDate | Person | - |
| Transitive | isPartOf | Organization | - |

#### Expressions & Axioms

Click **Expr. & Axioms** in the sidebar to define OWL class expressions and axioms.

1. Click **Add Axiom** button
2. Select **Axiom Type**:
   - **Class Relationships**: Equivalent, Disjoint, Union, Intersection
   - **Property Relationships**: Inverse, Property Chain, Disjoint Properties
3. Configure subject, objects, and type-specific options
4. Click **Save Axiom**

**Example Axioms:**

| Type | Subject | Objects |
|------|---------|---------|
| Equivalent Classes | Employee | Person, hasJob |
| Disjoint With | Person | Organization |
| Union Of | Contact | Person, Company |
| Inverse Of | isParentOf | hasParent |
| Property Chain | hasGrandparent | hasParent, hasParent |

#### Data Quality (SHACL Shapes)

Click **Data Quality** in the sidebar to define data quality rules using W3C SHACL (Shapes Constraint Language).

1. Click **Add Shape** to create a new data quality shape
2. Select a **Category**:

| Category | Purpose | Example Constraints |
|----------|---------|---------------------|
| **Completeness** | Required fields exist | `sh:minCount 1` — every entity must have a label |
| **Cardinality** | Correct number of values | `sh:maxCount 3` — at most 3 phone numbers |
| **Uniqueness** | Values are unique | `sh:hasValue` — specific value required |
| **Consistency** | Type-correct references | `sh:class` — target must be of correct type |
| **Conformance** | Format compliance | `sh:pattern` — email must match regex |
| **Structural** | Graph structure rules | `sh:closed` — no unexpected properties |

3. Select the **Target Class** and **Property** to constrain
4. Choose the **SHACL Constraint Type** (minCount, maxCount, pattern, datatype, class, hasValue, etc.)
5. Configure constraint-specific parameters
6. Set **Severity** (Violation, Warning, Info) and an optional **Message**
7. Click **Save**

**Import/Export**: Shapes can be exported as W3C-compliant SHACL Turtle and imported from existing SHACL files.

**Validation**: Shapes are executed against the triple store in the Digital Twin → Data Quality section, either via SQL compilation or PySHACL in-memory validation.

### Dashboard Mapping

You can assign Databricks dashboards to entity types for embedded visualization in the Digital Twin:

1. Go to **Ontology** → **Entities**
2. Select an entity type (e.g., Customer, Meter)
3. In the entity details panel, find the **Dashboard** section
4. Click **Assign** to open the dashboard picker
5. Select a dashboard from your Databricks workspace
6. If the dashboard has parameters, map them to entity attributes:
   - **Entity ID**: Maps the parameter to the entity's unique identifier
   - **Attribute Name**: Maps the parameter to a specific entity attribute
7. Click **Save**

**Parameter Mapping:**
When a dashboard has filter parameters (e.g., customer_id, meter_id), you can map them to:
- `__ID__`: The entity's unique identifier (extracted from URI)
- Any attribute defined on the entity type

When viewing an entity in the Digital Twin visualization, the dashboard will be embedded with the correct parameter values.

### Option C: AI-Powered Wizard

Click **Wizard** in the sidebar to generate an ontology automatically from your database schema using an LLM.

1. Select the **LLM Endpoint** (a Databricks Model Serving endpoint)
2. Choose which **catalog/schema** metadata to include
3. Write custom **Guidelines** or pick a **Quick Template**
4. Click **Generate** to create the ontology

#### Quick Templates

The Wizard provides predefined guideline templates for common domains (CRM, E-Commerce, IoT, Healthcare, Energy, etc.). Click a template button to pre-fill the guidelines textarea.

Templates are defined in `src/shared/config/constants.py` under `WIZARD_TEMPLATES` and served to the frontend via the `GET /ontology/wizard/templates` endpoint. To add a new template, add an entry to the dictionary:

```python
WIZARD_TEMPLATES = {
    "my_domain": {
        "label": "My Domain",
        "icon": "star",            # Bootstrap Icons name (without "bi-")
        "guidelines": "Generate an ontology for ...",
    },
    # ... existing templates
}
```

The button will appear automatically in the Wizard UI — no HTML changes needed.

### Option D: Import Industry-Standard Ontologies

Click **Import** in the sidebar to load ontologies from files or industry standards:

- **OWL**: Import an OWL file from your local machine or Unity Catalog
- **RDFS**: Import an RDFS schema file
- **FIBO**: Import the Financial Industry Business Ontology (EDM Council) — select domains such as Foundations, Business Entities, Securities, etc.
- **CDISC**: Import Clinical Data Interchange Standards (PhUSE) — select SDTM, CDASH, SEND, or ADaM modules
- **IOF**: Import the Industrial Ontologies Foundry (OAGi) — select Core, Maintenance, or Supply Chain domains for digital manufacturing ontologies

Industry-standard modules are fetched directly from their official servers/repositories and merged automatically. The Core/Foundation module is always included when selecting domain-specific modules.

### Preview OWL Output

Click **OWL Content** in the sidebar to see the generated OWL in Turtle format.

### Save Your Ontology

- **Validate**: Click to check for issues
- **Save**: Store to Unity Catalog Volume
- **Load**: Load an existing ontology from UC

---

## Step 2: Map Data Sources (Mapping)

Navigate to the **Mapping** page by clicking "Mapping" in the navigation bar.

> **Note**: You must have an ontology loaded before creating mappings. The "Ontology" indicator in the navbar should show a green checkmark.

### Information (Sidebar)

Click **Information** in the sidebar to view the current mapping status:
- Summary of mapped vs unmapped entities
- Summary of mapped vs unmapped relationships
- Count of mapped attributes across all entities
- Overall completion percentage and status

### Visual Mapping Designer

Click **Designer** in the sidebar to use the visual mapping interface. This view provides an interactive force-directed graph of your ontology with color-coded mapping status:

- **Green nodes**: Fully assigned entities (all attributes mapped)
- **Orange nodes**: Partially assigned entities (some attributes missing)
- **Red nodes**: Unassigned entities

#### Mapping Entities

1. Click on any entity node in the graph to open the mapping panel
2. The panel has three tabs:
   - **Wizard**: AI-powered SQL generation using your LLM endpoint and table metadata
   - **SQL**: Direct SQL editing
    - **Mapping**: Interactive column-mapping grid with data preview
3. For **already-assigned** entities, clicking directly loads the Mapping tab and runs the query automatically (direct edit mode)
4. For **new** entities, the Wizard tab opens by default — describe your data and click **Generate** to create SQL
5. In the **Mapping** tab:
   - Click column headers to assign them as ID, Label, or specific attributes
   - Use the **Limit** input to control how many preview rows are shown
   - Click **Refresh** to re-run the query with the current limit
6. Click **Save** in the panel footer

> **Note**: The SQL query is stored without a `LIMIT` clause. The preview limit only affects the grid display and is not part of the saved mapping.

**Example SQL Query:**
```sql
SELECT person_id, name, email, salary
FROM main.default.person
```

#### Mapping Relationships

1. Click on any relationship line in the graph
2. The same panel opens with Wizard, SQL, and Mapping tabs
3. Assign **Source ID** and **Target ID** columns from query results
4. Click **Save**

#### Writing SQL Queries for Relationships

Your SQL query should return columns identifying source and target entities.

**Example 1: Simple Foreign Key**
```sql
SELECT person_id, department_id 
FROM main.default.person
WHERE department_id IS NOT NULL
```

**Example 2: Join/Bridge Table**
```sql
SELECT pd.person_id, pd.department_id
FROM main.default.person_department pd
```

**Example 3: Self-referential Relationship**
```sql
SELECT person1_id as source_id, person2_id as target_id
FROM main.default.person_collaboration
```

### Manual Mapping (Sidebar)

Click **Manual** in the sidebar for a tree-based view of all entities and relationships organized by mapping status. The bottom panel shares the same UI and functionality as the Designer view panel — clicking an item opens the same Wizard/SQL/Mapping tabs.

### Auto-Map (Sidebar)

Click **Auto-Map** in the sidebar to batch-assign all unmapped entities and relationships:

1. The page shows counts of unassigned entities and relationships
2. Click **Start Auto-Map** to launch an asynchronous task
3. Progress is tracked with a progress bar — you can navigate away and return later
4. Results are displayed in a report table showing success/failure per item

**Re-Assign Missing Attributes**: If some entities are assigned but have incomplete attribute mappings, a third card appears showing the count and a **Re-Assign Missing Attributes** button. This re-runs auto-mapping only for those specific entities to fill in the missing attribute mappings.

### Validate Your Mappings

Mapping validation checks:
- All ontology classes have entity mappings
- All object properties have relationship mappings
- All attributes of mapped entities are assigned to columns
- No missing or incomplete mappings

A green checkmark appears in the navbar when all mappings are complete.

### R2RML Output

The R2RML mapping output is available in the **Domain** section under **R2RML**. Navigate to Domain → R2RML to:
- View the automatically generated R2RML mapping in Turtle format
- Copy to clipboard
- Download as `.ttl` file

---

## Step 3: Digital Twin (Sync & Explore)

Navigate to the **Digital Twin** page by clicking "Digital Twin" in the navigation bar (URL: `/dtwin`).

> **Note**: You need both Ontology and Mapping loaded (green checkmarks in navbar). The Sync page shows a readiness status and disables actions until both are ready.

### Sync (Sidebar)

Click **Status** in the sidebar to manage your triple store:

- **Readiness Status**: Shows whether Ontology and mappings (including attribute mappings) are all complete
- **Synchronize**: Generates all triples from your mappings and writes them to the configured triple store backend (Delta or LadybugDB)
- **Last Updated**: When the table contains data, the status area displays the last modification date and time (for Delta from Unity Catalog metadata; for LadybugDB from the local file timestamp)

### Quality (Sidebar)

Click **Quality** in the sidebar to run data quality checks against the triple store:

- Quality checks run **asynchronously** as a background task with progress tracking
- You can navigate away and return — the task resumes from where it left off
- Validates cardinality constraints, value constraints, property characteristics, and global rules
- Shows pass/fail results with violation details
- Displays the generated SQL for each check

### Triples (Sidebar)

Click **Triples** in the sidebar to view triples in an interactive data grid. Triple store data is **automatically loaded** when you navigate to this section:

- **Sortable columns**: Click headers to sort
- **Resizable columns**: Drag column borders
- **Result count**: Shown in tab badge
- Cells show URIs and literal values

### Knowledge Graph (Sidebar)

Click **Knowledge Graph** in the sidebar to explore triples as an interactive sigma.js WebGL-powered graph. Triple store data is **automatically loaded** when you navigate to this section:

**Main Graph Area (left):**
- **Nodes**: Entities (colored by class type with emoji icons in labels)
- **Edges**: Relationships between entities
- **Labels**: Entity labels from rdfs:label or mapped label column
- **Hover**: Highlights the hovered entity and its neighbors; dims unrelated nodes
- **Click**: Selects an entity and locks the highlight until another entity or the background is clicked
- **Zoom**: Scroll to zoom in/out
- **Pan**: Click and drag background
- **Fit to View**: Click the fullscreen button to fit all entities in view

**Find & Filter:**
- **Find**: Search for entities by label or URI — matching entities and their neighbors are highlighted and the camera zooms to focus on results
- **Filters**: Advanced filtering by entity type, field (label/URI), match type (contains, exact, starts with, ends with), with relationship depth control — rebuilds the graph with only matching triples

**Entity Details Panel (right):**
When you click on an entity in the graph, the right panel shows:
- **Entity Type**: The ontology class (e.g., Person, Department) with icon
- **Entity ID**: The unique identifier
- **Entity Label**: The display name
- **Mapped Attributes**: All attributes defined in the mapping with their values
- **Relationships**: All incoming and outgoing relationships with clickable entity links for navigation
- **Dashboard**: If a dashboard is assigned to this entity type, a "View Dashboard" button appears

**Dashboard Embedding:**
If an entity type has an assigned Databricks dashboard (configured in Ontology → Entities):
1. Click "View Dashboard" in the entity details panel
2. A modal opens with the embedded dashboard
3. Dashboard parameters are automatically populated from entity attributes
4. The dashboard displays data specific to the selected entity

**Note**: Empty entities (type URIs without data) are automatically filtered out.

**Data Clusters:**

The Knowledge Graph includes a **Data Clusters** panel (in the View tab) for detecting communities in the graph:

1. **Detect clusters (local)**: Runs the Louvain community detection algorithm client-side using Graphology on the currently displayed subgraph. Adjust the **Resolution** slider to control cluster granularity (higher = more clusters).
2. **Full graph (backend)**: Sends a request to the server which loads the entire triple store into NetworkX and runs the selected algorithm (Louvain, Label Propagation, or Greedy Modularity) on the full dataset. Use this for large graphs that exceed the visible subgraph.
3. **Color by cluster**: Toggle to recolor nodes by their detected community instead of by entity type.
4. **Collapse / Expand**: Collapse clusters into super-nodes that show the cluster size and member count. Click a collapsed cluster super-node to see its members in the detail panel. Expand individual clusters or all at once.
5. **Clear clusters**: Reset all cluster assignments and return to the default visualization.

The cluster panel also displays:
- Total number of detected clusters
- A color-coded chip list of all clusters with their sizes
- Click a chip to toggle collapse/expand for that cluster

### Quality Checks (Sidebar)

Click **Quality** in the sidebar to run automated quality checks on your triple store data:

**Running Quality Checks:**
1. Ensure your triple store has been synchronized
2. Click **Run All Checks** button
3. Quality checks run **asynchronously** — a progress bar shows the current check being executed
4. You can navigate away and return; the task resumes from `sessionStorage`
5. Results are displayed with pass/fail status once all checks complete

**Summary Cards:**
- **Passed**: Number of checks that passed (green)
- **Warnings**: Number of checks that couldn't be validated (yellow)
- **Failed**: Number of checks with violations (red)

**Check Categories:**

1. **Cardinality Constraints**: Validates min/max/exact cardinality on relationships
   - Example: "Each Employee must have exactly 1 manager"
   
2. **Value Constraints**: Validates attribute values
   - **contains**: Value must contain a substring (e.g., email contains "@")
   - **startsWith**: Value must start with a prefix
   - **endsWith**: Value must end with a suffix
   - **equals**: Value must exactly match
   - **matches**: Value must match a regex pattern
   
3. **Relationship Properties**: Validates property characteristics
   - **Functional**: Each subject has at most one value
   - **Symmetric**: If A relates to B, then B relates to A
   - **Asymmetric**: If A relates to B, then B cannot relate to A
   - **Irreflexive**: No entity relates to itself
   
4. **Global Rules**: Validates global data integrity
   - **Require Labels**: All entities must have rdfs:label
   - **No Orphans**: No isolated entities (has relationships)

**Viewing Details:**
- Click the **code icon** to view the generated SQL used for the check
- Click the **violations count button** to see detailed violations in a modal
- The violations modal shows a grid with entity URIs and values that failed the check

### Data Quality — SHACL (Sidebar)

Click **Data Quality** in the Digital Twin sidebar to run SHACL shape validations against the triple store:

1. Shapes defined in **Ontology → Data Quality** are listed with their category, target class, and severity
2. Click **Run Validation** to execute all enabled shapes
3. Each shape is compiled to SQL and executed against the triple store
4. Results show pass/fail status with violation counts and details
5. For small datasets, PySHACL can validate in-memory without SQL

### Reasoning (Sidebar)

Click **Reasoning** in the Digital Twin sidebar to run the multi-phase reasoning pipeline:

1. **OWL 2 RL** — Forward-chaining deductive closure on the ontology (infers subclass hierarchies, domain/range typing, property entailments)
2. **SWRL Rules** — Evaluates user-defined rules (violation detection and optional materialization)
3. **Graph Reasoning** — Transitive closure and symmetric expansion based on OWL property characteristics
4. **Constraint Checking** — Validates cardinality, functional properties, value constraints, and global rules

Results are displayed as inferred triples and violations. Inferred triples can be **materialized** (written back) to the triple store.

---

## Domain Management & Version Control

OntoBricks stores domains in **Unity Catalog Volumes** with built-in version control. Navigate to the **Domain** page by clicking "Domain" in the navigation bar.

### Domain Structure

Each domain is stored as a separate Unity Catalog Volume:
- **Volume Name**: Same as the domain name (sanitized: lowercase, underscores)
- **File Names**: Version numbers (e.g., `v1.json`, `v2.json`, `v3.json`)
- **Contents**: Ontology, mappings, design layout, and domain metadata

**What is Saved:**
- ✅ Ontology (classes, properties, constraints, rules, axioms)
- ✅ Mappings (entity and relationship mappings)
- ✅ Design layout (OntoViz positions and visual configuration)
- ✅ Domain metadata (name, description, author)

**What is NOT Saved (Security):**
- ❌ Databricks credentials (host, token)
- ❌ Query results
- ❌ R2RML output (regenerated on load)
- ❌ Generated OWL (regenerated on load)

### Domain Information (Global Tab)

The **Global** tab in the Domain Information section contains the main domain settings:

| Field | Description |
|-------|-------------|
| **Domain Name** | CamelCase, alphanumeric only (e.g. `MyOntologyDomain`). Defaults to `NewDomain`. Non-alphanumeric characters are stripped automatically and words are capitalized as you type. |
| **Version** | Current version number with version selector. |
| **Base URI** | The base namespace for all ontology entities. By default, auto-generated from `Settings → Default Base URI Domain / DomainName#`. Toggle the **Custom** switch to enter a custom URI. |
| **Description** | Free-text description of the domain. |
| **Author** | Automatically pre-filled with the current Databricks user email. Editable. |
| **API / MCP** | Toggle **Expose via API & MCP** to make this domain visible through the REST API (`/api/v1/domains`) and the MCP server. Disabled by default. |

#### Triple Store Tab

| Field | Description |
|-------|-------------|
| **Backend** | Select the triple store engine: **Delta** (Databricks SQL Warehouse) or **LadybugDB** (embedded graph database). |
| **Catalog.Schema** | *(Delta only)* Defaults to the same catalog and schema as the domain's registry location. Click the pencil icon to change. |
| **Table Name** | *(Delta only)* Defaults to `triplestore_<domain_name>` (sanitized lowercase). Updated automatically when the domain name changes. |
| **Graph Name** | *(LadybugDB)* Logical name for the embedded graph database (defaults to domain name). |

**Backend details:**

| Backend | Storage | Sync | Best for |
|---------|---------|------|----------|
| **Delta** | Databricks Delta table via SQL Warehouse | Immediate (SQL) | Production workloads with Unity Catalog governance |
| **LadybugDB** | Local disk (`/tmp`) with automatic UC Volume sync | Automatic after Digital Twin build | Lightweight demos and local development without a SQL Warehouse |

**Performance:**
- **Delta** tables are created with **Liquid Clustering** (`CLUSTER BY (predicate, subject)`), which co-locates rows by predicate and subject for faster query filtering. After each build, an `OPTIMIZE` command is automatically executed to compact data files and apply the clustering layout.
- **LadybugDB** stores data in an embedded graph database using Cypher. When the domain has an ontology loaded, LadybugDB uses a **true graph model** where each OWL class becomes a node table and each object property becomes a relationship table. Graph data is stored locally at `/tmp` and automatically archived to your registry UC Volume after each Digital Twin build (and restored on domain load).

### Saving Domains

#### First Save

1. Go to **Domain** page
2. Fill in domain details:
   - **Name**: Domain name in CamelCase (becomes the volume folder name)
   - **Description**: Optional description
   - **Author**: Your name (auto-filled from Databricks)
3. Select **Catalog** and **Schema** for storage
4. Click **Save to Unity Catalog**

OntoBricks will:
1. Create a volume named after your domain (if it doesn't exist)
2. Save the domain as `v{version}.json`

#### Subsequent Saves

When you save an existing domain:
- The current version file is **overwritten**
- Use **Create New Version** to preserve history

### Version Management

#### Listing Versions

1. Go to **Domain** → **Actions** tab
2. Select catalog/schema containing your domain
3. Click **Load versions** to see all saved versions

The version list shows:
- Version number (e.g., 1, 2, 3)
- File name (e.g., `v1.json`, `v2.json`)
- Active version indicator

#### Creating a New Version

To create a new version (preserving the current state):

1. Go to **Domain** → **Actions** tab
2. Click **Create New Version**
3. OntoBricks will:
   - Increment the version number (e.g., 1 → 2)
   - Save the new version to Unity Catalog
   - Keep previous versions intact

**Example version progression:**
```
my_domain/
├── v1.json    (initial version)
├── v2.json    (after first "Create New Version")
└── v3.json    (after second "Create New Version")
```

#### Loading a Domain from Registry

1. Click **Load Domain** in the top menu
2. Select a domain from the dropdown
3. The **active (latest) version** is automatically selected
4. Change the version if needed, then click **Load**

**Note**: Loading an older version puts you in "read-only" mode. Create a new version to make changes.

### Version Status

The Domain page shows the current version status:
- **Active Version**: You're working on the latest version
- **Historical Version**: You've loaded an older version (read-only for new versions)

### Domain Save/Load

Domains are saved in a versioned JSON format and can be stored in Unity Catalog Volumes. Use the **Save Domain** and **Load Domain** options in the top menu to persist and restore your work.

### Best Practices for Version Control

1. **Version Before Major Changes**: Create a new version before significant ontology modifications
2. **Use Descriptive Names**: Choose domain names that clearly identify the subject area
3. **Document Versions**: Use the description field to note changes between versions
4. **Regular Saves**: Save frequently to avoid losing work
5. **Test After Loading**: Verify R2RML regeneration after loading older versions

---

## Best Practices

### Ontology Design

1. **Use Meaningful Names**: Choose clear, descriptive names
2. **Consistent Naming**: Use CamelCase for classes, camelCase for properties
3. **Start Simple**: Begin with core entities, add complexity later
4. **Use the Visual Designer**: The Design view makes it easy to see relationships
5. **Choose Good Icons**: Visual icons help identify entities in graphs
6. **Set Relationship Directions**: Be explicit about data flow direction
7. **Use Inheritance Wisely**: Create class hierarchies for shared attributes
8. **Avoid Deep Hierarchies**: Keep inheritance chains shallow (2-3 levels max)

### Mapping Strategy

1. **Map Core Entities First**: Start with main entity types
2. **Verify ID Columns**: Ensure IDs are unique and stable
3. **Test SQL Queries**: Always test relationship queries before saving
4. **Use Consistent Column Types**: Source/target columns should match entity IDs

### Digital Twin Tips

1. **Sync After Changes**: Re-synchronize after modifying ontology or mappings
2. **Check Quality**: Run quality checks after syncing to catch constraint violations early
3. **Use Knowledge Graph**: The interactive graph is the best way to explore entity relationships
4. **Review Triples**: Browse the triples grid to verify the generated data looks correct
5. **Performance**: The `/stats` API aggregates all scalar metrics in a single SQL query and the `/triples/find` BFS traversal uses a recursive CTE, minimizing SQL Warehouse round trips
6. **Programmatic Access**: Use the Digital Twin API (`/api/v1/digitaltwin/`) or the MCP server for programmatic and conversational access to your knowledge graph

---

## GraphQL API

Once your triple store is materialized (synced via Digital Twin), OntoBricks automatically provides a **typed GraphQL API** for each domain. The schema is auto-generated from the ontology — no manual configuration required.

### Accessing GraphQL

1. Navigate to **Digital Twin** → **API** and scroll to the **GraphQL API** section
2. Alternatively, visit `/graphql/{domain_name}` to open the **GraphiQL Playground** directly

### Available Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/graphql` | GET | List all GraphQL-enabled domains |
| `/graphql/{domain_name}` | GET | Open GraphiQL playground for a domain |
| `/graphql/{domain_name}` | POST | Execute a GraphQL query |
| `/graphql/{domain_name}/schema` | GET | Get the SDL (Schema Definition Language) |

### Querying with GraphQL

The auto-generated schema provides two resolvers per ontology class:
- **`all<ClassName>`**: List entities with optional `limit`, `offset`, and `search` parameters
- **`<className>`**: Fetch a single entity by `id`

**Example query** (for an ontology with `Customer` and `Interaction` classes):

```graphql
{
  allCustomer(limit: 10, search: "Smith") {
    id
    label
    email
    hasInteraction {
      label
      date
    }
  }
}
```

### Schema Introspection

Use the SDL endpoint to inspect the full schema:

```bash
curl http://localhost:8000/graphql/my_domain/schema
```

This returns the complete type definitions, which is useful for integrating with external tools or for LLM agents to discover the data model programmatically.

### Tips

1. **Nested Traversal**: Unlike flat triple queries, GraphQL lets you traverse relationships (e.g., `customer → interactions → details`) in a single query
2. **Schema Reflects Ontology**: If you add new classes or properties, re-sync the triple store and the GraphQL schema updates automatically
3. **GraphiQL Auto-Complete**: The playground provides documentation and auto-complete for all types and fields

### Relationship Depth Control

GraphQL queries support configurable traversal depth for nested relationships:

- **Default depth**: 2 (direct neighbors and their neighbors)
- **Maximum depth**: 5
- **How to set**: Use the **Depth** dropdown in the GraphiQL playground, or include `"depth": N` in the POST request body

Higher depth values allow deeper nested traversal (e.g., `customer → interaction → contract → meter`) but may increase query time.

---

## MCP Server (Databricks Playground)

OntoBricks includes an MCP server that exposes knowledge-graph tools to LLM clients via the [Model Context Protocol](https://modelcontextprotocol.io/). This enables conversational access to your knowledge graph from the Databricks Playground, Cursor, Claude Desktop, and other MCP-compatible tools.

### Available Tools

| Tool | Description |
|------|-------------|
| `list_domains` | List all MCP-enabled domains in the registry |
| `select_domain` | Activate a domain for subsequent queries |
| `list_domain_versions` | List registry versions for a domain |
| `get_design_status` | Ontology / metadata / assignment readiness for a domain |
| `list_entity_types` | Overview of entity types, counts, and predicates |
| `describe_entity` | Full-text description of an entity with BFS traversal |
| `get_graphql_schema` | Auto-generated GraphQL schema (SDL) for the domain |
| `query_graphql` | Execute a GraphQL query with structured results |
| `get_status` | Triple store diagnostic (view, graph, count) |

### Using in Databricks Playground

1. Deploy the MCP server as `mcp-ontobricks` (see [Deployment Guide](deployment.md))
2. In your Databricks workspace, navigate to **Playground**
3. Select **mcp-ontobricks** from the MCP Servers list
4. Ask questions like *"What entity types are in the knowledge graph?"* or *"Tell me about Jacob Martinez"*

### Enabling Domains for MCP

Domains must have the **API / MCP** flag enabled to be visible through the MCP server:

1. Go to **Domain > Information > Global** tab
2. Toggle **Expose via API & MCP** to ON
3. Save the domain

See the [MCP Server documentation](mcp.md) for full details including local usage and client configuration.

---

## Example: HR Domain

### Step 1: Design Ontology (Using Visual Designer)

1. Open **Ontology** → **Design**
2. Create entities:
   - 👤 Person (add attributes: name, email)
   - 👔 Manager (add attributes: managementLevel)
   - 🏢 Department (add attributes: departmentName)
   - 📋 Project (add attributes: projectTitle, budget)
3. Create inheritance:
   - Click the △ button in the toolbar
   - Drag from Person to Manager → Manager inherits name and email from Person
4. Create relationships:
   - Drag from Person to Department → name it "worksIn" (Forward)
   - Drag from Manager to Project → name it "manages" (Forward)
   - Drag from Person to Person → name it "collaboratesWith" (Bidirectional)
5. Use **Auto Layout** to organize the diagram
6. Click **Center** to fit everything in view

### Step 2: Create Mappings (Mapping)

1. Open **Mapping** → **Designer**
2. Click on each entity to configure its mapping:

**Entity Mappings:**
| Class | Table | ID Column |
|-------|-------|-----------|
| Person | person | person_id |
| Department | department | department_id |
| Project | project | project_id |

3. Click on each relationship to configure its mapping:

**Relationship Mappings:**
| Property | SQL Query | Source Col | Target Col |
|----------|-----------|------------|------------|
| worksIn | `SELECT person_id, dept_id FROM person_department` | person_id | dept_id |
| manages | `SELECT manager_id, project_id FROM project_managers` | manager_id | project_id |
| collaboratesWith | `SELECT person1_id, person2_id FROM collaborations` | person1_id | person2_id |

### Step 3: Explore (Digital Twin)

1. Go to **Digital Twin** → **Status** and click **Synchronize** to generate triples from your mappings
2. Once synced, click **Triples** to browse all generated triples in a sortable grid
3. Click **Knowledge Graph** to explore the knowledge graph as an interactive sigma.js WebGL graph
4. Click on any entity node to see its type, label, attributes, and values in the details panel
5. Click **Quality** to run automated quality checks against your ontology constraints

---

## Troubleshooting

### Connection Issues

**Problem**: "Connection failed" error

**Solutions**:
- Verify Databricks host URL is correct (include `https://`)
- Check token has not expired
- Ensure SQL Warehouse is running
- Verify network connectivity

### No Tables Showing

**Problem**: Empty table list in mapping modal

**Solutions**:
- Verify catalog and schema names are correct
- Check permissions on the schema
- Ensure tables exist in the schema

### Query Test Fails

**Problem**: SQL query test returns error

**Solutions**:
- Check SQL syntax
- Verify table and column names
- Ensure you have SELECT permissions
- Only SELECT queries are allowed

### Empty Knowledge Graph

**Problem**: Graph shows no nodes or edges

**Solutions**:
- Ensure the triple store has been synchronized (check Triples section)
- Verify relationships exist in data
- Click "Fit to View" button
- Click the reload button to re-render the graph
- Check browser console for errors

### Mapping Validation Fails

**Problem**: Error when validating mappings

**Solutions**:
- Ensure all ontology classes are mapped (via Mapping → Designer)
- Check all object properties have relationship mappings
- Verify all entity attributes are assigned to SQL columns (check for orange indicators in Designer view)
- Use Auto-Map → Re-Assign Missing Attributes to fix incomplete attribute mappings
- Verify table and column names match

### LadybugDB Graph Not Persisting

**Problem**: LadybugDB data is lost after application restart

**Solutions**:
- The LadybugDB graph is automatically archived to the registry UC Volume after each Digital Twin build — ensure your build completes successfully
- After loading a domain, the graph archive is automatically restored from the registry
- If the registry is not configured, LadybugDB data lives only in `/tmp` and will not survive a restart

### Design Changes Not Saving

**Problem**: Changes in Design view are not persisted

**Solutions**:
- Wait for the "Saving..." indicator to complete
- Check browser console for errors
- Ensure you have network connectivity
- Try refreshing the page

### Relationship Direction Issues

**Problem**: Direction not displaying correctly

**Solutions**:
- Click the direction button (→/←/↔) to cycle through options
- Verify the direction is set in both Design and Relationships views
- Check that source and target entities are correctly identified

---

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl/Cmd + Enter | Confirm action |
| Tab | Navigate between form fields |
| Enter | Submit form / confirm dialog |
| Escape | Close modal |

---

## Support

- Review the [Architecture](architecture.md) documentation
- Check the [API Reference](api.md)
- See [Deployment Guide](deployment.md) for production setup
- Check sample data in the `data/` folder


---

## Automated triple-store pipeline (merged)

## Automated Triple Store Creation

This guide walks you through creating a fully populated **knowledge graph triple store** from scratch using OntoBricks' automated features. With LLM-powered ontology generation, automatic data mapping, and one-click synchronization, you can go from raw Databricks tables to a queryable triple store in minutes.

---

### Prerequisites

Before you start, make sure you have:

- A **Databricks workspace** with tables in Unity Catalog
- A **SQL Warehouse** (Serverless or Classic)
- A **Databricks Model Serving endpoint** (for LLM features — e.g., `databricks-meta-llama-3-3-70b-instruct`, or any chat/completions endpoint)
- A **Personal Access Token** with permissions to read tables and execute queries

---

### Overview

The automated pipeline follows these steps:

```
Step 1: Configure connection
        │
Step 2: Set up domain (LLM endpoint, triple store table)
        │
Step 3: Import table metadata from Unity Catalog
        │
Step 4: Generate ontology with the Wizard (LLM-powered)
        │
Step 5: Auto-Map data mappings (LLM-powered)
        │
Step 6: Synchronize to the triple store
        │
Step 7: Validate with quality checks
        │
        ▼
   Knowledge graph ready
```

---

### Step 1: Configure Databricks Connection

Navigate to **Settings** (gear icon in the top-right corner).

1. Enter your **Databricks Host** URL (e.g., `https://your-workspace.cloud.databricks.com`).
2. Enter your **Personal Access Token**.
3. Click **Test Connection** to verify connectivity.
4. Select a **SQL Warehouse** from the dropdown list.
5. Click **Save**.

The connection status indicator in the navbar should turn green.

---

### Step 2: Set Up the Domain

Navigate to **Domain** in the top navbar, then open the **Information** sidebar section.

1. Enter a **Domain Name** (e.g., `CustomerAnalytics`).
2. Set the **Base URI** for your ontology (e.g., `https://ontobricks.com/ontology/`). This is the namespace for all generated RDF resources.
3. Select the **LLM Endpoint** from the dropdown. This is the Databricks Model Serving endpoint used for ontology generation and auto-mapping.
4. Configure the **Triple Store Table**: select a catalog, schema, and table name where triples will be stored (e.g., `my_catalog.my_schema.triples`). The table will be created automatically during sync.

---

### Step 3: Import Table Metadata

Navigate to **Domain > Metadata** in the sidebar.

This step tells OntoBricks about the Databricks tables you want to model in your ontology.

1. Select a **Catalog** from the dropdown.
2. Select a **Schema** from the dropdown.
3. Click **List Tables** to see all available tables.
4. **Check the tables** you want to include in your ontology (or use "Select All").
5. Click **Initialize Metadata**.

OntoBricks fetches column names, types, and comments from Unity Catalog for each selected table. This metadata is used by the Wizard and Auto-Map features to understand your data structure.

> **Tip**: Include all tables that are relevant to the domain you want to model. The more context the LLM has, the better the ontology and mappings will be.

---

### Step 4: Generate the Ontology with the Wizard

Navigate to **Ontology** in the top navbar, then open **Wizard** in the sidebar.

The Wizard uses your LLM endpoint and the imported metadata to automatically design an ontology.

1. You'll see the list of tables loaded from metadata. **Check the tables** you want the LLM to consider.
2. **(Optional)** Click a **Quick Template** button to pre-fill domain-specific guidelines:
   - **CRM** — customers, contacts, accounts, opportunities
   - **E-Commerce** — products, categories, orders, payments
   - **IoT** — devices, sensors, measurements, locations
   - **Healthcare** — patients, providers, appointments, diagnoses
   - **Energy** — energy-sector customer relationship management
3. Review or edit the **guidelines** text area. You can add specific instructions like "Create a Customer entity with relationships to Contract and Invoice."
4. Configure generation options:
   - **Include Data Properties** — generate attributes for entities
   - **Include Relationships** — generate relationships between entities
   - **Include Inheritance** — generate class hierarchies
   - **Use Table Names** — use original table names as entity names
   - **Use Column Comments** — use UC column comments in descriptions
5. Click **Generate**.
6. The LLM generates an OWL ontology in Turtle format. You can **preview** the result.
7. Click **Apply** to import the generated ontology into your domain.

After applying, switch to the **Model** view in the sidebar to see the visual ontology with entities, relationships, and inheritance links.

> **Tip**: You can edit the generated ontology afterwards — add or remove entities, rename relationships, set icons, adjust attributes.

---

### Step 5: Auto-Map Data Mappings

Navigate to **Mapping** in the top navbar, then open **Auto-Map** in the sidebar.

Auto-Map uses the LLM to automatically generate SQL queries that map each ontology entity and relationship to your Databricks tables.

1. Review the list of **unmapped entities** and **unmapped relationships**.
2. Click **Start Auto-Map**.
3. OntoBricks processes each entity and relationship:
   - Sends the entity name, attributes, and metadata context to the LLM
   - The LLM generates a SQL query (SELECT statement)
   - The SQL is validated by executing it against the SQL warehouse
   - Column mappings are inferred (ID, Label, and attribute columns)
4. A progress bar shows the mapping progress.
5. When complete, review the results — successfully mapped items show in green.
6. Click **Apply All** to save all mappings to the domain.

You can verify individual mappings by switching to the **Designer** view:
- **Green** nodes = fully assigned (all attributes mapped)
- **Orange** nodes = partially assigned (some attributes missing)
- **Red** nodes = unassigned

> **Tip**: If some entities remain unassigned or partially assigned, you can click on them in the Designer view to manually edit or regenerate their SQL mapping.

---

### Step 6: Synchronize to the Triple Store

Navigate to **Digital Twin** in the top navbar. The **Status** section opens by default.

Before syncing, OntoBricks validates readiness:
- **Ontology**: At least one entity with a valid URI
- **Entity Mappings**: All entities have SQL assignments
- **Relationship Mappings**: All relationships have SQL assignments

If all checks pass:

1. Click **Synchronize**.
2. OntoBricks:
   - Generates R2RML mappings from your entity and relationship assignments
   - Translates the mappings into Spark SQL queries
   - Executes the queries against your SQL warehouse
   - Creates the triple store table (Delta format) with columns: `subject`, `predicate`, `object`
   - Inserts all generated triples
3. A progress indicator shows the sync status.
4. When complete, you'll see the **triple count** and **last updated timestamp**.

> **Note**: If the triple store table already exists, you can choose to **drop and recreate** it or append to the existing data.

> **Triple Store Backend**: OntoBricks supports two backends — **Delta** (SQL Warehouse, recommended for production) and **LadybugDB** (embedded graph, ideal for development/testing). Configure the backend in Domain > Information > Triple Store settings.

---

### Step 7: Validate with Quality Checks

Still in the **Digital Twin** section, open **Quality** in the sidebar.

Quality checks validate the triple store against your ontology using two complementary systems:

##### Legacy Constraint Checks
These check OWL property constraints defined in the ontology:

| Check | What It Validates |
|-------|-------------------|
| **Cardinality** | Min/max/exact property counts per entity |
| **Functional** | At most one value per subject for a property |
| **Inverse Functional** | At most one subject per object for a property |
| **Symmetric** | If A→B exists, B→A must also exist |
| **Asymmetric** | No symmetric pairs allowed |
| **Irreflexive** | No self-referencing triples |
| **Require Labels** | All entities have rdfs:label |
| **No Orphans** | No entities with only type and label |

##### SHACL Data Quality Shapes
Define fine-grained rules using the W3C SHACL standard (Ontology > Data Quality sidebar):

| Shape Type | What It Validates |
|------------|-------------------|
| **sh:minCount / sh:maxCount** | Required fields, exact cardinality |
| **sh:datatype** | Value type (integer, date, boolean, string) |
| **sh:pattern** | Regex pattern matching |
| **sh:hasValue** | Required specific values |
| **sh:class** | Object must be of a specific type |
| **sh:sparql** | Custom SPARQL-based rules (e.g., no orphans, unique IDs) |

SHACL shapes are compiled to **Spark SQL** for Delta execution or evaluated **in-memory** against LadybugDB. Results show violations, pass rates, and per-entity details.

1. Click **Run All Checks** to execute all applicable checks, or run individual checks.

2. Results show pass/fail for each check with details on any violations.
3. Use the results to identify data quality issues in your source tables or ontology design.

---

### Step 7b: Run Reasoning (Optional)

Still in the **Digital Twin** section, open **Reasoning** in the sidebar.

Reasoning discovers new facts (inferred triples) from your ontology rules:

1. Select the reasoning **phases** to run:
   - **T-Box (OWL 2 RL)**: Infers class hierarchies, property inheritance, and domain/range constraints
   - **SWRL Rules**: Executes your custom business rules (e.g., "if Person hasParent Parent and Parent hasParent Grandparent, then Person hasGrandparent Grandparent")
   - **Structural**: Applies transitivity, symmetry, and other property characteristics
2. Optionally enable **Materialization** to write inferred triples back to the triple store
3. Click **Start Reasoning**
4. Review the inferred triples — each shows the source rule and reasoning phase

> **Note**: Reasoning is most effective when you have SWRL rules defined (Ontology > Business Rules) and a rich ontology with property characteristics.

---

### Step 8: Explore Your Knowledge Graph

After sync, you can explore the triple store:

#### Triples Grid
Open **Triples** in the sidebar to browse the raw triple data in a sortable, searchable grid.

#### Knowledge Graph
Open **Knowledge Graph** in the sidebar to explore the knowledge graph interactively:
- **Find** specific entities by name, type, or URI — matching entities and their neighbors are highlighted
- **Filter** by entity type, field, match type, and relationship depth
- **Navigate** relationships — click an entity to see its attributes, values, and connected entities in the detail panel
- **Toggle labels** for node and edge labels
- **Hide orphans** to focus on connected entities

---

### Complete Workflow Summary

| Step | Where | Action | Automated? |
|------|-------|--------|------------|
| 1 | Settings | Configure Databricks connection | Manual (one-time) |
| 2 | Domain > Information | Set LLM endpoint and triple store table | Manual (one-time) |
| 3 | Domain > Metadata | Import table metadata from Unity Catalog | One click |
| 4 | Ontology > Wizard | Generate ontology from metadata using LLM | One click |
| 5 | Mapping > Auto-Map | Auto-map entities and relationships to SQL | One click |
| 6 | Digital Twin > Status | Synchronize to triple store | One click |
| 7 | Digital Twin > Quality | Run quality checks | One click |

After the initial one-time configuration (steps 1–2), the entire pipeline from metadata to triple store is **four clicks**: Import Metadata, Generate, Auto-Map, Synchronize.

---

### Tips for Best Results

- **Table and column naming**: The LLM performs best when table and column names are descriptive. If your tables use cryptic names, add **comments** in Unity Catalog before importing metadata.
- **Start with a template**: Use one of the Wizard quick-templates (CRM, IoT, etc.) if your domain matches — it provides better guidelines for the LLM.
- **Review before syncing**: After auto-map, quickly review the Designer view. Fix any red or orange nodes before synchronizing.
- **Iterate**: The pipeline is not a one-shot process. You can re-generate the ontology, re-run auto-map, or manually adjust individual mappings at any time.
- **Save your domain**: After achieving a good result, save the domain to Unity Catalog (**Save Domain** in the top menu) so you can reload it later.

---

### REST API — Programmatic Pipeline

The full pipeline is also available via REST API for automation or CI/CD integration:

```bash
## Step 3: Import metadata
curl -X POST /domain/metadata/initialize-async \
  -d '{"catalog": "my_catalog", "schema": "my_schema", "tables": ["t1", "t2"]}'

## Step 4: Generate ontology
curl -X POST /ontology/wizard/generate-async \
  -d '{"metadata": {...}, "guidelines": "...", "options": {...}}'

## Step 4b: Apply generated ontology
curl -X POST /ontology/import-owl \
  -d '{"content": "<turtle content>"}'

## Step 5: Auto-Map
curl -X POST /mapping/auto-assign/start \
  -d '{"entities": [...], "relationships": [...], "schema_context": {...}}'

## Step 6: Sync
curl -X POST /dtwin/sync/start \
  -d '{"triplestore_table": "catalog.schema.table", "drop_existing": true}'

## Step 7: Quality checks
curl -X POST /dtwin/quality/start \
  -d '{"triplestore_table": "catalog.schema.table"}'
```

Async endpoints return a `task_id`. Poll `GET /tasks/{task_id}/status` for progress and results.

---

### Programmatic & MCP Access

After your knowledge graph is built, it can be queried programmatically:

- **Digital Twin API** (`/api/v1/digitaltwin/`): Stateless REST endpoints for triple store status, entity search, ontology retrieval, and more. See [External API](api.md).
- **GraphQL API** (`/graphql/{domain_name}`): Auto-generated typed schema with nested relationship traversal. See [External API](api.md#graphql-api).
- **MCP Server**: Expose your knowledge graph to the Databricks Playground and LLM clients. See [MCP Server](mcp.md).

---

## Ontology import (merged)

## Importing Ontologies

OntoBricks supports multiple ways to create or bootstrap an ontology: from scratch using the visual designer, from OWL/RDFS files, or by importing industry-standard ontologies. This page covers every import method available in the **Ontology > Import** section.

---

### Import Methods at a Glance

| Method | Source | Formats | Use Case |
|--------|--------|---------|----------|
| **OWL** | Local file or Unity Catalog Volume | `.ttl`, `.owl`, `.rdf`, `.xml` | Load an existing OWL ontology |
| **RDFS** | Local file or Unity Catalog Volume | `.ttl`, `.rdf`, `.xml`, `.rdfs`, `.n3`, `.nt` | Load an RDF Schema |
| **FIBO** | EDM Council spec server | RDF/XML, Turtle | Financial industry ontology |
| **CDISC** | PhUSE GitHub repository | RDF/XML, Turtle | Clinical data standards |
| **IOF** | IOF GitHub repository | RDF/XML | Digital manufacturing ontology |

All imports merge the fetched content, parse it, and store the result in the current domain session. You can continue editing entities, relationships, and attributes after import.

---

### OWL Import

Import an ontology written in the **Web Ontology Language (OWL)**.

#### From a Local File

1. Open **Ontology > Import > OWL**.
2. Click **Choose File** and select an OWL file (`.ttl`, `.owl`, `.rdf`, `.xml`).
3. Click **Import**.
4. OntoBricks parses the file and loads all classes, properties, constraints, SWRL rules, and axioms.

#### From Unity Catalog

1. In the same OWL tab, switch to the **Unity Catalog** sub-tab.
2. Select a **Catalog**, **Schema**, and **Volume** from the dropdown lists.
3. OntoBricks lists the OWL files found in the volume.
4. Select a file and click **Load**.

---

### RDFS Import

Import an **RDF Schema** file. RDFS provides a lighter vocabulary than OWL — class hierarchies and property definitions are supported.

1. Open **Ontology > Import > RDFS**.
2. Choose a local file or a Unity Catalog Volume file (`.ttl`, `.rdf`, `.xml`, `.rdfs`, `.n3`, `.nt`).
3. Click **Import**.

---

### FIBO — Financial Industry Business Ontology

[FIBO](https://spec.edmcouncil.org/fibo/) is developed by the **EDM Council** and provides a comprehensive ontology for the financial industry: entities, instruments, products, business processes, and regulatory concepts.

#### Available Domains

| Domain | Key | Required | Description |
|--------|-----|----------|-------------|
| **Foundations** | FND | Yes (auto-included) | Core concepts: parties, agreements, relations, dates, organizations, accounting |
| **Business Entities** | BE | No | Legal entities, corporations, partnerships, government bodies, ownership structures |
| **Financial Business & Commerce** | FBC | No | Financial products, services, intermediaries, markets, instruments |
| **Loans** | LOAN | No | Loan products, applications, mortgages, real estate lending |
| **Securities** | SEC | No | Equities, bonds, debt instruments, investment funds |
| **Derivatives** | DER | No | Options, futures, swaps, commodity and currency contracts |

#### How to Import

1. Open **Ontology > Import > FIBO**.
2. Check the domains you want. **Foundations (FND)** is always included because all other domains depend on it.
3. Click **Import Selected Domains**.
4. OntoBricks fetches the modules from `spec.edmcouncil.org`, merges them with RDFLib, and parses the result.
5. A summary notification shows the number of classes and properties imported.

#### Source

Modules are downloaded from the EDM Council specification server:

```
https://spec.edmcouncil.org/fibo/ontology/master/latest/
```

---

### CDISC — Clinical Data Interchange Standards

[CDISC](https://www.cdisc.org/) standards define data models for clinical research. OntoBricks imports the RDF representations published by the [PhUSE](https://github.com/phuse-org/rdf.cdisc.org) working group.

#### Available Standards

| Standard | Key | Required | Description |
|----------|-----|----------|-------------|
| **Schemas** | SCHEMAS | Yes (auto-included) | Meta-Model (ISO 11179), CT Schema, CDISC Schema — the foundational layer |
| **SDTM** | SDTM | No | Study Data Tabulation Model (v1.2, v1.3, IG 3.1.2, IG 3.1.3) |
| **CDASH** | CDASH | No | Clinical Data Acquisition Standards Harmonization (v1.1) |
| **SEND** | SEND | No | Standard for Exchange of Nonclinical Data (IG 3.0) |
| **ADaM** | ADaM | No | Analysis Data Model (v2.1, IG 1.0) |

#### How to Import

1. Open **Ontology > Import > CDISC**.
2. Check the standards you want. **Schemas** is always included.
3. Click **Import Selected Standards**.
4. OntoBricks fetches the modules from GitHub, merges them, and applies a custom mapper that translates CDISC-specific constructs (DomainContext, Domain, Dataset, DataElement) into OWL classes and properties.

#### Source

Modules are downloaded from the PhUSE GitHub repository:

```
https://github.com/phuse-org/rdf.cdisc.org
```

#### Note on CDISC Mapping

When CDISC standard data (SDTM, CDASH, etc.) is imported, OntoBricks uses a specialized mapper rather than the generic OWL parser:

- **DomainContext** entries become top-level OWL classes.
- **Domain** / **Dataset** entries become OWL classes grouped under their context.
- **DataElement** entries become data properties attached to their parent domain.

This produces a more meaningful ontology structure than a raw RDF parse.

---

### IOF — Industrial Ontologies Foundry

[IOF](https://www.industrialontologies.org/) is developed by **OAGi** and provides ontologies for digital manufacturing. All IOF ontologies are built on **BFO (Basic Formal Ontology)**.

#### Available Domains

| Domain | Key | Required | Description |
|--------|-----|----------|-------------|
| **Core** | CORE | Yes (auto-included) | Common manufacturing concepts: agents, processes, capabilities, organizations, products, business functions |
| **Maintenance** | MAINTENANCE | No | Maintenance management, procedures, asset failure analysis, FMEA |
| **Supply Chain** | SUPPLYCHAIN | No | Procurement, transportation, warehousing, distribution, logistics |

#### How to Import

1. Open **Ontology > Import > IOF**.
2. Check the domains you want. **Core** is always included because all domain ontologies depend on it.
3. Click **Import Selected Domains**.
4. OntoBricks fetches the RDF modules from GitHub, merges them with RDFLib, and parses the result.
5. A post-processing step extracts relationships from OWL restrictions and resolves BFO property labels to human-readable names.

#### Source

Modules are downloaded from the IOF GitHub repository:

```
https://github.com/iofoundry/ontology
```

#### Note on BFO-Based Ontologies

IOF ontologies define most class-to-class relationships through **OWL restrictions** (`owl:someValuesFrom` / `owl:allValuesFrom` inside `rdfs:subClassOf`) rather than explicit `rdfs:domain` / `rdfs:range` on properties. OntoBricks includes a dedicated extraction step that:

1. Scans `rdfs:subClassOf` and `owl:equivalentClass` axioms for restriction patterns.
2. Resolves opaque BFO property URIs (e.g., `BFO_0000057`) to readable labels (e.g., `hasParticipantAtSomeTime`) using graph labels and a built-in dictionary.
3. Filters out relationships whose endpoints reference external BFO classes not present in the import.

This ensures the ontology model displays accurate, readable relationships.

---

### REST API Endpoints

All import operations are also available through the REST API.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ontology/parse-owl` | Parse OWL content (body: `{"content": "..."}`) |
| `POST` | `/ontology/parse-rdfs` | Parse RDFS content (body: `{"content": "..."}`) |
| `GET` | `/ontology/list-owl-files` | List OWL files in a UC Volume (params: `catalog`, `schema`, `volume`) |
| `POST` | `/ontology/load-owl-file` | Load an OWL file from UC (body: `catalog`, `schema`, `volume`, `filename`) |
| `GET` | `/ontology/{kind}-catalog` | Get an industry ontology catalog (`kind` = `fibo`, `cdisc`, or `iof`) |
| `POST` | `/ontology/import-{kind}` | Import an industry ontology (`kind` = `fibo`, `cdisc`, or `iof`; body: `{"domains": [...]}`) |

See the [API Reference](api.md) for complete request/response details.

---

### Common Import Workflow

Regardless of the method, the import workflow follows the same pattern:

```
Select source & options
        │
        ▼
Fetch modules (concurrently for standards)
        │
        ▼
Merge into a single RDFLib graph
        │
        ▼
Serialize to Turtle
        │
        ▼
Parse with OWL/RDFS parser
        │
        ▼
Post-processing (filtering, restriction extraction, label resolution)
        │
        ▼
Store in domain session
        │
        ▼
UI refreshes (entities, relationships, map)
```

After import you can:

- **Edit** entities, relationships, and attributes in the visual designer or form views.
- **Add** new entities or relationships on top of the imported ontology.
- **Map** entities to Databricks tables in the Mapping section.
- **Export** the combined ontology as OWL/Turtle.

---

### Tips

- **Start small**: When importing a large standard like FIBO, start with the required foundation module and one domain to evaluate the result before adding more.
- **Network access**: Industry-standard imports require outbound internet access to fetch modules from their public repositories. If running in Databricks Apps with restricted egress, download the files manually and use the OWL file import instead.
- **Incremental import**: Each import replaces the current ontology. If you need to combine multiple standards, export after each import and merge the OWL files externally.
- **Layout reset**: After importing a large ontology, use **Auto-Layout** in the Model view to arrange entities automatically.
