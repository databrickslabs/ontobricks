# Examples

These walkthroughs take you from raw tables to an explorable knowledge graph. Each one can be completed in 15–30 minutes.

| Example | Difficulty | What you'll learn |
|---------|-----------|-------------------|
| [Family Tree](#example-family-tree-ontology-mapping) | Beginner | One entity, two relationships — the simplest end-to-end flow |
| [Customer Journey](#example-customer-journey-ontology-energy-provider) | Intermediate | 10 tables, complex relationships, visual Designer, advanced SPARQL |

---

## Example: Family Tree Ontology Mapping

### What you'll build

A knowledge graph of family relationships (parent–child links) starting from a single CSV table. By the end you will have an OWL ontology, an R2RML mapping, and a navigable graph in the Digital Twin view.

### Dataset

#### CSV Data

The `family.csv` file contains family relationships:

```csv
person_id,name,gender,father_id,mother_id
p1,John Smith,M,,
p2,Mary Johnson,F,,
p3,Robert Smith,M,p1,p2
p4,Emily Smith,F,p1,p2
p5,Sarah Davis,F,,
p6,Michael Smith,M,p3,p5
p7,Lisa Smith,F,p3,p5
```

#### Database Schema

```sql
CREATE TABLE person (
    person_id STRING PRIMARY KEY,
    name STRING,
    gender STRING,
    father_id STRING REFERENCES person(person_id),
    mother_id STRING REFERENCES person(person_id)
);
```

### Target Ontology

We'll map this data to a simple family ontology:

```turtle
@prefix family: <http://example.org/family#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

family:Person a rdfs:Class ;
    rdfs:label "Person" .

family:hasFather a rdfs:Property ;
    rdfs:domain family:Person ;
    rdfs:range family:Person .

family:hasMother a rdfs:Property ;
    rdfs:domain family:Person ;
    rdfs:range family:Person .

foaf:name a rdfs:Property .
foaf:gender a rdfs:Property .
```

### Mapping Steps

#### Step 1: Configure Connection

1. Open OntoBricks in your browser
2. Navigate to Configuration
3. Enter your Databricks credentials:
   ```
   Host: https://your-workspace.cloud.databricks.com
   Token: dapi...
   Warehouse ID: abc123...
   ```
4. Test connection

#### Step 2: Load Data Source

1. Go to **Mapping** page
2. Click **Designer** in the sidebar
3. Enter:
   ```
   Catalog: main
   Schema: default
   ```
3. Click "Load Tables"
4. You should see `person` in the table list

#### Step 3: Create Entity Mapping

1. Click on the **Person** entity in the Designer view
2. Fill in the form:
   ```
   Table: person
   Class URI: http://example.org/family#Person
   ID Column: person_id
   ```
3. Add property mappings:
   ```
   Column: name
   Property URI: http://xmlns.com/foaf/0.1/name
   
   Column: gender
   Property URI: http://xmlns.com/foaf/0.1/gender
   ```
4. Click **Save Mapping**

#### Step 4: Create Relationship Mappings

##### Father Relationship

1. Click on the **hasFather** relationship in the Designer view
2. Fill in:
   ```
   Source Table: person
   Source Column: father_id
   Target Table: person
   Target Column: person_id
   Property URI: http://example.org/family#hasFather
   ```
3. Save

##### Mother Relationship

1. Click on the **hasMother** relationship in the Designer view
2. Fill in:
   ```
   Source Table: person
   Source Column: mother_id
   Target Table: person
   Target Column: person_id
   Property URI: http://example.org/family#hasMother
   ```
3. Save

#### Step 5: View R2RML Output

1. Navigate to **Domain** → **Export**
2. The R2RML mapping is auto-generated
3. Copy or download as needed
4. Review the generated R2RML
5. Click "Download .ttl" to save

### Generated R2RML

The complete R2RML mapping:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.org/> .
@prefix family: <http://example.org/family#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

## Person Class Mapping
ex:PersonTriplesMap a rr:TriplesMap ;
    rr:logicalTable [ 
        rr:tableName "main.default.person" 
    ] ;
    
    rr:subjectMap [
        rr:template "http://example.org/person/{person_id}" ;
        rr:class family:Person
    ] ;
    
    # Name property
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant foaf:name ] ;
        rr:objectMap [ rr:column "name" ]
    ] ;
    
    # Gender property
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant foaf:gender ] ;
        rr:objectMap [ rr:column "gender" ]
    ] .

## Father Relationship Mapping
ex:FatherRelationshipMap a rr:TriplesMap ;
    rr:logicalTable [ 
        rr:tableName "main.default.person" 
    ] ;
    
    rr:subjectMap [
        rr:template "http://example.org/person/{person_id}"
    ] ;
    
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant family:hasFather ] ;
        rr:objectMap [
            rr:parentTriplesMap ex:PersonTriplesMap ;
            rr:joinCondition [
                rr:child "father_id" ;
                rr:parent "person_id"
            ]
        ]
    ] .

## Mother Relationship Mapping
ex:MotherRelationshipMap a rr:TriplesMap ;
    rr:logicalTable [ 
        rr:tableName "main.default.person" 
    ] ;
    
    rr:subjectMap [
        rr:template "http://example.org/person/{person_id}"
    ] ;
    
    rr:predicateObjectMap [
        rr:predicateMap [ rr:constant family:hasMother ] ;
        rr:objectMap [
            rr:parentTriplesMap ex:PersonTriplesMap ;
            rr:joinCondition [
                rr:child "mother_id" ;
                rr:parent "person_id"
            ]
        ]
    ] .
```

### Generated RDF Triples

When the R2RML mapping is executed, it will produce RDF triples like:

```turtle
@prefix ex: <http://example.org/> .
@prefix family: <http://example.org/family#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:person/p1 a family:Person ;
    foaf:name "John Smith" ;
    foaf:gender "M" .

ex:person/p2 a family:Person ;
    foaf:name "Mary Johnson" ;
    foaf:gender "F" .

ex:person/p3 a family:Person ;
    foaf:name "Robert Smith" ;
    foaf:gender "M" ;
    family:hasFather ex:person/p1 ;
    family:hasMother ex:person/p2 .

ex:person/p4 a family:Person ;
    foaf:name "Emily Smith" ;
    foaf:gender "F" ;
    family:hasFather ex:person/p1 ;
    family:hasMother ex:person/p2 .

ex:person/p5 a family:Person ;
    foaf:name "Sarah Davis" ;
    foaf:gender "F" .

ex:person/p6 a family:Person ;
    foaf:name "Michael Smith" ;
    foaf:gender "M" ;
    family:hasFather ex:person/p3 ;
    family:hasMother ex:person/p5 .

ex:person/p7 a family:Person ;
    foaf:name "Lisa Smith" ;
    foaf:gender "F" ;
    family:hasFather ex:person/p3 ;
    family:hasMother ex:person/p5 .
```

### SPARQL Queries (External API)

The following SPARQL queries can be executed programmatically via the external REST API (`/api/v1/query`). In the OntoBricks UI, triple store data is explored visually through the Digital Twin section without writing queries manually:

#### Query 1: Find all people

```sparql
PREFIX family: <http://example.org/family#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name ?gender
WHERE {
    ?person a family:Person ;
            foaf:name ?name ;
            foaf:gender ?gender .
}
```

#### Query 2: Find John Smith's children

```sparql
PREFIX family: <http://example.org/family#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex: <http://example.org/>

SELECT ?child ?name
WHERE {
    ?child family:hasFather ex:person/p1 ;
           foaf:name ?name .
}
```

#### Query 3: Find grandchildren of John and Mary

```sparql
PREFIX family: <http://example.org/family#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex: <http://example.org/>

SELECT ?grandchild ?name
WHERE {
    ?child family:hasFather ex:person/p1 .
    ?grandchild family:hasFather ?child ;
                foaf:name ?name .
}
```

#### Query 4: Find all parent-child relationships

```sparql
PREFIX family: <http://example.org/family#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?parentName ?childName ?relationship
WHERE {
    {
        ?child family:hasFather ?parent .
        BIND("father" AS ?relationship)
    } UNION {
        ?child family:hasMother ?parent .
        BIND("mother" AS ?relationship)
    }
    ?parent foaf:name ?parentName .
    ?child foaf:name ?childName .
}
ORDER BY ?parentName ?childName
```

### Using the R2RML Mapping

#### With Ontop

```bash
## Install Ontop
wget https://github.com/ontop/ontop/releases/download/ontop-5.0.0/ontop-cli-5.0.0.zip
unzip ontop-cli-5.0.0.zip

## Configure connection
cat > family.properties <<EOF
jdbc.url=jdbc:spark://your-databricks-host:443/default
jdbc.driver=com.simba.spark.jdbc.Driver
jdbc.user=token
jdbc.password=your-token
EOF

## Run query
./ontop-cli-5.0.0/ontop query \
  --mapping=family-mapping.ttl \
  --properties=family.properties \
  --query=query.sparql
```

#### With D2RQ

```bash
## Install D2RQ
wget https://github.com/d2rq/d2rq/releases/download/v0.8.1/d2rq-0.8.1.zip
unzip d2rq-0.8.1.zip

## Start D2RQ server
./d2rq-0.8.1/d2r-server family-mapping.ttl
```

### Next Steps

1. **Extend the ontology**: Add more properties (birthDate, birthPlace, etc.)
2. **Add more relationships**: siblings, grandparents, cousins
3. **Include lifecycle events**: births, marriages, deaths
4. **Connect to other ontologies**: Use FOAF, Schema.org
5. **Build a visualization**: Create a family tree visualization from the RDF data

### Summary

This example demonstrated:
- Configuring OntoBricks with Databricks
- Creating class mappings for ontology classes
- Defining relationship mappings for object properties
- Generating W3C-compliant R2RML
- Exploring the resulting knowledge graph through the Digital Twin Knowledge Graph

The same approach can be applied to any relational database schema and target ontology.


---

## Example: Customer Journey Ontology (Energy Provider)

### What you'll build

A full customer-journey knowledge graph for an energy provider — 10 interconnected tables covering customers, contracts, meters, readings, invoices, payments, calls, claims, and interactions. This example exercises the visual Designer, entity and relationship mapping, and the Knowledge Graph explorer.

### Dataset

#### Source Tables

The dataset is located in `data/customer/` and includes:

| Table | Records | Description |
|-------|---------|-------------|
| customer | 200 | Core customer information |
| contract | 300 | Energy supply contracts |
| subscription | 350 | Pricing plans and tariffs |
| meter | 400 | Physical meters |
| meter_reading | 1,000 | Consumption readings |
| invoice | 800 | Billing records |
| payment | 700 | Payment transactions |
| call | 300 | Customer service calls |
| claim | 150 | Customer complaints |
| interaction | 500 | General interactions |

#### Loading the Data

```bash
cd data/customer
python generate_data.py              # Generate CSV files
python create_databricks_tables.py   # Load into Databricks
```

#### Data Model

```
                              CUSTOMER
                                 │
            ┌────────────────────┼────────────────────┐
            │                    │                    │
            ▼                    ▼                    ▼
        CONTRACT               CALL               CLAIM
            │                                        │
    ┌───────┼───────┐                               │
    │       │       │                               │
    ▼       ▼       ▼                               │
SUBSCRIPTION METER INVOICE ◄────────────────────────┘
                │      │
                ▼      ▼
         METER_READING PAYMENT
```

---

### Step 1: Design the Ontology

#### Using the Visual Designer

1. Open OntoBricks and go to **Ontology → Design**
2. Create the following entities:

##### Core Entities

| Entity | Icon | Attributes |
|--------|------|------------|
| Customer | 👤 | email, phone, city, segment, loyalty_points |
| Contract | 📄 | energy_type, status, monthly_fee, payment_method |
| Subscription | 📋 | plan_type, price_per_kwh, price_per_m3, green_energy |
| Meter | 🔌 | meter_serial, meter_type, energy_type, location |

##### Transaction Entities

| Entity | Icon | Attributes |
|--------|------|------------|
| MeterReading | 📊 | reading_value, unit, reading_type, validated |
| Invoice | 💰 | amount_ht, vat_amount, amount_ttc, status |
| Payment | 💳 | amount, payment_method, status |

##### Interaction Entities

| Entity | Icon | Attributes |
|--------|------|------------|
| ServiceCall | 📞 | duration_seconds, reason, satisfaction_score |
| Claim | ⚠️ | claim_type, priority, status, compensation_amount |
| Interaction | 💬 | channel, interaction_type, sentiment |

#### Creating Relationships

Draw relationships between entities:

| Relationship | From | To | Direction |
|--------------|------|-----|-----------|
| hasContract | Customer | Contract | Forward (→) |
| hasSubscription | Contract | Subscription | Forward (→) |
| hasMeter | Contract | Meter | Forward (→) |
| hasReading | Meter | MeterReading | Forward (→) |
| hasInvoice | Contract | Invoice | Forward (→) |
| hasPayment | Invoice | Payment | Forward (→) |
| madeCall | Customer | ServiceCall | Forward (→) |
| filedClaim | Customer | Claim | Forward (→) |
| hasInteraction | Customer | Interaction | Forward (→) |

#### Auto-Layout

Click **Auto Layout** to organize the diagram, then **Center** to fit the view.

---

### Step 2: Map Data Sources

#### Entity Mappings

Go to **Mapping → Map** and click on each entity to map:

##### Customer Entity

```sql
SELECT 
    customer_id,
    first_name || ' ' || last_name as name,
    email,
    phone,
    city,
    segment,
    loyalty_points
FROM your_catalog.your_schema.customer
WHERE is_active = 'true'
```

- **ID Column**: `customer_id`
- **Label Column**: `name`

##### Contract Entity

```sql
SELECT 
    contract_id,
    customer_id,
    energy_type,
    status,
    monthly_fee,
    payment_method
FROM your_catalog.your_schema.contract
WHERE status = 'active'
```

- **ID Column**: `contract_id`

##### Meter Entity

```sql
SELECT 
    meter_id,
    contract_id,
    meter_serial,
    meter_type,
    energy_type,
    location
FROM your_catalog.your_schema.meter
WHERE status = 'active'
```

- **ID Column**: `meter_id`

#### Relationship Mappings

In **Mapping → Map**, click on each relationship to map:

##### hasContract

```sql
SELECT customer_id, contract_id
FROM your_catalog.your_schema.contract
```

- **Source Column**: `customer_id`
- **Target Column**: `contract_id`

##### hasMeter

```sql
SELECT contract_id, meter_id
FROM your_catalog.your_schema.meter
```

- **Source Column**: `contract_id`
- **Target Column**: `meter_id`

---

### Step 3: Explore the Data (Digital Twin)

In the OntoBricks UI, navigate to the **Digital Twin** page and click **Synchronize** to generate triples, then explore the knowledge graph visually. The SPARQL queries below illustrate the underlying data model and can be used via the external REST API (`/api/v1/query`).

#### Example Queries (External API)

##### Find all customers with their contracts

```sparql
PREFIX ont: <https://databricks-ontology.com/CustomerJourney#>

SELECT ?customer ?name ?contract ?energyType
WHERE {
    ?customer a ont:Customer .
    ?customer ont:name ?name .
    ?customer ont:hasContract ?contract .
    ?contract ont:energy_type ?energyType .
}
LIMIT 50
```

##### Find customers with electricity contracts

```sparql
PREFIX ont: <https://databricks-ontology.com/CustomerJourney#>

SELECT ?customer ?name ?city
WHERE {
    ?customer a ont:Customer .
    ?customer ont:name ?name .
    ?customer ont:city ?city .
    ?customer ont:hasContract ?contract .
    ?contract ont:energy_type "electricity" .
}
```

#### Advanced Queries

##### Customer 360 View

```sparql
PREFIX ont: <https://databricks-ontology.com/CustomerJourney#>

SELECT ?customer ?name ?segment 
       (COUNT(DISTINCT ?contract) as ?contracts)
       (COUNT(DISTINCT ?call) as ?calls)
WHERE {
    ?customer a ont:Customer .
    ?customer ont:name ?name .
    ?customer ont:segment ?segment .
    OPTIONAL { ?customer ont:hasContract ?contract }
    OPTIONAL { ?customer ont:madeCall ?call }
}
GROUP BY ?customer ?name ?segment
```

---

### Knowledge Graph

After synchronization, switch to the **Knowledge Graph** tab to see an interactive graph:

- **Nodes**: Represent entities (Customer, Contract, Meter, etc.) with emoji icons
- **Edges**: Represent relationships (hasContract, hasMeter, etc.)
- **Click**: Select a node to see all its attributes, values, and relationships in the detail panel
- **Find**: Search for specific entities by name or URI
- **Filters**: Narrow down the graph by entity type, field, and relationship depth

---

### Reference

- **Dataset Documentation**: [data/customer/README.md](../data/customer/README.md)
- **Data Model Diagram**: See the ASCII diagram in the dataset README

---

*This example uses the Customer Journey dataset included with OntoBricks.*
