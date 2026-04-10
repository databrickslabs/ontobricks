# Sample Dataset - Data Model

This dataset provides sample data for testing OntoBricks ontology mapping capabilities with Unity Catalog.

## Overview

The dataset models an organizational structure with **3 entities** and **3 types of relationships**:
- **Entities**: Person, Department, Project
- **Relationships**: 
  - Person → Department (one-directional, many-to-one)
  - Department → Project (one-directional, many-to-many)
  - Person ↔ Person (bi-directional, many-to-many)

---

## Data Model Schema

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           ORGANIZATIONAL MODEL                          │
└─────────────────────────────────────────────────────────────────────────┘


                         ┌──────────────────────┐
                    ┌────┤      Department      │
                    │    ├──────────────────────┤
                    │    │ department_id (PK)   │◄─────────┐
                    │    │ department_name      │          │
                    │    │ location             │          │
                    │    │ manager_id (FK)      │          │
                    │    │ budget               │          │
                    │    └──────────┬───────────┘          │
                    │               │                       │
                    │               │                       │
         manages    │               │ sponsors             │
                    │               │ (N:M)                │
                    │               │                       │
                    │               ▼                       │
                    │    ┌──────────────────────┐          │
                    │    │ department_project   │          │
                    │    ├──────────────────────┤          │
                    │    │ department_id (FK)   │──────────┘
                    │    │ project_id (FK)      │──────────┐
                    │    │ sponsorship_type     │          │
                    │    │ funding_amount       │          │
                    │    │ start_date           │          │
                    │    └──────────────────────┘          │
                    │                                       │
                    │                                       ▼
                    │                            ┌──────────────────────┐
                    │                            │       Project        │
                    │                            ├──────────────────────┤
                    │                            │ project_id (PK)      │
                    │                            │ project_name         │
                    │                            │ start_date           │
                    │                            │ end_date             │
                    │                            │ budget               │
                    │                            │ status               │
                    │                            │ description          │
                    │                            └──────────────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │       Person         │◄─────────┐
         ├──────────────────────┤          │
         │ person_id (PK)       │          │
         │ first_name           │          │
         │ last_name            │          │
         │ email                │          │
         │ job_title            │          │
         │ hire_date            │          │
         │ salary               │          │
         └──────┬───────────────┘          │
                │                           │
                │ belongs_to                │
                │ (N:1)                     │
                │                           │
                ▼                           │
     ┌──────────────────────┐              │
     │ person_department    │              │
     ├──────────────────────┤              │
     │ person_id (FK)       │──────────────┘
     │ department_id (FK)   │──────────────┐
     │ role_in_dept         │              │
     │ assignment_date      │              │
     └──────────────────────┘              │
                                            │
                                            ▼
                                 ┌──────────────────────┐
                                 │      Department      │
                                 └──────────────────────┘
                                 (reference shown above)


              ┌──────────────────────────────────────────────┐
              │         person_collaboration                 │
              │      (Bi-directional Relationship)           │
              ├──────────────────────────────────────────────┤
   ┌──────────┤ person_id_1 (FK)                            │
   │          │ person_id_2 (FK)                            │───────┐
   │          │ collaboration_type                           │       │
   │          │ project_id (FK)                              │       │
   │          │ start_date                                   │       │
   │          │ hours_per_week                               │       │
   │          └──────────────────────────────────────────────┘       │
   │                                                                  │
   │                      Person ◄─────────────────────────────────►Person
   └──────────────────────────────────────────────────────────────────┘
                         (symmetric relationship)
```

---

## Entities

### 1. Person (`person.csv`)
Represents employees in the organization.

| Column      | Type    | Description                    |
|-------------|---------|--------------------------------|
| person_id   | STRING  | Primary Key (e.g., P001)       |
| first_name  | STRING  | Employee first name            |
| last_name   | STRING  | Employee last name             |
| email       | STRING  | Employee email address         |
| job_title   | STRING  | Job title/position             |
| hire_date   | DATE    | Date of hire                   |
| salary      | DECIMAL | Annual salary                  |

**Rows:** 12

---

### 2. Department (`department.csv`)
Represents organizational departments.

| Column          | Type    | Description                        |
|-----------------|---------|-------------------------------------|
| department_id   | STRING  | Primary Key (e.g., D001)           |
| department_name | STRING  | Department name                     |
| location        | STRING  | Physical location                   |
| budget          | DECIMAL | Annual department budget            |
| manager_id      | STRING  | Foreign Key → Person (department manager) |

**Rows:** 5

---

### 3. Project (`project.csv`)
Represents company projects.

| Column       | Type    | Description                    |
|--------------|---------|--------------------------------|
| project_id   | STRING  | Primary Key (e.g., PR001)      |
| project_name | STRING  | Project name                   |
| start_date   | DATE    | Project start date             |
| end_date     | DATE    | Project end date               |
| budget       | DECIMAL | Project budget                 |
| status       | STRING  | Status (Active, Completed, etc.)|
| description  | STRING  | Project description            |

**Rows:** 7

---

## Relationships

### 1. Person → Department (One-Directional, Many-to-One)
**File:** `person_department.csv`

Defines which department each person belongs to. Many persons can belong to one department.

| Column          | Type   | Description                        |
|-----------------|--------|------------------------------------|
| person_id       | STRING | Foreign Key → Person               |
| department_id   | STRING | Foreign Key → Department           |
| role_in_dept    | STRING | Role within the department         |
| assignment_date | DATE   | Date assigned to department        |

**Rows:** 12  
**Direction:** Person → Department (forward in OntoBricks)  
**Cardinality:** Many-to-One (N:1)

---

### 2. Department → Project (One-Directional, Many-to-Many)
**File:** `department_project.csv`

Defines which departments sponsor which projects. A department can sponsor multiple projects, and a project can have multiple department sponsors.

| Column           | Type    | Description                           |
|------------------|---------|---------------------------------------|
| department_id    | STRING  | Foreign Key → Department              |
| project_id       | STRING  | Foreign Key → Project                 |
| sponsorship_type | STRING  | Type (Primary, Supporting)            |
| funding_amount   | DECIMAL | Amount of funding provided            |
| start_date       | DATE    | Sponsorship start date                |

**Rows:** 10  
**Direction:** Department → Project (forward in OntoBricks)  
**Cardinality:** Many-to-Many (N:M)

---

### 3. Person ↔ Person (Bi-Directional, Many-to-Many)
**File:** `person_collaboration.csv`

Defines collaboration relationships between employees. This is a **symmetric, bi-directional** relationship: if Person A collaborates with Person B, then Person B also collaborates with Person A.

| Column             | Type    | Description                           |
|--------------------|---------|---------------------------------------|
| person_id_1        | STRING  | Foreign Key → Person                  |
| person_id_2        | STRING  | Foreign Key → Person                  |
| collaboration_type | STRING  | Type of collaboration                 |
| project_id         | STRING  | Foreign Key → Project (context)       |
| start_date         | DATE    | Collaboration start date              |
| hours_per_week     | DECIMAL | Time commitment per week              |

**Rows:** 12  
**Direction:** Bi-directional (↔ in OntoBricks)  
**Cardinality:** Many-to-Many (N:M)  
**Property:** Symmetric (if A→B then B→A)

---

## Mapping to OntoBricks

### Using the Visual Designer

1. Open **Ontology** → **Design**
2. Create entities:
   - **Person** (👤): Add attributes `email`, `job_title`, `salary`
   - **Department** (🏢): Add attributes `departmentName`, `location`, `budget`
   - **Project** (📋): Add attributes `projectName`, `budget`, `status`
3. Create relationships:
   - Drag from Person to Department → name "worksIn" → set direction to **Forward** (→)
   - Drag from Department to Project → name "sponsors" → set direction to **Forward** (→)
   - Drag from Person to Person → name "collaboratesWith" → set direction to **Bidirectional** (↔)
4. Click **Auto Layout** to organize
5. Click **Center** to fit the view

### Relationship Direction Guide

| Relationship | Source | Target | Direction Setting |
|--------------|--------|--------|-------------------|
| worksIn | Person | Department | Forward (→) |
| sponsors | Department | Project | Forward (→) |
| collaboratesWith | Person | Person | Bidirectional (↔) |

---

## Loading the Data

Use the provided notebook `load_sample_data.ipynb` to:
1. Create a schema in Unity Catalog
2. Automatically discover CSV files in a Volume
3. Create tables with proper data types
4. Verify data integrity

### Quick Start

```python
# 1. Upload CSV files to a Unity Catalog Volume
# 2. Update notebook configuration
catalog = "your_catalog"
schema = "your_schema"
volume_name = "sample_data"

# 3. Run the notebook - it will create all tables automatically
```

---

## Sample Queries

### Find all employees in a department
```sql
SELECT p.first_name, p.last_name, p.job_title, d.department_name
FROM person p
JOIN person_department pd ON p.person_id = pd.person_id
JOIN department d ON pd.department_id = d.department_id
WHERE d.department_name = 'Data Engineering';
```

### Find all projects with their sponsors
```sql
SELECT p.project_name, d.department_name, dp.sponsorship_type, dp.funding_amount
FROM project p
JOIN department_project dp ON p.project_id = dp.project_id
JOIN department d ON dp.department_id = d.department_id
ORDER BY p.project_name;
```

### Find collaboration partners
```sql
SELECT 
    p1.first_name || ' ' || p1.last_name as person_1,
    p2.first_name || ' ' || p2.last_name as person_2,
    pc.collaboration_type,
    pc.hours_per_week
FROM person_collaboration pc
JOIN person p1 ON pc.person_id_1 = p1.person_id
JOIN person p2 ON pc.person_id_2 = p2.person_id;
```

---

## Use Cases for OntoBricks

This dataset is ideal for testing:
- ✅ **Class Mapping**: Map tables to OWL classes (Person, Department, Project)
- ✅ **Forward Object Properties**: worksIn (Person → Department), sponsors (Department → Project)
- ✅ **Bi-Directional Object Properties**: collaboratesWith (Person ↔ Person)
- ✅ **Relationship Attributes**: Properties on relationships (role, funding_amount)
- ✅ **Data Properties**: Scalar values (name, email, salary, budget, etc.)
- ✅ **R2RML Generation**: Generate RDF mappings with direction support
- ✅ **Knowledge Graph**: Build a semantic graph from relational data
- ✅ **Visual Design**: Use OntoViz to create the ontology visually

---

## Dataset Statistics

| Table                   | Rows | Type         | Direction       |
|-------------------------|------|--------------|-----------------|
| person                  | 12   | Entity       | -               |
| department              | 5    | Entity       | -               |
| project                 | 7    | Entity       | -               |
| person_department       | 12   | Relationship | Forward (→)     |
| department_project      | 10   | Relationship | Forward (→)     |
| person_collaboration    | 12   | Relationship | Bidirectional (↔) |

**Total:** 6 tables, 58 rows

---

## Notes

- All IDs use string identifiers (P001, D001, PR001) for readability
- Date format: YYYY-MM-DD
- No NULL values in primary or foreign keys
- Person collaboration is intentionally symmetric/bi-directional
- Department.manager_id references Person (manager is also an employee)
- Relationship directions in OntoBricks match the semantic meaning of the data
