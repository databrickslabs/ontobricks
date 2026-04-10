# University Academic Research Dataset

This dataset simulates a complete academic management system for a university with students, faculty, courses, and research. It contains 12 tables with approximately 3,100 records covering academic programs, enrollments, research projects, publications, and grants.

## Overview

**Industry**: Higher Education  
**Use Case**: University Academic Management / Research Analytics  
**Total Tables**: 12  
**Total Records**: ~3,100

The dataset models the complete academic lifecycle:
- **Student Lifecycle**: Student records, enrollments, grades, transcripts
- **Academic Programs**: Departments, courses, prerequisites, course assignments
- **Faculty**: Faculty profiles, department affiliations, teaching assignments
- **Research**: Projects, publications, grants, collaborations

---

## Data Model Schema

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     UNIVERSITY ACADEMIC RESEARCH MODEL                          │
└─────────────────────────────────────────────────────────────────────────────────┘


                              ┌─────────────────────┐
                              │      STUDENT        │
                              ├─────────────────────┤
                              │ student_id (PK)     │◄────────────────────────┐
                              │ first_name          │                         │
                              │ last_name           │                         │
                              │ email               │                         │
                              │ enrollment_year     │                         │
                              │ major               │                         │
                              │ gpa                 │                         │
                              │ status              │                         │
                              │ date_of_birth       │                         │
                              └──────────┬──────────┘                         │
                                         │                                     │
                                         │ enrolls_in (1:N)                    │
                                         │                                     │
                                         ▼                                     │
┌─────────────────────┐      ┌─────────────────────┐                            │
│     DEPARTMENT      │      │     ENROLLMENT      │                            │
├─────────────────────┤      ├─────────────────────┤                            │
│ department_id (PK)  │      │ enrollment_id (PK)   │                            │
│ department_name     │      │ student_id (FK)─────┼────────────────────────────┘
│ building            │      │ course_id (FK)───────┼──────────┐
│ budget              │      │ semester             │          │
│ head_faculty_id(FK) │      │ year                 │          │
└──────────┬──────────┘      │ grade                │          │
           │                 │ status               │          │
           │                 └──────────────────────┘          │
           │                                                    │
           │ offers (1:N)                                      │
           │                                                    │
           ▼                                                    ▼
┌─────────────────────┐      ┌─────────────────────┐      ┌─────────────────────┐
│       COURSE        │◄─────│ course_prerequisite  │      │  COURSE_ASSIGNMENT   │
├─────────────────────┤      ├─────────────────────┤      ├─────────────────────┤
│ course_id (PK)      │      │ course_id (FK)      │      │ assignment_id (PK)   │
│ department_id (FK)──┼──┐   │ prerequisite_id(FK) │      │ course_id (FK)───────┼──┐
│ course_name         │  │   │ requirement_type   │      │ faculty_id (FK)──────┼──┼──┐
│ course_code         │  │   └─────────────────────┘      │ semester              │  │  │
│ credits             │  │                                │ year                  │  │  │
│ level               │  │                                │ role                  │  │  │
│ max_enrollment      │  │                                └──────────────────────┘  │  │
└─────────────────────┘  │                                                          │  │
                         │                                                          │  │
                         │                                                          │  │
                         │   ┌─────────────────────┐                                  │  │
                         │   │ department_affiliation                              │  │
                         │   ├─────────────────────┤                                  │  │
                         └───│ department_id (FK) │                                  │  │
                             │ faculty_id (FK)─────┼──────────────────────────────────┘  │
                             │ role                │                                     │
                             │ start_date          │   ┌─────────────────────┐            │
                             │ is_primary          │   │      FACULTY        │            │
                             └─────────────────────┘   ├─────────────────────┤            │
                                       ▲                │ faculty_id (PK)    │◄───────────┘
                                       │                │ first_name         │
                                       │                │ last_name          │
                                       │                │ email              │
                                       │                │ title              │
                                       │                │ tenure_status      │
                                       │                │ hire_date          │
                                       │                │ research_area      │
                                       │                └──────────┬─────────┘
                                       │                             │
                                       │                ┌────────────┴────────────┐
                                       │                │                        │
                                       │                │ collaborates (N:M)      │ receives_grant (1:N)
                                       │                │                        │
                                       │                ▼                        ▼
                                       │   ┌─────────────────────┐      ┌─────────────────────┐
                                       │   │ project_collaboration      │    GRANT_AWARD      │
                                       │   ├─────────────────────┤      ├─────────────────────┤
                                       └───│ project_id (FK)     │      │ grant_id (PK)       │
                                           │ faculty_id (FK)     │      │ project_id (FK)     │
                                           │ role                │      │ faculty_id (FK)     │
                                           │ start_date          │      │ grant_name          │
                                           │ hours_per_week      │      │ agency              │
                                           └─────────────────────┘      │ amount              │
                                                   ▲                    │ start_date         │
                                                   │                    │ end_date           │
                                                   │                    │ status             │
                                                   │                    └──────────┬─────────┘
                                                   │                               │
                                                   │                    ┌──────────┴─────────┐
                                                   │                    │                   │
                              ┌─────────────────────┐                    │                   │
                              │  RESEARCH_PROJECT    │◄───────────────────┘                   │
                              ├─────────────────────┤                                        │
                              │ project_id (PK)     │                                        │
                              │ department_id (FK)──┼────────────────────────────────────────┘
                              │ project_name        │
                              │ start_date          │
                              │ end_date            │
                              │ status              │
                              │ funding_amount      │
                              │ funding_source      │
                              └──────────┬──────────┘
                                         │
                                         │ publishes (1:N)
                                         │
                                         ▼
                              ┌─────────────────────┐
                              │    PUBLICATION      │
                              ├─────────────────────┤
                              │ publication_id (PK) │
                              │ project_id (FK)     │
                              │ title                │
                              │ journal              │
                              │ publication_date     │
                              │ doi                  │
                              │ citation_count       │
                              └─────────────────────┘
```

---

## Entity Tables

### 1. Student (`student.csv`)

Core student information including personal details and academic status.

| Column          | Type    | Description                              |
|-----------------|---------|------------------------------------------|
| student_id      | STRING  | Primary Key (e.g., STU00001)             |
| first_name      | STRING  | Student first name                       |
| last_name       | STRING  | Student last name                        |
| email           | STRING  | Email address                            |
| enrollment_year | INTEGER | Year of enrollment                       |
| major           | STRING  | Declared major                           |
| gpa             | DECIMAL | Cumulative GPA                           |
| status          | STRING  | active, graduated, on_leave, withdrawn   |
| date_of_birth   | DATE    | Date of birth                            |

**Rows:** 300

---

### 2. Faculty (`faculty.csv`)

Faculty member information and research profile.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| faculty_id    | STRING  | Primary Key (e.g., FAC00001)            |
| first_name    | STRING  | Faculty first name                       |
| last_name     | STRING  | Faculty last name                        |
| email         | STRING  | Email address                            |
| title         | STRING  | Professor, Associate Professor, etc.    |
| tenure_status | STRING  | tenured, tenure_track, non_tenure       |
| hire_date     | DATE    | Date of hire                             |
| research_area | STRING  | Primary research area                    |

**Rows:** 60

---

### 3. Department (`department.csv`)

Academic department information.

| Column          | Type    | Description                              |
|-----------------|---------|------------------------------------------|
| department_id   | STRING  | Primary Key (e.g., DEPT001)             |
| department_name | STRING  | Department name (e.g., Computer Science)|
| building        | STRING  | Physical building                        |
| budget          | DECIMAL | Annual budget                            |
| head_faculty_id | STRING  | Foreign Key → Faculty (department head)  |

**Rows:** 10

---

### 4. Course (`course.csv`)

Course catalog with department association.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| course_id     | STRING  | Primary Key (e.g., CRS00001)            |
| department_id | STRING  | Foreign Key → Department                 |
| course_name   | STRING  | Full course name                         |
| course_code   | STRING  | Course code (e.g., CS101, BIO201)       |
| credits       | INTEGER | Credit hours                             |
| level         | STRING  | undergraduate, graduate, mixed          |
| max_enrollment| INTEGER | Maximum enrollment capacity             |

**Rows:** 80

---

## Transaction Tables

### 5. Enrollment (`enrollment.csv`)

Student course enrollments with grades.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| enrollment_id | STRING  | Primary Key (e.g., ENR000001)           |
| student_id    | STRING  | Foreign Key → Student                    |
| course_id     | STRING  | Foreign Key → Course                     |
| semester      | STRING  | Fall, Spring, Summer                     |
| year          | INTEGER | Academic year                             |
| grade         | STRING  | A, B+, C, etc.                           |
| status        | STRING  | enrolled, completed, withdrawn, etc.    |

**Rows:** 2,000

---

### 6. Course Assignment (`course_assignment.csv`)

Faculty assignments to courses.

| Column        | Type    | Description                              |
|---------------|---------|------------------------------------------|
| assignment_id | STRING  | Primary Key (e.g., ASN00001)            |
| course_id     | STRING  | Foreign Key → Course                     |
| faculty_id    | STRING  | Foreign Key → Faculty                    |
| semester      | STRING  | Fall, Spring, Summer                     |
| year          | INTEGER | Academic year                             |
| role          | STRING  | instructor, co_instructor, ta, grader   |

**Rows:** 120

---

### 7. Research Project (`research_project.csv`)

Research project records.

| Column         | Type    | Description                              |
|----------------|---------|------------------------------------------|
| project_id     | STRING  | Primary Key (e.g., PRJ00001)            |
| department_id  | STRING  | Foreign Key → Department                 |
| project_name   | STRING  | Project name                             |
| start_date     | DATE    | Project start date                       |
| end_date       | DATE    | Project end date                         |
| status         | STRING  | active, completed, on_hold, planning     |
| funding_amount | DECIMAL | Total funding amount                     |
| funding_source | STRING  | NSF, NIH, DOE, etc.                     |

**Rows:** 40

---

### 8. Publication (`publication.csv`)

Publications linked to research projects.

| Column           | Type    | Description                              |
|------------------|---------|------------------------------------------|
| publication_id   | STRING  | Primary Key (e.g., PUB00001)            |
| project_id       | STRING  | Foreign Key → Research Project           |
| title            | STRING  | Publication title                        |
| journal          | STRING  | Journal name (Nature, Science, etc.)     |
| publication_date | DATE    | Publication date                         |
| doi              | STRING  | Digital Object Identifier                |
| citation_count   | INTEGER | Number of citations                       |

**Rows:** 200

---

### 9. Grant Award (`grant_award.csv`)

Grant funding records.

| Column      | Type    | Description                              |
|-------------|---------|------------------------------------------|
| grant_id    | STRING  | Primary Key (e.g., GRT00001)            |
| project_id  | STRING  | Foreign Key → Research Project           |
| faculty_id  | STRING  | Foreign Key → Faculty (PI)               |
| grant_name  | STRING  | Grant name                               |
| agency      | STRING  | NSF, NIH, DOE, NASA, etc.               |
| amount      | DECIMAL | Grant amount                             |
| start_date  | DATE    | Grant start date                         |
| end_date    | DATE    | Grant end date                           |
| status      | STRING  | active, completed, pending, terminated  |

**Rows:** 50

---

## Relationship Tables

### 10. Project Collaboration (`project_collaboration.csv`)

Faculty collaboration on research projects.

| Column         | Type    | Description                              |
|----------------|---------|------------------------------------------|
| collab_id      | STRING  | Primary Key (e.g., COL00001)            |
| project_id     | STRING  | Foreign Key → Research Project           |
| faculty_id     | STRING  | Foreign Key → Faculty                    |
| role           | STRING  | PI, Co-I, Researcher, Postdoc, etc.     |
| start_date     | DATE    | Collaboration start date                 |
| hours_per_week | DECIMAL | Time commitment per week                 |

**Rows:** 80

---

### 11. Course Prerequisite (`course_prerequisite.csv`)

Course prerequisite relationships.

| Column           | Type   | Description                              |
|------------------|--------|------------------------------------------|
| course_id        | STRING | Foreign Key → Course                     |
| prerequisite_id  | STRING | Foreign Key → Course (prerequisite)      |
| requirement_type| STRING | required, recommended, equivalent        |

**Rows:** 60

---

### 12. Department Affiliation (`department_affiliation.csv`)

Faculty affiliation with departments.

| Column        | Type   | Description                              |
|---------------|--------|------------------------------------------|
| faculty_id    | STRING | Foreign Key → Faculty                    |
| department_id| STRING | Foreign Key → Department                 |
| role          | STRING | Professor, Associate, Adjunct, etc.      |
| start_date   | DATE   | Affiliation start date                   |
| is_primary    | STRING | true/false - primary department         |

**Rows:** 80

---

## Relationships Summary

| Relationship       | Source        | Target           | Cardinality | Description                    |
|--------------------|---------------|------------------|-------------|-------------------------------|
| enrolls_in         | Student       | Enrollment       | 1:N         | Student has enrollments       |
| takes_course       | Enrollment    | Course           | N:1         | Enrollment is for a course    |
| offers             | Department    | Course           | 1:N         | Department offers courses     |
| assigned_to       | Faculty       | Course Assignment| 1:N         | Faculty teaches courses       |
| teaches            | Course Assignment | Course      | N:1         | Assignment is for a course    |
| has_prerequisite   | Course        | Course           | N:M         | Course prerequisites          |
| sponsors           | Department    | Research Project  | 1:N         | Department sponsors projects  |
| publishes          | Research Project | Publication   | 1:N         | Project produces publications |
| receives_grant    | Research Project | Grant Award   | 1:N         | Project receives grants       |
| collaborates       | Faculty       | Research Project  | N:M         | Faculty collaborates on projects |
| affiliated_with    | Faculty       | Department       | N:M         | Faculty affiliated with depts |

---

## Loading the Data

### Prerequisites

1. Upload CSV files to a Unity Catalog Volume
2. Permissions to create schema and tables in the target catalog

### Using the Databricks Notebook

Use the provided notebook `load_data.py` to:

1. Create a schema in Unity Catalog
2. Load all 12 CSV files as tables with proper data types
3. Create useful views for analysis
4. Verify data integrity with test queries

### Quick Start

```python
# 1. Upload CSV files to a Unity Catalog Volume
# 2. Update notebook configuration
catalog = "main"
schema = "academic_research"
volume_name = "university_data"

# 3. Run the notebook - it will create all tables automatically
```

### Views Created

The loader script creates three analytical views:

| View                 | Description                                      |
|----------------------|--------------------------------------------------|
| `vw_student_transcript` | Student enrollments with courses and grades   |
| `vw_faculty_profile`    | Faculty with departments, courses, grants     |
| `vw_research_overview`  | Research projects with publications and funding |

---

## Sample Queries

### Student Transcript

```sql
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.major,
    s.gpa,
    c.course_code,
    c.course_name,
    e.semester,
    e.year,
    e.grade
FROM student s
JOIN enrollment e ON s.student_id = e.student_id
JOIN course c ON e.course_id = c.course_id
WHERE s.student_id = 'STU00001'
ORDER BY e.year, e.semester;
```

### Faculty Research Output

```sql
SELECT 
    f.faculty_id,
    f.first_name || ' ' || f.last_name as faculty_name,
    f.research_area,
    COUNT(DISTINCT p.publication_id) as publication_count,
    SUM(p.citation_count) as total_citations,
    SUM(ga.amount) as total_grant_funding
FROM faculty f
LEFT JOIN project_collaboration pc ON f.faculty_id = pc.faculty_id
LEFT JOIN publication p ON pc.project_id = p.project_id
LEFT JOIN grant_award ga ON f.faculty_id = ga.faculty_id
GROUP BY f.faculty_id, f.first_name, f.last_name, f.research_area;
```

### Course Prerequisites

```sql
SELECT 
    c1.course_code as course,
    c2.course_code as prerequisite,
    cp.requirement_type
FROM course_prerequisite cp
JOIN course c1 ON cp.course_id = c1.course_id
JOIN course c2 ON cp.prerequisite_id = c2.course_id
ORDER BY c1.course_code;
```

### Research Funding by Department

```sql
SELECT 
    d.department_name,
    COUNT(DISTINCT rp.project_id) as project_count,
    SUM(rp.funding_amount) as total_funding,
    COUNT(DISTINCT p.publication_id) as publication_count
FROM department d
JOIN research_project rp ON d.department_id = rp.department_id
LEFT JOIN publication p ON rp.project_id = p.project_id
GROUP BY d.department_name
ORDER BY total_funding DESC;
```

---

## Use Cases for OntoBricks

This dataset is ideal for testing and demonstrating:

### Ontology Modeling
- ✅ **Class Mapping**: Map tables to OWL classes (Student, Faculty, Course, ResearchProject, etc.)
- ✅ **Object Properties**: Model relationships (enrollsIn, teaches, hasPrerequisite, publishes, etc.)
- ✅ **Data Properties**: Scalar values (gpa, email, funding_amount, citation_count, etc.)
- ✅ **Hierarchical Concepts**: Department structure, course levels, research areas

### Knowledge Graph Construction
- ✅ **Student 360**: Build complete student profiles from enrollments and grades
- ✅ **Faculty Profile**: Connect teaching, research, and department affiliations
- ✅ **Research Impact**: Trace projects to publications and citations

### SPARQL Queries
- ✅ **Path Queries**: Find all prerequisites for a course
- ✅ **Aggregation**: Calculate total research funding by department
- ✅ **Pattern Matching**: Identify faculty with high citation counts

---

## Dataset Statistics

| Table                 | Rows   | Type          | Primary Key      |
|-----------------------|--------|---------------|------------------|
| student               | 300    | Core Entity   | student_id       |
| faculty               | 60     | Core Entity   | faculty_id       |
| department            | 10     | Core Entity   | department_id    |
| course                | 80     | Core Entity   | course_id        |
| enrollment            | 2,000  | Transaction   | enrollment_id    |
| course_assignment     | 120    | Transaction   | assignment_id    |
| research_project      | 40     | Core Entity   | project_id       |
| publication           | 200    | Transaction   | publication_id   |
| grant_award           | 50     | Transaction   | grant_id         |
| project_collaboration  | 80     | Relationship  | collab_id       |
| course_prerequisite   | 60     | Relationship  | (course_id, prerequisite_id) |
| department_affiliation| 80     | Relationship  | (faculty_id, department_id) |

**Total:** 12 tables, ~3,100 records

---

## Data Quality Notes

- All IDs use meaningful prefixes (STU, FAC, DEPT, CRS, ENR, etc.) for readability
- Date format: YYYY-MM-DD
- No NULL values in primary keys
- Foreign key references are valid and consistent
- Realistic university data: department names (Computer Science, Biology), course codes (CS101, BIO201)
- Journal names: Nature, Science, PNAS, IEEE Transactions, etc.
- Grant agencies: NSF, NIH, DOE, NASA, DARPA, Sloan Foundation, etc.

---

## Files in This Directory

| File                      | Description                    |
|---------------------------|--------------------------------|
| `student.csv`             | Student records                |
| `faculty.csv`             | Faculty records                |
| `department.csv`          | Department records             |
| `course.csv`              | Course catalog                 |
| `enrollment.csv`          | Student enrollments            |
| `course_assignment.csv`   | Faculty course assignments     |
| `research_project.csv`    | Research projects              |
| `publication.csv`         | Publications                   |
| `grant_award.csv`         | Grant awards                   |
| `project_collaboration.csv`| Project collaborations         |
| `course_prerequisite.csv` | Course prerequisites           |
| `department_affiliation.csv` | Department affiliations    |
| `load_data.py`            | Databricks loader notebook     |
| `generate_data.py`        | Data generation script         |
| `create_databricks_tables.py` | CLI table creation script  |
| `unstructured/business_description.txt` | Lakewood University institutional document |
| `README.md`               | This documentation             |

---

## Regenerating Data

To regenerate the dataset with different parameters:

```bash
cd data/university
python generate_data.py
```

Modify `generate_data.py` to:
- Change record counts
- Adjust date ranges
- Add/remove data fields
- Modify random seed for different data
