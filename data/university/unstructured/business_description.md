# Lakewood University

## Institutional Overview and Academic Data Management

**Internal Institutional Document**

| | |
|---|---|
| **Document Owner** | Office of Institutional Research |
| **Classification** | Internal Use Only |
| **Version** | 2.0 |
| **Last Updated** | March 2025 |
| **Review Cycle** | Annual |
| **Next Review** | March 2026 |

---

## Table of Contents

1. [Institutional Profile](#1-institutional-profile)
2. [Academic Programs and Student Body](#2-academic-programs-and-student-body)
   - 2.1 [Departments and Programs](#21-departments-and-programs)
   - 2.2 [Student Records and Enrollment](#22-student-records-and-enrollment)
   - 2.3 [Course Prerequisites and Curriculum](#23-course-prerequisites-and-curriculum)
3. [Faculty and Course Delivery](#3-faculty-and-course-delivery)
   - 3.1 [Faculty Appointments](#31-faculty-appointments)
   - 3.2 [Department Affiliations](#32-department-affiliations)
   - 3.3 [Course Assignments](#33-course-assignments)
4. [Research Initiatives and Funding](#4-research-initiatives-and-funding)
   - 4.1 [Research Projects](#41-research-projects)
   - 4.2 [Publications and Scholarly Output](#42-publications-and-scholarly-output)
   - 4.3 [Grant Awards](#43-grant-awards)
   - 4.4 [Project Collaborations](#44-project-collaborations)
5. [Academic Management Challenges](#5-academic-management-challenges)
6. [Data Model Summary](#6-data-model-summary)
7. [Key Performance Indicators](#7-key-performance-indicators)
8. [Appendix: Entity Relationship Overview](#8-appendix-entity-relationship-overview)

---

## 1. Institutional Profile

Lakewood University is a comprehensive research university founded in **1892**, located in the Pacific Northwest. The institution is classified as a **Carnegie R2** (Doctoral Universities — High Research Activity) institution and holds full regional accreditation.

> **Our Mission:** Excellence in teaching, advancement of knowledge through research, and service to the community and profession.

### University at a Glance

| Metric | Value |
|---|---|
| Founded | 1892 |
| Location | Pacific Northwest, USA |
| Carnegie Classification | R2 — High Research Activity |
| Student Population | ~3,500 (undergraduate and graduate) |
| Academic Departments | 10 |
| Full-time Faculty | ~60 |
| Active Research Projects | 40+ |
| Annual Grant Funding | $12M+ |
| Degree Levels | Bachelor's, Master's, Doctoral |

---

## 2. Academic Programs and Student Body

### 2.1 Departments and Programs

Lakewood offers degrees across the arts, sciences, engineering, and humanities through ten academic departments:

| Department | Building | Focus Areas |
|---|---|---|
| Computer Science | Engineering Hall | AI, Systems, Software Engineering |
| Biology | Science Building | Molecular Biology, Ecology, Genetics |
| Mathematics | Science Building | Pure Math, Applied Math, Statistics |
| Physics | Science Building | Theoretical, Experimental, Astrophysics |
| Chemistry | Science Building | Organic, Inorganic, Analytical |
| Psychology | Behavioral Sciences | Clinical, Cognitive, Developmental |
| Economics | Social Sciences | Micro, Macro, Econometrics |
| English | Humanities Building | Literature, Rhetoric, Creative Writing |
| History | Humanities Building | American, European, World History |
| Electrical Engineering | Engineering Hall | Circuits, Signal Processing, Embedded Systems |

Each department operates under an annual budget allocated by central administration and maintains its own faculty, curriculum, and research agenda.

### 2.2 Student Records and Enrollment

The student information system maintains comprehensive records for every enrolled student:

| Field | Description |
|---|---|
| Student ID | Unique institutional identifier |
| Personal Information | Name, email, date of birth |
| Enrollment Year | Year of first matriculation |
| Declared Major | Primary department affiliation |
| Cumulative GPA | Running grade point average |
| Status | Active, Graduated, On Leave, or Withdrawn |

#### Enrollment Lifecycle

Students progress through course enrollments that record:

- **Course** — The specific course taken
- **Semester and Year** — When the course was taken
- **Grade** — A through F, or Pass/Fail
- **Status** — Enrolled, Completed, Withdrawn, In Progress

| Enrollment Metric | Value |
|---|---|
| Active Enrollments per Semester | ~2,000 |
| Average Courses per Student | 4–5 per semester |
| Completion Rate | Tracked per department |

### 2.3 Course Prerequisites and Curriculum

Courses are characterized by:

| Attribute | Example |
|---|---|
| Course Code | CS101, BIO201, MATH301 |
| Course Name | Introduction to Computer Science |
| Credit Hours | 3–4 |
| Level | Undergraduate, Graduate, or Mixed |
| Max Enrollment | Varies by course and facility |
| Department | Curriculum owner |

The **course prerequisite** relationship is critical for curriculum design and student advising. For example:

- CS201 (Data Structures) requires CS101 (Intro to CS)
- MATH301 (Real Analysis) requires MATH201 (Linear Algebra)
- BIO201 (Genetics) requires BIO101 (Intro to Biology)

| Prerequisite Data | Value |
|---|---|
| Prerequisite Rules Defined | 60 |
| Courses with Prerequisites | ~50% of catalog |

---

## 3. Faculty and Course Delivery

### 3.1 Faculty Appointments

Faculty members are the core of Lakewood's academic enterprise. Each record includes:

| Field | Description |
|---|---|
| Faculty ID | Unique identifier |
| Name & Email | Contact information |
| Title | Professor, Associate, Assistant, Lecturer, Research Professor |
| Tenure Status | Tenured, Tenure Track, Non-tenure |
| Hire Date | Start of appointment |
| Research Area | Primary scholarly focus |

### Faculty Distribution by Title

| Title | Typical Count | Tenure Status |
|---|---|---|
| Professor | ~15 | Tenured |
| Associate Professor | ~15 | Tenured or Tenure Track |
| Assistant Professor | ~15 | Tenure Track |
| Lecturer | ~10 | Non-tenure |
| Research Professor | ~5 | Varies |

### 3.2 Department Affiliations

Faculty may hold appointments in one or more departments through the **department affiliation** relationship:

| Role | Description |
|---|---|
| Professor | Primary departmental appointment |
| Associate | Secondary or joint appointment |
| Adjunct | Part-time or visiting capacity |
| Visiting | Temporary cross-department appointment |
| Emeritus | Retired faculty with continued affiliation |

Each affiliation records a **start date** and whether it is the faculty member's **primary** department.

| Affiliation Data | Value |
|---|---|
| Total Affiliations | ~80 |
| Faculty with Multiple Affiliations | ~30% |

### 3.3 Course Assignments

Course delivery is managed through the **course assignment** system, linking faculty to courses for a given semester:

| Role | Description |
|---|---|
| Instructor | Primary course leader |
| Co-instructor | Shared teaching responsibility |
| Teaching Assistant | Graduate student support |
| Grader | Assessment and evaluation support |

| Assignment Data | Value |
|---|---|
| Course Assignments per Semester | ~120 |
| Average Teaching Load | 2–3 courses per faculty |

---

## 4. Research Initiatives and Funding

### 4.1 Research Projects

Research is central to Lakewood's institutional identity. Projects are initiated and led by faculty, often in collaboration with colleagues across departments.

| Field | Description |
|---|---|
| Project Name | Descriptive title |
| Department | Hosting academic unit |
| Status | Active, Completed, On Hold, Planning |
| Duration | Start and end dates |
| Total Funding | Aggregate amount from all sources |
| Primary Funding Source | Lead sponsor |

### Funding Sources

| Agency | Focus Areas |
|---|---|
| NSF | Science and engineering research |
| NIH | Biomedical and health sciences |
| DOE | Energy and physical sciences |
| DARPA | Defense-related research |
| NASA | Space and aerospace research |
| Private Foundations | Various interdisciplinary areas |

| Research Metric | Value |
|---|---|
| Active Research Projects | ~40 |
| Average Project Duration | 2–4 years |
| Total Annual Research Funding | $12M+ |

### 4.2 Publications and Scholarly Output

Research projects generate publications recorded with:

- **Title** — Publication name
- **Journal** — Peer-reviewed venue
- **Publication Date** — Date of publication
- **DOI** — Digital Object Identifier
- **Citation Count** — Tracked for impact measurement

| Publication Metric | Value |
|---|---|
| Total Publications | ~200 |
| Average Citations per Paper | Varies by discipline |
| Publications per Faculty (avg.) | 3–5 |

### 4.3 Grant Awards

Grant awards represent external funding secured for projects:

| Field | Description |
|---|---|
| Grant Name | Title of the award |
| Funding Agency | Sponsoring organization |
| Amount | Dollar value of the award |
| Principal Investigator | Lead faculty member |
| Duration | Start and end dates |
| Status | Active, Completed, Pending, Terminated |

| Grant Metric | Value |
|---|---|
| Total Grant Awards | ~50 |
| Median Grant Size | $200K–$500K |
| Grant Success Rate | ~25% of submissions |

### 4.4 Project Collaborations

The many-to-many relationship between faculty and research projects is captured through **project collaborations**:

| Role | Description |
|---|---|
| Principal Investigator | Overall project lead |
| Co-Investigator | Senior research collaborator |
| Researcher | Contributing faculty member |
| Postdoc | Postdoctoral research associate |
| Graduate Assistant | Doctoral or master's student |

Each collaboration records **hours per week** committed and a **start date**, supporting effort reporting and conflict-of-interest disclosure.

| Collaboration Metric | Value |
|---|---|
| Active Collaborations | ~80 |
| Average Collaborators per Project | 2–3 |

---

## 5. Academic Management Challenges

Lakewood faces several cross-cutting challenges that require integrated data views:

### Challenge 1: Student Success

| Need | Data Sources |
|---|---|
| At-risk student identification | Enrollments, Grades, GPA trends |
| Prerequisite enforcement | Courses, Prerequisites, Enrollment history |
| Degree audit and graduation clearance | Student records, Course completions |
| Transcript generation | Enrollments with grades, Courses |

### Challenge 2: Faculty Workload and Productivity

| Need | Data Sources |
|---|---|
| Teaching load balancing | Course Assignments, Semesters |
| Research productivity assessment | Publications, Citations, Grants |
| Promotion and tenure dossier | All faculty activity data |
| Cross-department coordination | Department Affiliations |

### Challenge 3: Research Impact and Funding

| Need | Data Sources |
|---|---|
| Publication and citation analytics | Publications, Research Projects |
| Grant lifecycle management | Grant Awards, Project status |
| Compliance and effort reporting | Collaborations, Hours per week |
| Strategic research planning | Projects, Funding trends |

### Challenge 4: Data Integrity

| Need | Description |
|---|---|
| Referential integrity | Students → valid courses, Faculty → valid departments |
| Orphan prevention | No enrollments without valid student and course |
| Consistency checks | GPA calculations match enrolled grades |
| Audit trail | Change tracking across all records |

---

## 6. Data Model Summary

### Core Entities

| Entity | Description | Approx. Records |
|---|---|---|
| Student | Enrolled learners | 300 |
| Faculty | Academic staff | 60 |
| Department | Academic units | 10 |
| Course | Course catalog | 80 |

### Relationship Entities

| Entity | Description | Approx. Records |
|---|---|---|
| Enrollment | Student × Course registrations | 2,000 |
| Course Assignment | Faculty × Course teaching roles | 120 |
| Course Prerequisite | Course dependency rules | 60 |
| Department Affiliation | Faculty × Department memberships | 80 |

### Research Entities

| Entity | Description | Approx. Records |
|---|---|---|
| Research Project | Faculty-led research initiatives | 40 |
| Publication | Scholarly output | 200 |
| Grant Award | External funding | 50 |
| Project Collaboration | Faculty × Project participation | 80 |

**Total records: ~3,080**

---

## 7. Key Performance Indicators

| KPI | Target | Scope |
|---|---|---|
| 4-year Graduation Rate | ≥ 75% | Undergraduate |
| Student Retention (Year 1 → 2) | ≥ 88% | Undergraduate |
| Average Class Size | 25–35 | All courses |
| Faculty-to-Student Ratio | 1:18 | University-wide |
| Research Expenditure per Faculty | ≥ $200K/year | Tenure-track faculty |
| Publications per Faculty (annual) | ≥ 2.0 | Tenure-track faculty |
| Grant Success Rate | ≥ 25% | All submissions |
| Course Completion Rate | ≥ 92% | All enrollments |

---

## 8. Appendix: Entity Relationship Overview

### Primary Relationships

```
Student ──(N:1)──► Department (major)
Student ──(1:N)──► Enrollment
Course ──(1:N)──► Enrollment
Course ──(N:1)──► Department
Faculty ──(1:N)──► Course Assignment
Course ──(1:N)──► Course Assignment
Faculty ──(1:N)──► Department Affiliation
Department ──(1:N)──► Department Affiliation
Course ──(1:N)──► Course Prerequisite
Department ──(1:N)──► Research Project
Faculty ──(1:N)──► Project Collaboration
Research Project ──(1:N)──► Project Collaboration
Research Project ──(1:N)──► Publication
Research Project ──(1:N)──► Grant Award
Faculty ──(1:N)──► Grant Award (PI)
```

### Analytical Views

- **Student Transcript:** Student → Enrollment → Course (with grades, semesters)
- **Faculty Profile:** Faculty → Course Assignments + Publications + Grants + Department Affiliations
- **Research Overview:** Research Project → Publications + Grant Awards + Collaborations

---

*This document supports ontology mapping and knowledge graph construction using OntoBricks. The academic data model and relationships are designed to be directly translatable into RDF/OWL ontology structures for semantic querying and institutional analytics.*
