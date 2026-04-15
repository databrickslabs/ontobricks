#!/usr/bin/env python3
"""
Generate University Academic Research Dataset
Creates 12 tables with realistic data for a university (students, faculty, courses, research).
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

# Seed for reproducibility
random.seed(42)

# Output directory
OUTPUT_DIR = Path(__file__).parent

# Helper functions
def random_date(start_year, end_year):
    """Generate a random date between start_year and end_year."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

# Data pools
FIRST_NAMES = [
    "James", "Emma", "Michael", "Olivia", "William", "Ava", "Alexander", "Sophia",
    "Benjamin", "Isabella", "Lucas", "Mia", "Henry", "Charlotte", "Daniel", "Amelia",
    "Matthew", "Harper", "Joseph", "Evelyn", "David", "Abigail", "Samuel", "Emily",
    "Christopher", "Elizabeth", "Andrew", "Sofia", "Joshua", "Avery", "Ethan", "Ella",
    "Nathan", "Scarlett", "Ryan", "Grace", "Jacob", "Chloe", "Nicholas", "Victoria",
    "Thomas", "Riley", "Charles", "Aria", "George", "Lily", "Robert", "Aurora",
    "John", "Zoey", "Edward", "Penelope", "Oliver", "Layla", "Sebastian", "Nora",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]

DEPARTMENTS = [
    ("Computer Science", "Engineering Hall", "DEPT001"),
    ("Biology", "Science Building", "DEPT002"),
    ("Mathematics", "Math Tower", "DEPT003"),
    ("Physics", "Science Building", "DEPT004"),
    ("Chemistry", "Science Building", "DEPT005"),
    ("Psychology", "Social Sciences Hall", "DEPT006"),
    ("Economics", "Business School", "DEPT007"),
    ("English", "Humanities Building", "DEPT008"),
    ("History", "Humanities Building", "DEPT009"),
    ("Electrical Engineering", "Engineering Hall", "DEPT010"),
]

MAJORS = [
    "Computer Science", "Biology", "Mathematics", "Physics", "Chemistry",
    "Psychology", "Economics", "English", "History", "Electrical Engineering",
    "Data Science", "Neuroscience", "Political Science", "Sociology", "Art History",
]

COURSE_CODES = {
    "Computer Science": ["CS101", "CS201", "CS301", "CS401", "CS501", "CS102", "CS202", "CS302"],
    "Biology": ["BIO101", "BIO201", "BIO301", "BIO401", "BIO102", "BIO202", "BIO302"],
    "Mathematics": ["MATH101", "MATH201", "MATH301", "MATH401", "MATH102", "MATH202"],
    "Physics": ["PHYS101", "PHYS201", "PHYS301", "PHYS401", "PHYS102", "PHYS202"],
    "Chemistry": ["CHEM101", "CHEM201", "CHEM301", "CHEM401", "CHEM102", "CHEM202"],
    "Psychology": ["PSY101", "PSY201", "PSY301", "PSY401", "PSY102", "PSY202"],
    "Economics": ["ECON101", "ECON201", "ECON301", "ECON401", "ECON102", "ECON202"],
    "English": ["ENG101", "ENG201", "ENG301", "ENG401", "ENG102", "ENG202"],
    "History": ["HIST101", "HIST201", "HIST301", "HIST401", "HIST102", "HIST202"],
    "Electrical Engineering": ["EE101", "EE201", "EE301", "EE401", "EE102", "EE202"],
}

JOURNALS = [
    "Nature", "Science", "Cell", "PNAS", "PLOS ONE", "Journal of the ACM",
    "IEEE Transactions", "Physical Review Letters", "Chemical Reviews",
    "Psychological Science", "American Economic Review", "The Lancet",
]

GRANT_AGENCIES = ["NSF", "NIH", "DOE", "DARPA", "NASA", "DOE-ARPA-E", "Sloan Foundation", "Moore Foundation"]

FACULTY_TITLES = ["Professor", "Associate Professor", "Assistant Professor", "Lecturer", "Research Professor"]
TENURE_STATUS = ["tenured", "tenure_track", "non_tenure"]
RESEARCH_AREAS = [
    "Machine Learning", "Computational Biology", "Quantum Computing", "Climate Science",
    "Neuroscience", "Materials Science", "Social Psychology", "Macroeconomics",
    "Literary Theory", "Ancient History", "Signal Processing", "Organic Chemistry",
]

SEMESTERS = ["Fall", "Spring", "Summer"]
YEARS = [2020, 2021, 2022, 2023, 2024]
GRADES = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", "W", "I"]
ENROLLMENT_STATUS = ["enrolled", "completed", "withdrawn", "in_progress"]
STUDENT_STATUS = ["active", "graduated", "on_leave", "withdrawn"]
REQUIREMENT_TYPES = ["required", "recommended", "equivalent"]
COLLAB_ROLES = ["Principal Investigator", "Co-Investigator", "Researcher", "Postdoc", "Graduate Assistant"]
AFFILIATION_ROLES = ["Professor", "Associate", "Adjunct", "Visiting", "Emeritus"]
PROJECT_STATUS = ["active", "completed", "on_hold", "planning"]
GRANT_STATUS = ["active", "completed", "pending", "terminated"]


def generate_students(count=300):
    """Generate student records."""
    students = []
    for i in range(1, count + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        students.append({
            "student_id": f"STU{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}{i % 100}@university.edu",
            "enrollment_year": random.choice(YEARS[:-1]),
            "major": random.choice(MAJORS),
            "gpa": round(random.uniform(2.0, 4.0), 2),
            "status": random.choice(STUDENT_STATUS),
            "date_of_birth": random_date(1995, 2006),
        })
    return students


def generate_faculty(count=60):
    """Generate faculty records."""
    faculty = []
    for i in range(1, count + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        faculty.append({
            "faculty_id": f"FAC{i:05d}",
            "first_name": first,
            "last_name": last,
            "email": f"{first.lower()}.{last.lower()}@university.edu",
            "title": random.choice(FACULTY_TITLES),
            "tenure_status": random.choice(TENURE_STATUS),
            "hire_date": random_date(1995, 2023),
            "research_area": random.choice(RESEARCH_AREAS),
        })
    return faculty


def generate_departments(faculty):
    """Generate department records."""
    departments = []
    for i, (name, building, dept_id) in enumerate(DEPARTMENTS):
        head = random.choice(faculty)
        departments.append({
            "department_id": dept_id,
            "department_name": name,
            "building": building,
            "budget": round(random.uniform(500000, 5000000), 2),
            "head_faculty_id": head["faculty_id"],
        })
    return departments


def generate_courses(departments, count=80):
    """Generate course records."""
    courses = []
    for i in range(1, count + 1):
        dept = random.choice(departments)
        dept_name = dept["department_name"]
        codes = COURSE_CODES.get(dept_name, ["XXX101", "XXX201", "XXX301"])
        code = random.choice(codes)
        courses.append({
            "course_id": f"CRS{i:05d}",
            "department_id": dept["department_id"],
            "course_name": f"{code} - {random.choice(['Introduction to', 'Advanced', 'Topics in'])} {dept_name}",
            "course_code": code,
            "credits": random.choice([3, 4]),
            "level": random.choice(["undergraduate", "graduate", "mixed"]),
            "max_enrollment": random.choice([30, 50, 75, 100, 150]),
        })
    return courses


def generate_enrollments(students, courses, count=2000):
    """Generate enrollment records."""
    enrollments = []
    for i in range(1, count + 1):
        student = random.choice(students)
        course = random.choice(courses)
        year = random.choice(YEARS)
        semester = random.choice(SEMESTERS)
        key = (student["student_id"], course["course_id"], semester, year)
        enrollments.append({
            "enrollment_id": f"ENR{i:06d}",
            "student_id": student["student_id"],
            "course_id": course["course_id"],
            "semester": semester,
            "year": year,
            "grade": random.choice(GRADES),
            "status": random.choice(ENROLLMENT_STATUS),
        })
    return enrollments


def generate_course_assignments(courses, faculty, count=120):
    """Generate course assignment records."""
    assignments = []
    for i in range(1, count + 1):
        course = random.choice(courses)
        fac = random.choice(faculty)
        year = random.choice(YEARS)
        semester = random.choice(SEMESTERS)
        assignments.append({
            "assignment_id": f"ASN{i:05d}",
            "course_id": course["course_id"],
            "faculty_id": fac["faculty_id"],
            "semester": semester,
            "year": year,
            "role": random.choice(["instructor", "co_instructor", "ta", "grader"]),
        })
    return assignments


def generate_research_domains(departments, count=40):
    """Generate research domain records (research programs / grants units)."""
    domains = []
    for i in range(1, count + 1):
        dept = random.choice(departments)
        start = random_date(2018, 2023)
        end_dt = datetime.strptime(start, "%Y-%m-%d") + timedelta(days=random.randint(365, 1825))
        end = end_dt.strftime("%Y-%m-%d")
        domains.append({
            "domain_id": f"PRJ{i:05d}",
            "department_id": dept["department_id"],
            "domain_name": f"Research in {random.choice(RESEARCH_AREAS)} - {dept['department_name']}",
            "start_date": start,
            "end_date": end,
            "status": random.choice(PROJECT_STATUS),
            "funding_amount": round(random.uniform(50000, 2000000), 2),
            "funding_source": random.choice(GRANT_AGENCIES),
        })
    return domains


def generate_publications(domains, count=200):
    """Generate publication records."""
    publications = []
    for i in range(1, count + 1):
        domain = random.choice(domains)
        pub_date = random_date(2019, 2025)
        publications.append({
            "publication_id": f"PUB{i:05d}",
            "domain_id": domain["domain_id"],
            "title": f"Findings from {domain['domain_name'][:30]}...",
            "journal": random.choice(JOURNALS),
            "publication_date": pub_date,
            "doi": f"10.1234/uni.{i:05d}",
            "citation_count": random.randint(0, 500),
        })
    return publications


def generate_grant_awards(domains, faculty, count=50):
    """Generate grant award records."""
    grants = []
    for i in range(1, count + 1):
        domain = random.choice(domains)
        fac = random.choice(faculty)
        start = random_date(2019, 2024)
        end_dt = datetime.strptime(start, "%Y-%m-%d") + timedelta(days=random.randint(365, 1460))
        grants.append({
            "grant_id": f"GRT{i:05d}",
            "domain_id": domain["domain_id"],
            "faculty_id": fac["faculty_id"],
            "grant_name": f"{random.choice(GRANT_AGENCIES)} Grant - {domain['domain_name'][:25]}",
            "agency": random.choice(GRANT_AGENCIES),
            "amount": round(random.uniform(50000, 1500000), 2),
            "start_date": start,
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "status": random.choice(GRANT_STATUS),
        })
    return grants


def generate_domain_collaborations(domains, faculty, count=80):
    """Generate domain collaboration records (faculty on research domains)."""
    collabs = []
    for i in range(1, count + 1):
        domain = random.choice(domains)
        fac = random.choice(faculty)
        collabs.append({
            "collab_id": f"COL{i:05d}",
            "domain_id": domain["domain_id"],
            "faculty_id": fac["faculty_id"],
            "role": random.choice(COLLAB_ROLES),
            "start_date": random_date(2019, 2024),
            "hours_per_week": round(random.uniform(2, 20), 1),
        })
    return collabs


def generate_course_prerequisites(courses, count=60):
    """Generate course prerequisite records."""
    prereqs = []
    course_list = list(courses)
    seen = set()
    attempts = 0
    while len(prereqs) < count and attempts < count * 3:
        course = random.choice(course_list)
        prereq = random.choice([c for c in course_list if c["course_id"] != course["course_id"]])
        key = (course["course_id"], prereq["course_id"])
        if key not in seen:
            seen.add(key)
            prereqs.append({
                "course_id": course["course_id"],
                "prerequisite_id": prereq["course_id"],
                "requirement_type": random.choice(REQUIREMENT_TYPES),
            })
        attempts += 1
    return prereqs


def generate_department_affiliations(faculty, departments, count=80):
    """Generate department affiliation records."""
    affiliations = []
    for i in range(1, count + 1):
        fac = random.choice(faculty)
        dept = random.choice(departments)
        affiliations.append({
            "faculty_id": fac["faculty_id"],
            "department_id": dept["department_id"],
            "role": random.choice(AFFILIATION_ROLES),
            "start_date": random_date(1995, 2023),
            "is_primary": random.choice(["true", "true", "false"]),
        })
    return affiliations


def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Created {filename} ({len(data)} records)")


def main():
    print("=" * 60)
    print("Generating University Academic Research Dataset")
    print("=" * 60)
    print()

    print("Generating data...")
    students = generate_students(300)
    faculty = generate_faculty(60)
    departments = generate_departments(faculty)
    courses = generate_courses(departments, 80)
    enrollments = generate_enrollments(students, courses, 2000)
    course_assignments = generate_course_assignments(courses, faculty, 120)
    research_domains = generate_research_domains(departments, 40)
    publications = generate_publications(research_domains, 200)
    grant_awards = generate_grant_awards(research_domains, faculty, 50)
    domain_collaborations = generate_domain_collaborations(research_domains, faculty, 80)
    course_prerequisites = generate_course_prerequisites(courses, 60)
    department_affiliations = generate_department_affiliations(faculty, departments, 80)

    print()
    print("Writing CSV files...")

    write_csv("student.csv", students, [
        "student_id", "first_name", "last_name", "email", "enrollment_year",
        "major", "gpa", "status", "date_of_birth"
    ])
    write_csv("faculty.csv", faculty, [
        "faculty_id", "first_name", "last_name", "email", "title",
        "tenure_status", "hire_date", "research_area"
    ])
    write_csv("department.csv", departments, [
        "department_id", "department_name", "building", "budget", "head_faculty_id"
    ])
    write_csv("course.csv", courses, [
        "course_id", "department_id", "course_name", "course_code",
        "credits", "level", "max_enrollment"
    ])
    write_csv("enrollment.csv", enrollments, [
        "enrollment_id", "student_id", "course_id", "semester", "year", "grade", "status"
    ])
    write_csv("course_assignment.csv", course_assignments, [
        "assignment_id", "course_id", "faculty_id", "semester", "year", "role"
    ])
    write_csv("research_domain.csv", research_domains, [
        "domain_id", "department_id", "domain_name", "start_date", "end_date",
        "status", "funding_amount", "funding_source"
    ])
    write_csv("publication.csv", publications, [
        "publication_id", "domain_id", "title", "journal", "publication_date",
        "doi", "citation_count"
    ])
    write_csv("grant_award.csv", grant_awards, [
        "grant_id", "domain_id", "faculty_id", "grant_name", "agency",
        "amount", "start_date", "end_date", "status"
    ])
    write_csv("domain_collaboration.csv", domain_collaborations, [
        "collab_id", "domain_id", "faculty_id", "role", "start_date", "hours_per_week"
    ])
    write_csv("course_prerequisite.csv", course_prerequisites, [
        "course_id", "prerequisite_id", "requirement_type"
    ])
    write_csv("department_affiliation.csv", department_affiliations, [
        "faculty_id", "department_id", "role", "start_date", "is_primary"
    ])

    print()
    print("=" * 60)
    print("✅ Dataset generation complete!")
    print("=" * 60)

    all_data = [students, faculty, departments, courses, enrollments, course_assignments,
                research_domains, publications, grant_awards, domain_collaborations,
                course_prerequisites, department_affiliations]
    total = sum(len(d) for d in all_data)
    print(f"\n📊 Summary:")
    print(f"   • student.csv:            {len(students):5d} records")
    print(f"   • faculty.csv:            {len(faculty):5d} records")
    print(f"   • department.csv:         {len(departments):5d} records")
    print(f"   • course.csv:             {len(courses):5d} records")
    print(f"   • enrollment.csv:         {len(enrollments):5d} records")
    print(f"   • course_assignment.csv:  {len(course_assignments):5d} records")
    print(f"   • research_domain.csv:    {len(research_domains):5d} records")
    print(f"   • publication.csv:        {len(publications):5d} records")
    print(f"   • grant_award.csv:        {len(grant_awards):5d} records")
    print(f"   • domain_collaboration.csv: {len(domain_collaborations):5d} records")
    print(f"   • course_prerequisite.csv: {len(course_prerequisites):5d} records")
    print(f"   • department_affiliation.csv: {len(department_affiliations):5d} records")
    print(f"   ─────────────────────────────────")
    print(f"   Total:                    {total:5d} records")


if __name__ == "__main__":
    main()
