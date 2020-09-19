import openpyxl
from psycopg2 import connect, sql
from table import Table
from worksheet import Worksheet
from relation import Relation

wb = openpyxl.load_workbook(
  filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
  data_only=True
)
worksheet = wb.active

CONNECTION_PARAMETERS = {
  'dbname': 'postgres',
  'user': 'postgres',
  'password': 'postgrespassword',
  'host': 'localhost',
  'port': '5432'
}

def empty_tables(cur):
  cur.execute("""
    DELETE FROM student_labels;
    DELETE FROM students;
    DELETE FROM admins;
    DELETE FROM colleges;
    DELETE FROM degree_types;
    DELETE FROM enrollment_statuses;
    DELETE FROM genders;
    DELETE FROM highschools;
    DELETE FROM interactions;
    DELETE FROM interaction_types;
    DELETE FROM labels;
    DELETE FROM person;
    DELETE FROM registration_statuses;
  """)

with connect(**CONNECTION_PARAMETERS) as conn:
  with conn.cursor() as cur:
    empty_tables(cur)
    ws = Worksheet(worksheet)

    # genders
    genders_data = ws.get_values('Gender')
    genders_relation = Relation(genders_data, ['type'])
    genders_relation.insert(cur, 'genders')

    # highschools
    highschools_data = ws.get_values('HighSchoolName', 'HighSchoolState')
    highschools_relation = Relation(highschools_data, ['name', 'state'])
    highschools_relation.add_ids()
    highschools_relation.insert(cur, 'highschools')

    # colleges
    colleges_data = ws.get_values('CollegeName', 'CollegeCity', 'CollegeState')
    colleges_relation = Relation(colleges_data, ['name', 'city', 'state'])
    colleges_relation.add_ids()
    colleges_relation.insert(cur, 'colleges')

    # degree types
    degree_types_data = ws.get_values('PlannedDegreeType')
    degree_types_relation = Relation(degree_types_data, ['type'])
    degree_types_relation.insert(cur, 'degree_types')

    # enrollment statuses
    enrollment_statuses_data = ws.get_values('Status')
    enrollment_statuses_relation = Relation(enrollment_statuses_data, ['type'])
    enrollment_statuses_relation.insert(cur, 'enrollment_statuses')

    # labels
    labels_data = ws.get_values('Labels')
    uniqs = set()
    for labels in labels_data:
      labs = labels[0].split("; ")
      for label in labs:
        uniqs.add(label.strip())
    labels_data = [[label] for label in list(uniqs)]
    labels_relation = Relation(labels_data, ['type'])
    labels_relation.insert(cur, 'labels')

    # student persons
    students_data = ws.get_values('Student Id', 'Year', 'Status',
      'Gender', 'Phone', 'HighSchoolName', 'AcademicScore', 'PostHsPlans',
      'RegistrationStatus', 'Major', 'PlannedDegreeType',
      'ExpectedGraduationYear', 'CollegeName',)
    students_relation = Relation(students_data, [
      'id',
      'year',
      'enrollment_status',
      'gender',
      'phone',
      'highschool_name',
      'hs_acadmic_score',
      'post_hs_plans',
      'registration_status',
      'major',
      'degree_type',
      'expected_graduation_year',
      'college_name'
    ])