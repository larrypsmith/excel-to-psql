import openpyxl
import psycopg2
from faker import Faker
fake = Faker()

CONNECTION_PARAMETERS = {
  'dbname': 'postgres',
  'user': 'postgres',
  'password': 'postgrespassword',
  'host': 'localhost',
  'port': '5432'
}

def get_all(ws, *fields):
  ans = []
  for row in ws.iter_rows(min_row=2):
    relation = {}
    for cell in row:
      field = ws.cell(row=1, column=cell.column).value
      if field in fields:
        relation[field] = cell.value
    ans.append(relation)
  return ans

def get_column_number(ws, field_name):
  for row in ws.iter_rows(min_row=1, max_row=1):
    for cell in row:
      if cell.value == field_name:
        return cell.column
  return None

def get_unique(ws, fields, unique):
  result = []
  uniqs = set()
  unique_field_column_number = get_column_number(ws, unique)

  for row in ws.iter_rows(min_row=2):
    unique_field_value = ws.cell(row=row[0].row, column=unique_field_column_number).value
    if (unique_field_value in uniqs or unique_field_value is None):
      continue
    record = {}
    for cell in row:
      field = ws.cell(row=1, column=cell.column).value
      if field in fields:
        record[field] = cell.value
        if field == unique:
          uniqs.add(cell.value)
    result.append(record)
  
  return result

def get_unique_labels(ws):
  uniqs = set()
  label_col_number = get_column_number(ws, 'Labels')
  for row in ws.iter_rows(min_row=2, min_col=label_col_number, max_col=label_col_number):
    cell = row[0]
    labels = cell.value.split("; ")
    for label in labels:
      uniqs.add((label.strip()))
  return list(uniqs)

def main():
  # Open Gradsnapp data
  wb = openpyxl.load_workbook(
    filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
    data_only=True
  )
  ws = wb.active

  # Connect to DB
  with psycopg2.connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      
      # insert highschools
      highschools = get_unique(
        ws, fields=['HighSchoolName','HighSchoolState'], unique='HighSchoolName'
      )
      highschool_id = 1
      for highschool in highschools:
        cur.execute("""
          INSERT INTO highschools (id, name, state)
          VALUES (%s, %s, %s)
        """, (
          highschool_id,
          highschool['HighSchoolName'],
          highschool['HighSchoolState']
        ))
        highschool_id += 1

      # insert colleges
      colleges = get_unique(
        ws,
        fields=['CollegeName','CollegeCity','CollegeState'],
        unique='CollegeName'
      )
      college_id = 1
      for college in colleges:
        cur.execute("""
          INSERT INTO colleges (id, name, city, state)
          VALUES (%s, %s, %s, %s)
        """, (
          college_id,
          college['CollegeName'],
          college['CollegeCity'],
          college['CollegeState']
        ))
        college_id += 1 

      # insert genders
      genders = get_unique(ws, fields=['Gender'], unique='Gender')
      for gender in genders:
        cur.execute("""
          INSERT INTO genders (type)
          VALUES (%s)
        """, (gender['Gender'],))

      # insert labels
      labels = get_unique_labels(ws)
      for label in labels:
        cur.execute("""
          INSERT INTO labels (type)
          VALUES (%s)
        """, (label,))

      # insert degree types
      degree_types = get_unique(
        ws, fields=['PlannedDegreeType'], unique='PlannedDegreeType'
      )
      for degree_type in degree_types:
        cur.execute("""
          INSERT INTO degree_types
          VALUES (%s)
        """,
        (degree_type['PlannedDegreeType'],))

      # insert enrollment statuses
      enrollment_statuses = get_unique(ws, fields=['Status'], unique='Status')
      for status in enrollment_statuses:
        cur.execute("""
          INSERT INTO enrollment_statuses
          VALUES (%s)
        """,
        (status['Status'],))

      # insert registration statuses
      registration_statuses = get_unique(
        ws,
        fields=['RegistrationStatus'],
        unique='RegistrationStatus'
      )
      for reg_status in registration_statuses:
        cur.execute("""
          INSERT INTO registration_statuses
          VALUES (%s)
        """,
        (reg_status['RegistrationStatus'],))

      # insert students into person table
      student_persons = get_all(ws, 'FirstName', 'LastName', 'Email')
      person_id = 1
      for person in student_persons:
        cur.execute("""
          INSERT INTO person (id, first_name, last_name, email)
          VALUES (%s, %s, %s, %s)
        """, (
          person_id,
          person['FirstName'] or fake.first_name(),
          person['LastName'] or fake.last_name(),
          person['Email'] or fake.email()
        ))
        person_id += 1

if __name__ == '__main__':
  main()