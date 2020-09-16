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

INSERT_PERSON_SQL = """
  INSERT INTO Person (id, first_name, last_name, email)
  VALUES (%s, %s, %s, %s)
"""

INSERT_HIGHSCHOOL_SQL = """
  INSERT INTO highschools (id, name, state)
  VALUES (%s, %s, %s)
"""

INSERT_COLLEGE_SQL = """
  INSERT INTO colleges (id, name, city, state)
  VALUES (%s, %s, %s, %s)
"""

INSERT_GENDER_SQL = """
  INSERT INTO genders (type)
  VALUES (%s)
"""

INSERT_LABEL_SQL = """
  INSERT INTO labels (type)
  VALUES (%s)
"""

HIGHSCHOOL_FIELDS = [
  'HighSchoolName',
  'HighSchoolState'
]

COLLEGE_FIELDS = [
  'CollegeName',
  'CollegeCity',
  'CollegeState'
]

GENDER_FIELDS = [
  'Gender'
]

LABEL_FIELDS = [
  'Labels'
]

DEGREE_TYPE_FIELDS = [
  'PlannedDegreeType'
]

def get_active_worksheet(path):
  # Open workbook as read-only
  wb = openpyxl.load_workbook(filename=path, data_only=True)

  # Get first worksheet
  return wb.active

def get_data(ws, *fields):
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

# Iterate across rows
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


if __name__ == "__main__":
  # Open Gradsnapp data
  ws = get_active_worksheet('fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx')

  highschools = get_unique(ws, fields=HIGHSCHOOL_FIELDS, unique='HighSchoolName')
  colleges = get_unique(ws, fields=COLLEGE_FIELDS, unique='CollegeName')
  genders = get_unique(ws, fields=GENDER_FIELDS, unique='Gender')
  labels = get_unique_labels(ws)
  degree_types = get_unique(ws, fields=DEGREE_TYPE_FIELDS, unique='PlannedDegreeType')

  # Connect to DB
  with psycopg2.connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      
      # insert highschools into Highschools table
      highschool_id = 1
      for highschool in highschools:
        cur.execute(INSERT_HIGHSCHOOL_SQL, (
          highschool_id,
          highschool['HighSchoolName'],
          highschool['HighSchoolState']
        ))
        highschool_id += 1

      # insert colleges into Colleges table
      college_id = 1
      for college in colleges:
        cur.execute(INSERT_COLLEGE_SQL, (
          college_id,
          college['CollegeName'],
          college['CollegeCity'],
          college['CollegeState']
        ))
        college_id += 1 

      # insert genders into Genders tables
      for gender in genders:
        value = gender['Gender']
        cur.execute(INSERT_GENDER_SQL, (
          value,
        ))

      # insert labels into Labels table
      for label in labels:
        cur.execute(INSERT_LABEL_SQL, (
          label,
        ))

      # insert degree types into DegreeTypes table
      for degree_type in degree_types:
        cur.execute("""
          INSERT INTO degree_types
          VALUES (%s)
        """,
        (degree_type['PlannedDegreeType'],))

      # insert students into person table
