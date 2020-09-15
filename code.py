import openpyxl
import psycopg2

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

INSERT_HIGHSCHOOLS_SQL = """
  INSERT INTO Highschools (id, name, address, city, state)
  VALUES (%s, %s, %s, %s, %s)
"""

HIGHSCHOOL_FIELDS = [
  'HighSchoolName',
  # 'HighSchoolAddress',
  # 'HighSchoolCity',
  'HighSchoolState'
]

# PERSONS = [
#   (1, 'Larry', 'Smith', 'larry@smith.com'),
#   (2, 'Stephen', 'Smith', 'stephen@smith.com')
# ]

def get_active_worksheet(path):
  # Open workbook as read-only
  wb = openpyxl.load_workbook(filename=path, data_only=True)

  # Get first worksheet
  return wb.active

# def get_data(ws):
#   # Get column names
#   fields = list(ws.values)[0]

#   # Generate dict from records
#   data = [
#     {field: value for (field, value) in zip(fields, row)}
#     for row in ws.values
#   ]

#   # remove column names
#   data.pop(0)
#   return data


def get_unique_records(ws, column_name):
  uniques = set()

  for col in ws.columns:
    if col[0].value == column_name:
      col_number = col[0].column
      for row in ws.iter_rows(min_row=2, min_col=col_number, max_col=col_number):
        uniques.add(row[0].value)

  return list(uniques)


# Student Id, FirstName, LastName
# -- Iterate across each field name
# -- For each field, find its col number
# -- Iterate across rows
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


# Iterate across rows
def get_unique(ws, fields, unique):
  result = []
  uniqs = set()
  unique_field_column_number = None

  for row in ws.iter_rows(min_row=1, max_row=1):
    for cell in row:
      if cell.value == unique:
        unique_field_column_number = cell.column


  for row in ws.iter_rows(min_row=2):
    unique_field_value = ws.cell(row=row[0].row, column=unique_field_column_number).value
    if (unique_field_value in uniqs):
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

if __name__ == "__main__":
  # Open Gradsnapp data
  ws = get_active_worksheet('fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx')

  highschools = get_unique(ws, fields=HIGHSCHOOL_FIELDS, unique='HighSchoolName')

  # Connect to DB
  with psycopg2.connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      highschool_id = 1
      for highschool in highschools:
        cur.execute(INSERT_HIGHSCHOOLS_SQL, (
          highschool_id,
          highschool['HighSchoolName'],
          '',
          '',
          highschool['HighSchoolState']
        ))
        highschool_id += 1

      
      # Insert students into Person table
      # person_id = 1
      # for student in students:
      #   # Add person_id to each student record
      #   student['person_id'] = person_id
      #   # Insert student into DB
      #   cur.execute(INSERT_PERSON_SQL, (
      #     person_id,
      #     # Empty strings won't be needed when we have full student data
      #     student['FirstName'] or '',
      #     student['LastName'] or '',
      #     student['Email'] or ''
      #   ))
      #   person_id += 1
