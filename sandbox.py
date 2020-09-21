import psycopg2
import openpyxl

CONNECTION_PARAMETERS = {
  'dbname': 'postgres',
  'user': 'postgres',
  'password': 'postgrespassword',
  'host': 'localhost',
  'port': '5432'
}

def clear_tables(cur):
  cur.execute("""DELETE FROM student_labels; DELETE FROM students;
    DELETE FROM admins; DELETE FROM colleges; DELETE FROM degree_types;
    DELETE FROM enrollment_statuses; DELETE FROM genders;
    DELETE FROM highschools; DELETE FROM interactions;
    DELETE FROM interaction_types; DELETE FROM labels; DELETE FROM person;
    DELETE FROM registration_statuses;
  """)

def get_types(students, column_name):
  types = set()
  for student in students:
    if student[column_name] is not None:
      types.add(student[column_name])
  return dict(types)

def insert_genders(students, cur):
  genders = get_types(students, 'Gender')
  cur.execute("""
    INSERT INTO genders (types)
    VALUES (%s)
  """, genders)

def insert(cur, table, fields, values):
  cur.execute("""
    INSERT INTO {table} ({fields})
    VALUES ({placeholders})
  """)


if __name__ == '__main__':
  # connect to workbook and open first worksheet
  wb = openpyxl.load_workbook(
    filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
    data_only=True
  )
  ws = wb.active

  # build list of student objects
  headers = [cell.value for cell in ws['1']]
  students = [
    {attribute: cell.value for attribute, cell in zip(headers, row)}
    for row in ws.iter_rows(min_row=2)
  ]

  # connect to postgres db
  with psycopg2.connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      clear_tables(cur)
      
      insert_genders(students, cur)
    
