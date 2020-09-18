import openpyxl
from psycopg2 import connect, sql
from table import Table

print(Table)
wb = openpyxl.load_workbook(
    filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
    data_only=True
)
ws = wb.active

CONNECTION_PARAMETERS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgrespassword',
    'host': 'localhost',
    'port': '5432'
}

highschool_params = {
  'table': 'highschools',
  'fields': [
    {
      'name': 'name',
      'source': 'HighSchoolName'
    },
    {
      'name': 'state',
      'source': 'HighSchoolState'
    }
  ],
  'generate_ids': True
}

with connect(**CONNECTION_PARAMETERS) as conn:
  with conn.cursor() as cur:
    # empty_tables(cur)
    highschools = Table(
        ws, highschool_params['table'], highschool_params['fields'])
    print(highschools._records)