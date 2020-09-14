import openpyxl
import psycopg2

# Local development
# CONNECTION_PARAMETERS = {
#   'host': 'postgres',
#   'dbname': 'postgres',
#   'user': 'postgres',
#   'password': 'postgrespassword',
#   'port': '5432'
# }

# with psycopg2.connect(**CONNECTION_PARAMETERS) as conn:
#   with conn.cursor() as curs:
#     print(curs.execute('SELECT * FROM INFORMATION_SCHEMA.TABLES'))

def get_active_worksheet(path):
  # Open workbook as read-only
  wb = openpyxl.load_workbook(filename=path, read_only=True, data_only=True)

  # Get first worksheet
  return wb.active

def get_data(ws):
  # Get column names
  fields = list(ws.values)[0]

  # Generate dict from records
  return [
    {field_name: value for (field_name, value) in zip(fields, row)}
    for row in ws.values
  ]



if __name__ == "__main__":
  ws = get_active_worksheet('fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx')
  print(get_data(ws)[1])
