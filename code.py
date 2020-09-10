import openpyxl
import psycopg2

def print_cells(path):
  wb = openpyxl.load_workbook(filename=path, read_only=True)
  ws = wb.active
  for value in ws.values:
    print(value)

if __name__ == "__main__":
  print_cells('fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx')
