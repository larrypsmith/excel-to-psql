import openpyxl
from psycopg2 import connect, sql
from faker import Faker
fake = Faker()

"""
data_maps = [
  {
    'table': 'highschools',
    'fields': [
      {
        'name': 'id',
        'source': 'PRIMARY_KEY'
      }
      {
        'name': 'name'
        'source': 'HighSchoolName',
      },
      {
        'name': 'state'
        'source': 'HighSchoolState',
      }
    ]
  },
  {
    'table': 'colleges',
    'fields': [
      {
        'name': 'id',
        'source': 'PRIMARY_KEY'
      },
      {
        'name': 'name',
        'source': 'CollegeName'
      }
      {
        'name': 'city',
        'source': 'CollegeCity'
      }
      {
        'name': 'state',
        'source': 'CollegeState'
      }
    ]
  },
  {
    'table': 'person',
    'fields': [
      {
        'name': 'id',
        'source: 'PRIMARY_KEY'
      },
      {
        'name': 'first_name',
        'source': 'FirstName',
        'fakeable': fake.first_name
      },
      {
        'name': 'last_name',
        'source': 'LastName',
        'fakeable': fake.last_name
      },
      {
        'name': 'email',
        'source': 'Email',
        'fakeable': fake.email
      }
    ]
  },
  {
    'table': 'students',
    'fields': [
      {
        'name': 'id',
        'source': 'PRIMARY_KEY',
      },
      {
        'name': 'person_id',
        'source': { 'table': 'person' }
      },
      {
        'name': 'year',
        'source': 'Year',
      },
      {
        'name': 'enrollment_status',
        'source': 'Status'
      },
      {
        'name': 'gender',
        'source': 'Gender'
      },
      {
        'name': 'phone',
        'source': 'Phone'
      },
      {
        'name': 'highschool_id',
        'source': {
          'table': 'highschools',
          'field': 'name'
          'column': 'HighSchoolName'
        }
      },
      {
        'name': 'college_id',
        'source': {
          'table': 'colleges',
          'field': 'name'
          'column': 'CollegeName'
        }
      },
      {
        'name': 'hs_academic_score',
        'source': 'AcademicScore'
      },
      {
        'name': 'post_hs_plans',
        'source': 'PostHsPlans'
      },
      {
        'name': 'registration_status',
        'source': 'RegistrationStatus'
      },
      {
        'name': 'major',
        'source': 'Major'
      },
      {
        'name': 'degree_type',
        'source': 'PlannedDegreeType'
      },
      {
        'name': 'expected_graduation_year',
        'source': 'ExpectedGraduationYear'
      }
    ]
  }
]
"""

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

class Record:
  def __init__(self, ws, row, fields):
    self._data = self.data(ws, row, fields)

  def data(self, ws, row, fields):
    """
    Transform row data and field params into dict
    where keys are field names and values are cell values
    """
    result = {}
    for field in fields:
      value = self.get_value_from_ws(ws, row, field['source'])
      result[field['name']] = value
    return result

  def get_value_from_ws(self, ws, row, column_name):
    column_number = self.get_column_number(ws, column_name)
    return row[column_number].value
      
  def get_column_number(self, ws, column_name):
    """
    get column number in worksheet of cell in
    first row whose value matches column_name
    """
    for cell in ws['1']:
      if cell.value == column_name:
        return cell.column
    raise Exception('Column does not exist in spreadsheet')

  def __repr__(self):
    return f"<Record ({self._data})>"

class Table:
  def __init__(self, ws, name, field_params):
    self._name = name
    self._records = self.build_records(ws, field_params)

  def build_records(self, ws, field_params):
    # construct records from ws data
    records = [
      Record(ws, row, field_params) for row in ws.iter_rows(min_row=2)
    ]
    # add auto-incrementing ids to records that want them
    if 'generate_ids' in field_params and field_params['generate_ids']:
      records = self.add_ids_to(records)
    return records

  def create_id(self, id=0):
    def increment():
      nonlocal id
      id += 1
      return id
    return increment

  def add_ids_to(self, records):
    id = self.create_id()
    return [[id()] + record for record in records]


# store = {}

# def get_column_number(ws, column_name):
#   for cell in ws['1']:
#     if cell.value == column_name:
#       return cell.column
#   raise Exception('Column does not exist in spreadsheet')

# def build_record_data(ws, table, fields, row):
#   record = {}
#   for field in fields:
#     if isinstance(field['source'], str):
#       col_num = get_column_number(ws, field['source'])
#       result[field['name']]

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

def create_id(id=0):
  def increment():
    nonlocal id
    id += 1
    return id
  return increment

def uniques(data):
  ans = []
  uniqs = set()
  for record in data:
    removed_NoneTypes = [' ' if not val else val for val in record]
    joined = ",".join(removed_NoneTypes)
    if joined not in uniqs:
      uniqs.add(joined)
      ans.append(record)
  return ans

def get_data(ws, *fields):
  # get column numbers of desired fields
  field_column_nums = [cell.column for cell in ws['1'] if cell.value in fields]
  data = [
    [cell.value for cell in row if cell.column in field_column_nums]
    for row in ws.iter_rows(min_row=2)
  ]
  # remove records consisting of only NoneTypes
  data = filter(lambda list: any(list), data)
  return uniques(data)

def get_labels(ws):
  labels = get_data(ws, 'Labels')
  uniqs = set()
  for labels_str in labels:
    labels_list = labels_str[0].split('; ')
    for label in labels_list:
      uniqs.add((label.strip()))
  return [[uniq] for uniq in uniqs]

def get_insert_query(table, fields, data):
  return sql.SQL("""
    INSERT INTO {table} ({fields})
    VALUES ({placeholders})
  """).format(
    table=sql.Identifier(table),
    fields=sql.SQL(", ").join(map(sql.Identifier, fields)),
    placeholders=sql.SQL(', ').join(sql.Placeholder() * len(fields))
  )

# def get_foreign_key(cur, table):

def insert_data(cur, table, fields, data, generate_ids=False):
  query = get_insert_query(table, fields, data)
  if generate_ids:
    id = create_id()
    data = [[id()] + datums for datums in data]
  cur.executemany(query, data)

def transfer_highschools(ws, cur):
  highschools = get_data(ws, 'HighSchoolName', 'HighSchoolState')
  insert_data(
    cur,
    table='highschools',
    fields=['id', 'name', 'state'],
    data=highschools,
    generate_ids=True
  )

def transfer_colleges(ws, cur):
  colleges = get_data(ws, 'CollegeName', 'CollegeCity', 'CollegeState')
  insert_data(
    cur,
    table='colleges',
    fields=['id', 'name', 'city', 'state'],
    data=colleges,
    generate_ids=True
  )
    # for student in students:
    #   if student['CollegeName'] == college['CollegeName']:
    #       student['college_id'] = college['college_id']

def transfer_genders(ws, cur):
  genders = get_data(ws, 'Gender')
  insert_data(
    cur,
    table='genders',
    fields=['type'],
    data=genders
  )

def transfer_labels(ws, cur):
  labels = get_labels(ws)
  insert_data(
    cur,
    table='labels',
    fields=['type'],
    data=labels
  )

def transfer_degree_types(ws, cur):
  degree_types = get_data(ws, 'PlannedDegreeType')
  insert_data(
    cur,
    table='degree_types',
    fields=['type'],
    data=degree_types
  )

def transfer_enrollment_statuses(ws, cur):
  enrollment_statuses = get_data(ws, 'Status')
  insert_data(
    cur,
    table='enrollment_statuses',
    fields=['type'],
    data=enrollment_statuses
  )

def transfer_registration_statuses(ws, cur):
  registration_statuses = get_data(ws, 'RegistrationStatus')
  insert_data(
    cur,
    table='registration_statuses',
    fields=['type'],
    data=registration_statuses
  )

def transfer_students_to_person(ws, cur):
  student_persons = get_data(ws, 'Student Id', 'FirstName', 'LastName', 'Email')

  for sp in student_persons:
    # convert one student id with alpha char to integers only
    if sp[0][0].isalpha():
      sp[0] = 1000000000

    # Add fake data to statisfy db constraints
    sp[1] = sp[1] or fake.first_name() + ' (FAKE)'
    sp[2] = sp[2] or fake.last_name() + ' (FAKE)'
    sp[3] = sp[3] or fake.email() + ' (FAKE)'

  insert_data(
    cur,
    table='person',
    fields=['id', 'first_name', 'last_name', 'email'],
    data=student_persons,
  )

def transfer_students_to_students(ws, cur):
  students = get_data(
    ws,
    'Student Id'
    'Year',
    'Status',
    'Gender',
    'Phone',
    'HighSchoolName',
    'AcademicScore',
    'PostHsPlans',
    'RegistrationStatus',
    'Major',
    'PlannedDegreeType',
    'ExpectedGraduationYear',
    'CollegeName',
  )

  # for student in students:


def main():
  # Open Gradsnapp data
  wb = openpyxl.load_workbook(
    filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
    data_only=True
  )
  ws = wb.active

  # Connect to DB
  with connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      
      # insert students into person table
      person_id = 1
      for student in students:
        student['person_id'] = person_id
        cur.execute("""
          INSERT INTO person (id, first_name, last_name, email)
          VALUES (%s, %s, %s, %s)
        """, (
          student['person_id'],
          student['FirstName'] or fake.first_name() + ' (FAKE)',
          student['LastName'] or fake.last_name() + ' (FAKE)',
          student['Email'] or fake.email() + ' (FAKE)'
        ))
        person_id += 1

      # insert students into students table
      for student in students:
        if student['HighSchoolName'] is None:
          student['highschool_id'] = None
        if student['CollegeName'] is None:
          student['college_id'] = None
        # handle one student ID that contains a letter
        if student['Student Id'][0].isalpha():
          student['Student Id'] = 10000000
        # convert majors that are empty strings to None
        if not student['Major']:
          student['Major'] = None
        cur.execute("""
          INSERT INTO students (id, person_id, year, enrollment_status, gender,
                                phone, highschool_id, college_id,
                                hs_academic_score, post_hs_plans,
                                registration_status, major, degree_type,
                                expected_graduation_year)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
          student['Student Id'],
          student['person_id'],
          student['Year'],
          student['Status'],
          student['Gender'],
          student['Phone'],
          student['highschool_id'],
          student['college_id'],
          student['AcademicScore'],
          student['PostHsPlans'],
          student['RegistrationStatus'],
          student['Major'],
          student['PlannedDegreeType'],
          student['ExpectedGraduationYear']
        ))

      # insert student_labels
      student_label_id = 1
      for student in students:
        labels = [label.strip() for label in student['Labels'].split('; ')]
        for label in labels:
          cur.execute("""
            INSERT INTO student_labels (id, student_id, label_type)
            VALUES (%s, %s, %s)
          """, (
            student_label_id,
            student['Student Id'],
            label
          ))
          student_label_id += 1

      # open interactions worksheet
      wb = openpyxl.load_workbook(
        filename='fwddata/PersistEngagementReport_Example.xlsx',
        data_only=True
      )
      ws = wb.active

      # insert admins
      admins = get_unique(ws, fields=['Created By'])
      admins = [admin for admin in admins if admin['Created By'] != '']
      admin_id = 1
      for admin in admins:
        name = admin['Created By'].split(' ')
        admin['first_name'] = name[0]
        if (len(name) == 2):
          admin['last_name'] = name[1]
        admin['person_id'] = person_id
        admin['id'] = admin_id
        # insert into person table
        cur.execute("""
          INSERT INTO person (id, first_name, last_name, email)
          VALUES (%s, %s, %s, %s)
        """, (
            admin['person_id'],
            admin['first_name'],
            admin['last_name'],
            fake.email() + ' (FAKE)'
        ))
        person_id += 1

        # insert into admins table
        cur.execute("""
          INSERT INTO admins (id, person_id, password)
          VALUES (%s, %s, %s)
        """, (
            admin['id'],
            admin['person_id'],
            'password'
        ))
        admin_id += 1

      # insert interaction_types
      interaction_types = get_unique(
        ws,
        fields=['Interaction Type']
      )
      for inter_type in interaction_types:
        cur.execute("""
          INSERT INTO interaction_types (type)
          VALUES (%s)
        """, (inter_type['Interaction Type'],))

      """
        Interaction content:
        -- Bulk Email: None
        -- Bulk Sms: SMS Message
        -- Sms Received: SMS Message
        -- Note: Contact Note
      """

      # interactions = get_all(ws, 'System Id', 'Interaction Type',
      #                        'Student ID', 'Created Date', 'SMS Message',
      #                        'Contact Note')
                  
      # # set interaction content based on type
      # for interaction in interactions:
      #   if interaction['Interaction Type'] == 'Bulk Email':
      #     interaction['content'] = None
      #   elif interaction['Interaction Type'] == 'Note':
      #     interaction['content'] = interaction['Contact Note']
      #   else:
      #     interaction['content'] = interaction['SMS Message']

      # for interaction in interactions:
      #     # get person_ids of students in interactions
      #     interaction['student_person_id'] = None
      #     for student in students:
      #       if student['Student Id'] == interaction['Student ID']:
      #         interaction['student_person_id'] = student['person_id']

      #     interaction['admin_person_id'] = None
      #     for admin in admins:
      #       for person in persons:
      #         if interaction['Created By'] == person['first_name'] + person['last_name'] and person['id'] == admin['person_id']:
      #           interaction['admin_person_id'] = person['id']

      #     # set interaction creator and receiver based on type
      #     if interaction['Interaction Type'] == 'Sms Recieved':
      #       interaction['recipient_id'] = interaction['admin_person_Id']
      #       interaction['created_by_id'] = interaction['student_person_id']
      #     else:
      #       interaction['recipient_id'] = interaction['student_person_id']
      #       interaction['created_by_id'] = interaction['admin_person_Id']
      # 
      # interaction_id = 1
      # for interaction in interactions:
      #   cur.execute("""
      #     INSERT INTO interactions (id, interaction_type, created_by_id, recipient_id, date, content)
      #     VALUES (%s, %s, %s, %s, %s, %s)
      #   """, (
      #     interaction_id,
      #     interaction['Interaction Type'],
      #     interaction['created_by_id'],
      #     interaction['recipient_id'],
      #     interaction['Created Date'],
      #     interaction['content']
      #   ))
      #   interaction_id += 1

if __name__ == '__main__':
  # main()

  wb = openpyxl.load_workbook(
    filename='fwddata/Gradsnapp Data - Cleaned of Personal Identifable Info (No Phone Email).xlsx',
    data_only=True
  )
  ws = wb.active

  with connect(**CONNECTION_PARAMETERS) as conn:
    with conn.cursor() as cur:
      # empty_tables(cur)
      highschools = Table(ws, highschool_params['table'], highschool_params['fields'])
      print(highschools._records)
