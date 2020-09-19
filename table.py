from record import Record
from psycopg2 import sql

class Table:
  def __init__(self, ws, params):
    self._generate_ids = 'generate_ids' in params and params['generate_ids']
    self._name = params['table']
    self._fields = [field['name'] for field in params['fields']]
    self._records = self.build_records(ws, params['fields'])

  def build_records(self, ws, field_params):
    # construct records from ws data
    records = [
      Record(ws, row, field_params) for row in ws.iter_rows(min_row=2)
    ]
    # filter non-unique records
    records = self.filter_uniques(records)
    # filter all-Nonetype records
    # records = filter(lambda record: any(record.values), records)
    # add auto-incrementing ids to table that want them
    if self._generate_ids:
      self._fields.append('id')
      id = self.create_id()
      for record in records:
        record.set_id(id())
    return records

  def create_id(self, id=0):
    def increment():
      nonlocal id
      id += 1
      return id
    return increment

  def insert_records_into_db(self, cur):
    query = sql.SQL("""
      INSERT INTO {table} ({fields})
      VALUES ({placeholders})
    """).format(
      table=sql.Identifier(self._name),
      fields=sql.SQL(", ").join(map(sql.Identifier, self._fields)),
      placeholders=sql.SQL(', ').join(sql.Placeholder() * len(self._fields))
    )
    cur.executemany(query, [record.values for record in self._records])

  def filter_uniques(self, records):
    ans = []
    uniqs = set()
    for record in records:
      removed_NoneTypes = [' ' if not val else val for val in record.values]
      joined = ",".join(removed_NoneTypes)
      if joined not in uniqs:
        uniqs.add(joined)
        ans.append(record)
    return ans