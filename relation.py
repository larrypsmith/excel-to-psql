from psycopg2 import sql

class Relation:
  def __init__(self, rows, field_names):
    self._records = self._build_data(rows, field_names)

  @property
  def records(self):
    return self._records

  def add_ids(self):
    id = self._create_id()
    for datum in self._records:
      datum['id'] = id()

  def insert(self, cur, table):
    query = self._sql(table)
    cur.executemany(query, self._query_values)


  def _build_data(self, rows, field_names):
    ans = []
    for data in rows:
      datum = {}
      for i in range(len(data)):
        datum[field_names[i]] = data[i]
      ans.append(datum)
    return ans

  def _create_id(self, id=0):
    def increment():
      nonlocal id
      id += 1
      return id
    return increment

  def _sql(self, table):
    return sql.SQL("""
      INSERT INTO {table} ({fields})
      VALUES ({placeholders})
    """).format(
        table=sql.Identifier(table),
        fields=sql.SQL(", ").join(map(sql.Identifier, self._fields)),
        placeholders=(sql.SQL(', ')
          .join(sql.Placeholder() * len(self._query_values[0])))
    )

  @property
  def _fields(self):
    return list(self._records[0].keys())

  @property
  def _query_values(self):
    return [list(record.values()) for record in self._records]
