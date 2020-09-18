from record import Record

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
