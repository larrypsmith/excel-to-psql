class Record:
  def __init__(self, ws, row, fields):
    self._data = self.data(ws, row, fields)

  @property
  def fields(self):
    return list(self._data.keys())

  @property
  def values(self):
    return list(self._data.values())
  
  def data(self, ws, row, fields):
    """
    Transform row data and field params into dict
    where keys are field names and values are cell values
    """
    column_names = [cell.value for cell in ws['1']]
    result = {}
    for field in fields:
      value = self.get_value_from_ws(column_names, row, field['source'])
      result[field['name']] = value
    return result

  def get_value_from_ws(self, column_names, row, column_name):
    column_number = column_names.index(column_name)
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

  def set_id(self, id):
    self._data['id'] = id

  def __repr__(self):
    return f"<Record ({self._data})>"
