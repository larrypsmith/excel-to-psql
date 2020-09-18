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
