class Worksheet:
  def __init__(self, ws):
    self._ws = ws
    # index is column number in ws, element is column name
    self._column_names = [None] + [cell.value for cell in ws['1']]

  def get_values(self, *column_names):
    ans = []
    uniqs = set()
    
    rows = self._filter_columns(column_names)
    for row in rows:
      joined = ",".join([str(cell.value) for cell in row])
      if joined not in uniqs:
        uniqs.add(joined)
        ans.append([cell.value for cell in row])
    ans = filter(lambda row: any(row), ans)
    return list(ans)

  def _filter_columns(self, column_names):
    column_numbers = []
    for name in column_names:
      col_number = self._column_names.index(name)
      if not col_number:
        raise Exception(f'Column {name} does not exist in spreadsheet')
      column_numbers.append(col_number)

    res = []
    for row in self._ws.iter_rows(min_row=2):
      res.append(tuple(filter(lambda cell: cell.column in column_numbers, row)))
    return res
