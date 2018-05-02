from datetime import datetime as dt
from marshmallow import Schema, fields
import pandas as pd
from lfmc.query.Query import Query

class TemporalQuery(Query):
  """
    TemporalQuery is a date range. All data in the temporal range is returned regardless of resolution.
  """
  def __init__(self, start, finish):
    self.start = dt.strptime(start, '%Y%m%d').date()
    self.finish = dt.strptime(finish, '%Y%m%d').date()
    
  def dates(self):
      return pd.date_range(self.start, self.finish)

class TemporalQuerySchema(Schema):
    start = fields.Date()
    finish = fields.Date()

