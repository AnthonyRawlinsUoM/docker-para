from lfmc.query.Query import Query, QuerySchema
from marshmallow import Schema, fields


class SpatialQuery(Query):
  """ SpatialQuery is a Bounding box defined by the NW/SE corners of a rectangular selection
  """

  def __init__(self, lat1, lon1, lat2, lon2):
    """Short summary.

    Parameters
    ----------
    lat1 : type
        Description of parameter `lat1`.
    lon1 : type
        Description of parameter `lon1`.
    lat2 : type
        Description of parameter `lat2`.
    lon2 : type
        Description of parameter `lon2`.

    Returns
    -------
    type
        Description of returned object.

    """

    self.lat1 = lat1
    self.lon1 = lon1
    self.lat2 = lat2
    self.lon2 = lon2

  pass

class SpatialQuerySchema(Schema):
  lat1 = fields.Decimal(places=8, as_string=True)
  lon1 = fields.Decimal(places=8, as_string=True)
  lat2 = fields.Decimal(places=8, as_string=True)
  lon2 = fields.Decimal(places=8, as_string=True)


# sq = SpatialQuery(-10, 110, -45, 155)
# schema = SpatialQuerySchema()
# data, errors = schema.dump(sq)
# 
# data
