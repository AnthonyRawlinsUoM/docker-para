from marshmallow import Schema, fields
import datetime as dt

class DataPoint:
    def __init__(self, observation_time, value, mean, min, max, std):
        """Short summary.

        Parameters
        ----------
        observation_time : type
                Description of parameter `observation_time`.
        value : type
                Description of parameter `value`.
        mean : type
                Description of parameter `mean`.
        min : type
                Description of parameter `min`.
        max : type
                Description of parameter `max`.
        std : type
                Description of parameter `std`.

        Returns
        -------
        type
                Description of returned object.

        """
        self.name = observation_time
        self.value = value
        self.mean = mean
        self.min = min
        self.max = max
        self.std = std
        self.date = dt.datetime.now()


class DataPointSchema(Schema):
    # date = fields.Date(attribute="test")
    name = fields.DateTime(format="%Y-%m-%dT00:00:00.000Z")
    value = fields.Decimal(places=4, as_string=True)
    mean = fields.Decimal(places=4, as_string=True)
    min = fields.Decimal(places=4, as_string=True)
    max = fields.Decimal(places=4, as_string=True)
    std = fields.Decimal(places=4, as_string=True)
