from datetime import datetime as dt
from marshmallow import Schema, fields


class Query:
    def __init__(self):
        self.request_time = dt.now()
        self.response_time = None

    def logResponse(self):
        self.response_time = dt.now()

    def is_complete(self):
        return self.response_time is not None

    pass


class QuerySchema(Schema):
    request_time = fields.DateTime()
    response_time = fields.DateTime()
