from datetime import datetime as dt
from marshmallow import Schema, fields

class Query:
    def __init__(self):
        self.request_time = dt.now()

    def logResponse(self):
        self.response_time = dt.now()
    
    pass

class QuerySchema(Schema):
    request_time = fields.DateTime()
    response_time = fields.DateTime()
