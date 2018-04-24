import hug
from abc import abstractmethod
from marshmallow import Schema, fields
from lfmc.results.Author import Author
from lfmc.models.ModelMetaData import ModelMetaDataSchema
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery

class Model():
    def __init__(self):
        self.name = "Base Model Class"
        self.metadata = {}
        self.parameters = {}
        self.outputs = {}
        self.tolerance = 0
        
    def __init__(self, model):
        """ Copy-constructor """
        self.name = model.name
        self.metadata = model.metadata
        self.parameters = model.parameters
        self.outputs = model.outputs
        self.tolerance = model.tolerance

    @staticmethod
    def path():
        return '/FuelModels/'

    @abstractmethod
    def get_metadata(self):
        return self.metadata

    @abstractmethod
    def get_parameters(self):
        return self.parameters

    @abstractmethod
    def get_outputs(self):
        return self.outputs

    @abstractmethod
    def get_tolerance(self):
        return self.tolerance
    
    @abstractmethod
    def answer(self, query: SpatioTemporalQuery) -> ModelResult:
        return None

    pass

class ModelSchema(Schema):
    name = hug.types.Text
    # metadata = fields.Nested(ModelMetaDataSchema, many=False)
    # parameters = fields.Nested(fields.String)
    # outputs = fields.Nested(fields.String)
    # tolerance = fields.Decimal(as_string=True)
