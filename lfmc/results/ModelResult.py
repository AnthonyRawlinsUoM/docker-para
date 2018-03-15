from marshmallow import Schema, fields
from lfmc.results.DataPoint import DataPoint, DataPointSchema
from lfmc.models.ModelMetaData import ModelMetaData, ModelMetaDataSchema


class ModelResult:
    def __init__(self, model_name, data_points):
        """Short summary.

        Parameters
        ----------
        model_name : type
                        Description of parameter `model_name`.
        data_points : type
                        Description of parameter `data_points`.
        Returns
        -------
        type
                        Description of returned object.
        """
        self.series = data_points
        self.name = model_name


class ModelResultSchema(Schema):
    name = fields.String()
    series = fields.Nested(DataPointSchema, many=True)
