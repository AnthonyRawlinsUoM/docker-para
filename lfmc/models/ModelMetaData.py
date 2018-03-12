from marshmallow import Schema, fields
from lfmc.results.Author import AuthorSchema


class ModelMetaData:
    def __init__(self, authors, published_date, fuel_types):
        """Short summary.

        Parameters
        ----------
        authors : type
                Description of parameter `authors`.
        published_date : type
                Description of parameter `published_date`.
        fuel_types : type
                Description of parameter `fuel_types`.

        Returns
        -------
        type
                Description of returned object.

        """
        self.authors = authors
        self.published_date = published_date
        self.fuel_types = fuel_types


class ModelMetaDataSchema(Schema):
    authors = fields.Nested(AuthorSchema, many=True)
    published_date = fields.Date()
    fuel_types = fields.Nested(fields.String, many=True)
