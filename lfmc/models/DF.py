import os
import os.path
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.results.ModelResult import ModelResult
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults
import xarray as xr


class DFModel(Model):

    def __init__(self):
        self.name = "ffdi"

        # TODO - Proper metadata!
        authors = [
            Author(name="Test1", email="test1@example.com",
                   organisation="Test Organisation"),
            Author(name="Test2", email="test2@example.com",
                   organisation="Test Organisation"),
            Author(name="Test3", email="test3@example.com",
                   organisation="Test Organisation")
        ]
        pub_date = dt.datetime(2015, 9, 9)

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010")

        self.path = os.path.abspath(
            '/media/arawlins/Backups/DataSources/geoserver_data/FuelModels/Drought_Factor') + '/'

        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": self.name,
                "suffix": ".nc"
            }
        }

    def get_timeseries(self, query: SpatioTemporalQuery) -> ModelResult:
        # MAGIC HAPPENS HERE

        # An array of mockup DataPoints for testing only
        dr = DummyResults()
        dps = dr.dummy_data(query)
        return ModelResult(self.name, dps)

    def get_resultcube(self, query: SpatioTemporalQuery) -> xr.DataArray:
        return xr.DataArray(None)
