import asyncio

import os
import os.path
import glob
from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.query import ShapeQuery
from lfmc.results import ModelResult
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class KBDIModel(BomBasedModel):

    def __init__(self):
        self.name = "kbdi"

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

        self.path = os.path.abspath(Model.path() + 'KBDI') + '/'
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "KBDI_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return [p + "/IDV71147_VIC_KBDI_SFC.nc" for p in glob.glob(Model.path() + "Weather/{}*".format(when.strftime("%Y%m%d")))]

    def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        return super().get_shaped_timeseries(query)


