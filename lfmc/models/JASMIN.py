import asyncio
import os
import os.path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class JasminModel(BomBasedModel):

    def __init__(self):
        self.name = "jasmin"

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

        self.path = os.path.abspath(Model.path() + 'JASMIN') + '/'

        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "smd",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return os.path.abspath(
            Model.path() + "/JASMIN/rescaled/21vls/jasmin.kbdi/cdf temporal/jasmin.kbdi.cdf_temporal.2lvls.{}.nc".format(
                when.strftime("%Y")))
