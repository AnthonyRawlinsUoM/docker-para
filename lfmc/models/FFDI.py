import asyncio
import os
import os.path
import glob
from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class FFDIModel(BomBasedModel):

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

        self.path = os.path.abspath(Model.path() + 'FFDI') + '/'
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "FFDI_SFC",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return [p + "/IDV71117_VIC_FFDI_SFC.nc" for p in glob.glob(Model.path() + "Weather/{}*".format(when.strftime("%Y%m%d")))]
