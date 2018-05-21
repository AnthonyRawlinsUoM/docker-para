import asyncio
import os
import os.path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class TempModel(BomBasedModel):

    def __init__(self):
        self.name = "temperature"

        # TODO - Proper metadata!
        authors = [
            Author(name="", email="",
                   organisation="")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)
        self.ident = "Temperature"
        self.code = "Tmax"
        self.path = os.path.abspath(Model.path() + 'Weather') + '/'
        self.crs = "EPSG:3111"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "Tmx",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return os.path.abspath(
            Model.path() + "/Dead_FM/Tmx/Tmx_{}.grid.nc".format(when.strftime("%Y%m%d")))
