import asyncio
import os, os.path
import xarray as xr

from lfmc.query import ShapeQuery
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.results.ModelResult import ModelResult
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class AWRAModel(Model):

    def __init__(self):
        self.name = "awra"

        # TODO - Proper metadata!
        authors = [
            Author(name="Test1", email="test1@example.com", organisation="Test Organisation"),
            Author(name="Test2", email="test2@example.com", organisation="Test Organisation"),
            Author(name="Test3", email="test3@example.com", organisation="Test Organisation")
        ]
        pub_date = dt.datetime(2015, 9, 9)

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010")

        self.path = os.path.abspath(Model.path() + 'AWRA-L') + '/'

        # 2017 is here...
        # /media/arawlins/Backups/DataSources/geoserver_data/FuelModels/AWRA-L/s0_pct_2017_Actual_day.nc


        self.outputs = {
            "type": "soil moisture",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "sm_pct",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return self.path + "sm_pct_{}_Actual_day.nc".format(when.strftime("%Y"))

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        logger.debug(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        logger.debug("Spatial Component is: \n%s" % str(query.spatial))
        logger.debug("Temporal Component is: \n%s" % str(query.temporal))
        logger.debug("\nDerived LAT1: %s\nDerived LON1: %s\nDerived LAT2: %s\nDerived LON2: %s" %
                     query.spatial.expanded(0.05))

        sr = await (self.get_shaped_resultcube(query))
        sr.load()

        # Values in AWRA are in range 0..1
        # Normalise to between 0-100%
        sr[self.outputs["readings"]["prefix"]] *= 100

        dps = [self.get_datapoint_for_param(b=sr.sel(time=t), param=self.outputs["readings"]["prefix"]) for t in sr["time"]]
        asyncio.sleep(1)
        return ModelResult(model_name=self.name, data_points=dps)

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        fs = list(set([self.netcdf_name_for_date(when) for when in shape_query.temporal.dates()]))
        ts = xr.open_mfdataset(fs, chunks={'time': 1})
        asyncio.sleep(1)
        ts = ts.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"), shape_query.temporal.finish.strftime("%Y-%m-%d")))
        return shape_query.apply_mask_to(ts)
