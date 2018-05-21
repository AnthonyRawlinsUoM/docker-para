import asyncio
import os, os.path
import xarray as xr

from lfmc.query import ShapeQuery
from lfmc.results.Abstracts import Abstracts
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
            Author(name="BOM", email="test1@example.com", organisation="Bureau of Meteorology, Australia")
        ]
        pub_date = dt.datetime(2015, 9, 9)
        abstract = Abstracts("The information presented on the Australian Landscape Water Balance website is produced by \
         the Bureau's operational Australian Water Resources Assessment Landscape model (AWRA-L). AWRA-L is a daily 0.05°\
          grid-based, distributed water balance model, conceptualised as a small unimpaired catchment. It simulates the\
           flow of water through the landscape from the rainfall entering the grid cell through the vegetation and soil\
            moisture stores and then out of the grid cell through evapotranspiration, runoff or deep drainage to the groundwater.\n \
        Each spatial unit (grid cell) in AWRA-L is divided into two hydrological response units (HRU) representing deep \
        rooted vegetation (trees) and shallow rooted vegetation (grass). Hydrological processes are modelled separately \
        for each HRU, then the resulting fluxes or stores are combined to give cell outputs. Hydrologically, these two \
        HRUs differ in their aerodynamic control of evaporation and their interception capacities but the main difference\
         is in their degree of access to different soil layers. The AWRA-L model has three soil layers (upper: 0–10 cm, \
         lower: 10–100 cm, and deep: 1–6 m). The shallow rooted vegetation has access to subsurface soil moisture in the \
         upper and lower soil stores only, while the deep rooted vegetation also has access to moisture in the deep store.")

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'AWRA-L') + '/'
        self.ident = "AWRA-L"
        self.code = "AWRA"
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

        dps = [self.get_datapoint_for_param(b=sr.sel(time=t), param=self.outputs["readings"]["prefix"]) for t in
               sr["time"]]
        asyncio.sleep(1)
        return ModelResult(model_name=self.name, data_points=dps)

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        fs = list(set([self.netcdf_name_for_date(when) for when in shape_query.temporal.dates()]))
        ts = xr.open_mfdataset(fs, chunks={'time': 1})
        asyncio.sleep(1)
        ts = ts.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                               shape_query.temporal.finish.strftime("%Y-%m-%d")))
        return shape_query.apply_mask_to(ts)
