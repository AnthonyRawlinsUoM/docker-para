import asyncio
from abc import abstractmethod

import xarray as xr
from pathlib2 import Path
import lfmc.config.debug as dev
from lfmc.models.Model import Model
from lfmc.query import ShapeQuery
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class BomBasedModel(Model):

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        if dev.DEBUG:
            logger.debug(
                "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
            logger.debug("Spatial Component is: \n%s" % str(query.spatial))
            logger.debug("Temporal Component is: \n%s" % str(query.temporal))
            logger.debug("\nDerived LAT1: %s\nDerived LON1: %s\nDerived LAT2: %s\nDerived LON2: %s" %
                         query.spatial.expanded(0.05))
        sr = await (self.get_shaped_resultcube(query))
        sr.load()
        if dev.DEBUG:
            logger.debug("Shaped ResultCube is: \n%s" % sr)
        dps = []

        for r in sr['time']:
            t = r['time'].values
            o = sr.sel(time=t)
            p = self.outputs['readings']['prefix']
            df = o[p].to_dataframe()
            df = df[p]
            # TODO - This is a quick hack to massage the datetime format into a markup suitable for D3 & ngx-charts!
            m = df.mean()
            dps.append(DataPoint(observation_time=str(t).replace('.000000000', '.000Z'),
                                 value=m,
                                 mean=m,
                                 minimum=df.min(),
                                 maximum=df.max(),
                                 deviation=df.std()))

        asyncio.sleep(1)

        return ModelResult(model_name=self.name, data_points=dps)

    @abstractmethod
    def netcdf_name_for_date(self, when):
        if dev.DEBUG:
            logger.debug("called abstract ERGH!")
        return ['/tmp']  # DEBUG ONLY

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:

        fs = set()
        for when in shape_query.temporal.dates():
            fpaths = self.netcdf_name_for_date(when)
            logger.debug("fpaths= %s" % fpaths)
            if len(fpaths) > 1:
                [fs.add(file) for file in fpaths]
            elif len(fpaths) == 1:
                fs.add(*fpaths)

        fl = [file for file in fs if Path(file).is_file()]
        xr1 = xr.DataArray(None)
        if dev.DEBUG:
            [logger.debug("\n--> Will load: %s" % f) for f in fl]

        # Load these files in date order overwriting older data with the newer
        if len(fl) > 0:
            fl.sort()
            xr1 = xr.open_dataset(fl.pop())
            while len(fl) > 1:
                xr2 = xr.open_dataset(fl.pop())
                if dev.DEBUG:
                    logger.debug("\n--> Loading BOM SFC TS by overwriting older data: %s" % fl[0])
                xr1 = self.load_by_overwrite(xr1, xr2)

        ts = xr1
        ts.attrs['var_name'] = self.outputs["readings"]["prefix"]
        ts = ts.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"),
                               shape_query.temporal.finish.strftime("%Y-%m-%d")))
        return shape_query.apply_mask_to(ts)

    def load_by_overwrite(self, xr1, xr2):
        ds1_start = xr1[self.outputs["readings"]["prefix"]].isel(time=0).time.values
        ds2_start = xr2[self.outputs["readings"]["prefix"]].isel(time=0).time.values
        ds1_subset = xr1.sel(time=slice(str(ds1_start), str(ds2_start)))
        return xr.concat([ds1_subset, xr2], dim='time')
