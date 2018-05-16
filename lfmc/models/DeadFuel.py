import os
import os.path
import numpy as np

import datetime as dt
import xarray as xr
from pathlib2 import Path

import asyncio

from lfmc.results.Author import Author
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.results.DataPoint import DataPoint
from lfmc.results.MPEGFormatter import MPEGFormatter
from lfmc.results.ModelResult import ModelResult
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery

import matplotlib.pyplot as plt

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

plt.switch_backend('agg')


class DeadFuelModel(Model):
    """ This is somewhat of an unusual case in that the files used for VP and T come as grid files and require some
     pre-processing to get them into properly projected NetCDF files.
    """

    def __init__(self):

        self.name = "dead_fuel"

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

        self.metadata = ModelMetaData(authors=authors,
                                      published_date=pub_date,
                                      fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010")

        # Prefixes
        vapour_prefix = 'VP3pm'
        temp_prefix = 'Tmx'
        precipitation_prefix = 'P'
        dead_fuel_moisture_prefix = 'DFMC'

        self.path = os.path.abspath(Model.path() + 'Dead_FM') + '/'

        vapour_url = "http://www.bom.gov.au/web03/ncc/www/awap/vprp/vprph15/daily/grid/0.05/history/nat/"
        max_avg_temp_url = "http://www.bom.gov.au/web03/ncc/www/awap/temperature/maxave/daily/grid/0.05/history/nat/"
        precipitation_url = "http://www.bom.gov.au/web03/ncc/www/awap/rainfall/totals/daily/grid/0.05/history/nat/"

        vapour_path = self.path + vapour_prefix + "/"
        max_avg_temp_path = self.path + temp_prefix + "/"
        precipitation_path = self.path + precipitation_prefix + "/"

        self.tolerance = 0.06  # As a percentage accuracy

        self.parameters = {
            "vapour pressure": {
                "var": "VP3pm",
                "path": vapour_path,
                "url": vapour_url,
                "prefix": vapour_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            },
            "maximum average temperature": {
                "var": "T",
                "path": max_avg_temp_path,
                "url": max_avg_temp_url,
                "prefix": temp_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            },
            "precipitation": {
                "var": "P",
                "path": precipitation_path,
                "url": precipitation_url,
                "prefix": precipitation_prefix,
                "suffix": ".grid",
                "dataset": ".grid.nc",
                "compression_suffix": ".Z"
            }
        }

        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "path": self.path + dead_fuel_moisture_prefix + "/",
                "url": "",
                "prefix": dead_fuel_moisture_prefix,
                "suffix": ".nc",
            }
        }

        # self.storage_engine = LocalStorage(
        #     {"parameters": self.parameters, "outputs": self.outputs})

    def netcdf_name_for_date(self, when):
        return "{}{}_{}{}".format(self.outputs["readings"]["path"],
                                  self.outputs["readings"]["prefix"],
                                  when.strftime("%Y%m%d"),
                                  self.outputs["readings"]["suffix"])

    async def dataset_files(self, when):
        if self.date_is_cached(when):
            return self.netcdf_name_for_date(when)
        else:
            return self.do_compilation(await asyncio.gather(*[self.collect_parameter_data(param, when)
                                                              for param in self.parameters]), when)

    async def mpg(self, query: ShapeQuery):
        sr = await (self.get_shaped_resultcube(query))
        logger.debug(sr)
        mp4 = await (MPEGFormatter.format(sr, "DFMC"))
        asyncio.sleep(1)
        return mp4

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        sr = None
        fs = await asyncio.gather(*[self.dataset_files(when) for when in shape_query.temporal.dates()])
        fs = [f for f in fs if Path(f).is_file()]
        asyncio.sleep(1)
        if len(fs) > 0:
            with xr.open_mfdataset(fs) as ds:
                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")
            sr = sr.sel(time=slice(shape_query.temporal.start.strftime("%Y-%m-%d"), shape_query.temporal.finish.strftime("%Y-%m-%d")))

            return shape_query.apply_mask_to(sr)
        else:
            return xr.DataArray([])

    async def get_resultcube(self, query: SpatioTemporalQuery) -> xr.DataArray:
        """
        Does not guarantee a raster stack result.
        Quite possibly a jaggy edge.
        Essentially a subset of points only.
        """

        sr = None
        fs = await asyncio.gather(*[self.dataset_files(when) for when in query.temporal.dates()])
        asyncio.sleep(1)
        if len(fs) > 0:
            with xr.open_mfdataset(fs) as ds:
                if "observations" in ds.dims:
                    ds = ds.squeeze("observations")

                # expand coverage to tolerance
                # ensures single point returns at least 1 cell
                # also ensures ds slicing will work correctly
                lat1, lon1, lat2, lon2 = query.spatial.expanded(
                    0.05)  # <-- TODO - Remove magic number and get spatial pixel resolution from metadata

                # restrict coverage to extents of ds
                lat1 = max(lat1, ds["latitude"].min())
                lon1 = max(lon1, ds["longitude"].min())
                lat2 = min(lat2, ds["latitude"].max())
                lon2 = min(lon2, ds["longitude"].max())

                sr = ds.sel(latitude=slice(lat1, lat2),
                            longitude=slice(lon1, lon2))
                sr.load()

        return sr

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        logger.debug(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)

        # logger.debug("Spatial Component is: \n%s" % str(query.spatial))
        # logger.debug("Temporal Component is: \n%s" % str(query.temporal))
        #
        # logger.debug("\nDerived LAT1: %s\nDerived LON1: %s\nDerived LAT2: %s\nDerived LON2: %s" %
        #              query.spatial.expanded(0.05))

        sr = await (self.get_shaped_resultcube(query))
        sr.load()
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

    async def get_timeseries(self, query: SpatioTemporalQuery) -> ModelResult:
        """
        Essentially just time slicing the resultcube.
        DataPoint actually handles the creation of values from stats.
        :param query:
        :return:
        """
        logger.debug(
            "--->>> SpatioTemporal Query Called on %s Model!! <<<---" % self.name)
        sr = await (self.get_resultcube(query))
        sr.load()
        asyncio.sleep(1)
        dps = [self.get_datapoint_for_param(b=sr.isel(time=t), param="DFMC")
               for t in range(0, len(sr["time"]))]
        return ModelResult(model_name=self.name, data_points=dps)

    async def get_netcdf(self, query: SpatioTemporalQuery):
        sr = await (self.get_resultcube(query))
        asyncio.sleep(1)
        sr.to_netcdf('/tmp/temp.nc', format='NETCDF4')
        return '/tmp/temp.nc'

    @staticmethod
    def do_conversion(file_name, param, when):
        """ Converts Arc Grid input files to NetCDF4 """
        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")
        logger.debug(
            "\n--> Processing data for: %s-%s-%s\n--> Converting: %s" % (d, m, y, file_name))
        nc_version = "%s.nc" % file_name
        arr = xr.open_rasterio("%s" % file_name)
        arr = arr.to_dataset(name="observations", dim=param["prefix"])
        arr = arr.rename({'y': 'latitude', 'x': 'longitude', 'band': 'time'})
        arr.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
        arr.attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y, m, d)
        arr.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
        arr.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
        arr.to_netcdf(nc_version, mode='w', format='NETCDF4')
        arr.close()

        return nc_version

    def do_compilation(self, param_datasets, when):
        DFMC_file = self.netcdf_name_for_date(when)

        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")

        tempfile = '/tmp/temp%s-%s-%s.nc' % (d, m, y)

        if len(param_datasets) > 0:

            # if len(param_datasets) == 1:
            #     logger.debug("Will open just: %s" % param_datasets)
            # elif len(param_datasets) > 1:

            logger.debug("\n----> Will open: %s" % f for f in param_datasets)

            with xr.open_mfdataset(param_datasets, concat_dim="observations") as ds:
                vp = ds["VP3pm"].isel(time=0)
                tmx = ds["Tmx"].isel(time=0)
                dfmc = DeadFuelModel.calculate(vp, tmx)
                dfmc = dfmc.expand_dims('time')
                logger.debug("Processing data for: %s-%s-%s" % (d, m, y))
                DFMC = dfmc.to_dataset('DFMC')
                DFMC.to_netcdf(tempfile, format='NETCDF4')
                logger.debug("\n------> Wrote: %s" % tempfile)
                logger.debug(DFMC)

            param_datasets.append(tempfile)

            with xr.open_mfdataset(param_datasets) as combined:
                # DFMC.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
                combined['DFMC'].attrs['DFMC:units'] = "Percentage wet over dry by weight."
                combined['DFMC'].attrs['long_name'] = "Dead Fuel Moisture Content"
                combined['DFMC'].attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (
                    y, m, d)
                combined['DFMC'].attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
                combined.attrs['created'] = "%s" % (
                    dt.datetime.now().strftime("%d-%m-%Y"))
                combined.attrs['output_frequency'] = "daily"
                combined.attrs['convention'] = "CF-1.4"
                combined.attrs['references'] = "#refs"
                combined.attrs['comment'] = "#comments"
                combined.attrs['var_name'] = self.outputs["readings"]["prefix"]

                logger.debug(combined)

                combined.to_netcdf(DFMC_file, mode='w', format='NETCDF4')
                combined.close()

            os.remove(tempfile)
        else:
            logger.debug("--> Can't open partial requirements!!! ")

        # Send file to SWIFT Storage here?
        asyncio.sleep(1)

        return DFMC_file

    async def collect_parameter_data(self, param, when):
        """ Collects input parameters for the model as determined by the metadata. """
        param = self.parameters[param]
        file_path = Path(param['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)

        parameter_dataset_name = file_path / (param['prefix'] + "_" +
                                              param['dataset'])
        if parameter_dataset_name.is_file():
            return parameter_dataset_name
        else:
            data_file = file_path / (param['prefix'] + "_" +
                                     when.strftime("%Y%m%d") +
                                     param['suffix'])

            archive_file = Path(str(data_file) + param['compression_suffix'])

            if data_file.is_file():
                parameter_dataset_name = self.do_conversion(
                    data_file, param, when)

            elif not data_file.is_file() and archive_file.is_file():
                data_file = self.do_expansion(archive_file)
                parameter_dataset_name = self.do_conversion(
                    data_file, param, when)
                # Remove the archive?

            elif not data_file.is_file() and not archive_file.is_file():
                date_string = when.strftime("%Y%m%d")
                resource = date_string + date_string + param['suffix'] + \
                           param['compression_suffix']

                if await self.do_download(param["url"], resource, archive_file):
                    # has implicit await?
                    self.do_expansion(archive_file)
                    asyncio.sleep(1)
                    parameter_dataset_name = self.do_conversion(
                        data_file, param, when)

        return parameter_dataset_name

    @staticmethod
    def calculate(vp, t):
        """Short summary.

        Parameters
        ----------
        vp : type
            Description of parameter `vp`.
        t : type
            Description of parameter `t`.

        Returns
        -------
        type
            Description of returned object.

        """
        ea = vp * 0.1
        es = 0.6108 * np.exp(17.27 * t / (t + 237.3))
        d = np.clip(ea - es, None, 0)
        return 6.79 + (27.43 * np.exp(1.05 * d))
