import asyncio

import gdal
import pandas as pd
import os, os.path
import numpy as np
import pyproj
import requests
import xarray as xr
import datetime as dt

from pathlib2 import Path

from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.models.dummy_results import DummyResults
from lfmc.query import ShapeQuery
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.resource.SwiftStorage import SwiftStorage
from lfmc.results.Author import Author
from lfmc.results.ModelResult import ModelResult

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class LiveFuelModel(Model):

    def __init__(self):
        self.name = "live_fuel"

        # TODO - Proper metadata!
        authors = [
            Author(name="Test1", email="test1@example.com", organisation="Test Organisation"),
            Author(name="Test2", email="test2@example.com", organisation="Test Organisation"),
            Author(name="Test3", email="test3@example.com", organisation="Test Organisation")
        ]
        pub_date = dt.datetime(2015, 9, 9)

        # Which products from NASA
        product = "MOD09A1"
        version = "6"

        # AIO bounding box lower left longitude, lower left latitude, upper right longitude, upper right latitude.
        bbox = "108.0000,-45.0000,155.0000,-10.0000"

        self.modis_meta = product, version, bbox

        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010")
        self.path = os.path.abspath(Model.path() + 'Live_FM') + '/'
        self.ident = "Live Fuels"
        self.code = "LFMC"
        self.parameters = {
            "surface relectance band": {
                "var": "SRB",
                "path": "",
                "url": "",
                "prefix": "SRB",
                "suffix": ".hdf",
                "dataset": ".hdf",
                "compression_suffix": ".gz"
            }
        }
        self.outputs = {
            "type": "fuel moisture",
            "readings": {
                "path": "LiveFM",
                "url": "LiveFM",
                "prefix": "LFMC",
                "suffix": "_lfmc.nc",
            }
        }

        self.storage_engine = SwiftStorage({"parameters": self.parameters, "outputs": self.outputs})

    # @deprecated
    # def check_for_netrc(self):
    #     cmdline("cat /home/arawlins/.netrc")
    #

    def netcdf_name_for_date(self, when):
        return "{}{}_{}{}".format((self.outputs["readings"]["path"],
                                   self.outputs["readings"]["prefix"],
                                   when.strftime("%Y%m%d"),
                                   self.outputs["readings"]["suffix"]))

    @staticmethod
    def used_granules():
        """ Generates a list of tuples describing HV coords for granules that are used
        to generate a MODIS composite covering Australia.
        """
        return [(h, v) for h in range(27, 31) for v in range(9, 13)]

    def is_acceptable_granule(self, granule):
        return self.hv_for_modis_granule(granule) in LiveFuelModel.used_granules()

    @staticmethod
    def hv_for_modis_granule(granule):
        """ Extracts HV grid coords from naming conventions of HDF-EOS file.
        Assumes input is a file name string conforming to EOS naming conventions."""
        parts = granule.split('.')
        hv_component = parts[2].split('v')
        h = int(hv_component[0].replace('h', ''))
        v = int(hv_component[1])
        return h, v

    @staticmethod
    def date_for_modis_granule(granule) -> dt.datetime():
        """ Extracts the observation date from the naming conventions of a HDF-EOS file"""
        # unravel naming conventions
        parts = granule.split('.')

        # set the key for subgrouping to be the date of observation by parsing the Julian Date
        return dt.datetime.strptime((parts[1].replace('A', '')), '%Y%j')

    def get_hv(self, url):
        """ Parses a HDF_EOS URI to extract HV coords """
        uri_parts = url.split('/')
        return self.hv_for_modis_granule(uri_parts[-1])

    def retrieve_earth_observation_data(self, url):
        """ Please note: Requires a vaild .netrc file in users home directory! """
        # os.chdir(self.inputs)  <-- REMOVED for Dockerization purposes!

        file_name = url.split('/')[-1]

        xml_name = file_name + '.xml'
        hdf5_name = file_name + '_lfmc.nc'

        hdf_file = Path(file_name)
        xml_file = Path(xml_name)
        hdf5_name = Path(hdf5_name)

        if not self.storage_engine.swift_check_lfmc(hdf5_name):
            # No LFMC Product for this granule
            if not self.storage_engine.swift_check_modis(file_name):
                # No Granule held in cloud
                if (not hdf_file.is_file()) or (os.path.getsize(hdf_file) == 0):
                    # No local file either!
                    logger.debug("[Downloading]" + file_name)
                    # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url, file_name))
                    os.system(
                        "wget -L --accept hdf --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
                        url, file_name))
                if hdf_file.is_file():
                    # Local file now exists
                    # TODO -> Process the file and calc the Live FM here!
                    with self.convert_modis_granule_file_to_lfmc(hdf_file) as xlfmc:
                        # Upload the LFMC HDF5 file to swift API as well.
                        self.storage_engine.swift_put_lfmc(xlfmc)
                    # else:
                    #     raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))

                    # Make sure to save the original source
                    if self.storage_engine.swift_put_modis(file_name):
                        os.remove(file_name)
            else:
                # MODIS Source exists but derived LFMC HDF5 does not!
                self.storage_engine.swift_get_modis(file_name)

                # TODO -> Process the file and calc the Live FM here!\
                with self.convert_modis_granule_file_to_lfmc(hdf_file) as xlfmc:
                    # Upload the LFMC HDF5 file to swift API as well.
                    self.storage_engine.swift_put_lfmc(xlfmc)

                # else:
                #     raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))

            logger.debug("[OK] %s" % (file_name))

            if not self.storage_engine.swift_check_modis(xml_name):
                if (not xml_file.is_file()) or (os.path.getsize(xml_file) == 0):
                    logger.debug("[Downloading] " + xml_name)
                    os.system(
                        "wget -L --accept xml --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (
                        url, xml_name))
                    # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url+'.xml', xml_name))
                if xml_file.is_file():
                    if self.storage_engine.swift_put_modis(xml_name):
                        os.remove(xml_name)
            logger.debug("[OK] %s" % (xml_name))

        else:
            # LFMC exists for this granule in Nectar Cloud already!
            logger.debug('LFMC exists for this granule in Nectar Cloud already!')
        return hdf_file

    def group_queue_by_date(self, queue):
        grouped = {}
        # Sort the queue and group by date/granule HV coords  
        for elem in queue:
            fname = elem.split('/')[-1]
            if fname.lower().endswith('.hdf'):
                key = self.date_for_modis_granule(fname).strftime('%Y-%m-%d')
                grouped.setdefault(key, []).append(elem)

        return grouped

    def convert_modis_granule_file_to_lfmc(self, fobj) -> xr.DataArray:
        b1 = self.read_hdfeos_df_as_xarray(fobj, 'sur_refl_b01')
        b3 = self.read_hdfeos_df_as_xarray(fobj, 'sur_refl_b03')
        b4 = self.read_hdfeos_df_as_xarray(fobj, 'sur_refl_b04')
        vari = ((b4 - b1) / (b4 + b1 - b3)).clip(-1, 1)

        # Calc spectral index
        vari_max = vari.max()
        vari_min = vari.min()
        vari_range = vari_max - vari_min
        rvari = (vari - vari_min / vari_range).clip(0, 1)  # SI
        data = np.reshape(np.array(52.51 ** (1.36 * rvari)), (2400, 2400)).astype(np.float64)

        captured = b1.attrs['time']  #TODO <-- DEBUG THIS ATTRIBUTE is it correct?

        xrd = xr.DataArray(data, coords=b1.coords, dims=b1.dims)
        xrd.name = self.outputs["readings"]["prefix"]
        xrd = xrd.to_dataset().expand_dims('time')
        xrd.coords['time'] = [dt.datetime(captured.year, captured.month, captured.day)]
        xrd.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
        xrd.attrs['crs'] = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '
        xrd.attrs['time:units'] = 'days since %s' % (captured.strftime("%Y-%m-%d"))
        xrd.load()
        #           xrd.to_netcdf(fobj['name']+'_lfmc.nc')

        # lons = np.array(xrd.variables['longitude'][:])
        # lats = np.array(xrd.variables['latitude'][:])

        # logger.debug("Length of all longs is: %d" % len(lons))

        fm = np.array(xrd[xrd.name].data)
        return fm

    def read_hdfeos_df_as_xarray(self, file_name, data_field_name):

        grid_name = 'MOD_Grid_500m_Surface_Reflectance'
        gname = 'HDF4_EOS:EOS_GRID:"{0}":{1}:{2}'.format(file_name,
                                                         grid_name,
                                                         data_field_name)
        gdset = gdal.Open(gname)
        data = gdset.ReadAsArray().astype(np.float64)

        # Construct the grid.
        x0, xinc, _, y0, _, yinc = gdset.GetGeoTransform()
        nx, ny = (gdset.RasterXSize, gdset.RasterYSize)
        x = np.linspace(x0, x0 + xinc * nx, nx)
        y = np.linspace(y0, y0 + yinc * ny, ny)
        xv, yv = np.meshgrid(x, y)

        # In basemap, the sinusoidal projection is global, so we won't use it.
        # Instead we'll convert the grid back to lat/lons.
        sinu = pyproj.Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")
        wgs84 = pyproj.Proj("+init=EPSG:4326")
        lon, lat = pyproj.transform(sinu, wgs84, xv, yv)

        # Read the attributes.
        meta = gdset.GetMetadata()
        long_name = meta['long_name']
        units = meta['units']
        _FillValue = np.float64(meta['_FillValue'])
        scale_factor = np.float64(meta['scale_factor'])
        valid_range = [np.float64(x) for x in meta['valid_range'].split(', ')]

        del gdset

        invalid = np.logical_or(data > valid_range[1],
                                data < valid_range[0])
        invalid = np.logical_or(invalid, data == _FillValue)
        data[invalid] = np.nan
        data = data / scale_factor

        # TODO - Reinstate data masking!
        # data = np.ma.masked_array(data, np.isnan(data))

        df = pd.DataFrame(data, index=lat, columns=lon)
        xrd = xr.DataArray(df)
        xrd.name = data_field_name
        xrd = xrd.rename({'dim_0': 'latitude'})
        xrd = xrd.rename({'dim_1': 'longitude'})

        return xrd

    def get_timeseries(self, query: SpatioTemporalQuery) -> ModelResult:
        # MAGIC HAPPENS HERE
        # An array of mockup DataPoints for testing only
        dr = DummyResults()
        dps = dr.dummy_data(query)
        return ModelResult(self.name, dps)

    def get_resultcube(self, query: SpatioTemporalQuery) -> xr.DataArray:
        return xr.DataArray([])

    # ShapeQuery
    async def get_shaped_resultcube(self, shape_query: ShapeQuery) -> xr.DataArray:
        sr = None
        fs = await asyncio.gather(*[self.dataset_files(when) for when in shape_query.temporal.dates()])
        asyncio.sleep(1)
        if len(fs) > 0:
            with xr.open_mfdataset(fs) as ds:
                if "observations" in ds.dims:
                    sr = ds.squeeze("observations")
        if sr is not None and len(sr.data) > 0:
            return shape_query.apply_mask_to(sr)
        else:
            return xr.DataArray([])

    async def dataset_files(self, when):
        if self.date_is_cached(when):
            return self.netcdf_name_for_date(when)  # TODO - Overload this and use 8 Day product indexing
        else:
            ds_files = await self.collect_granules(when)
            asyncio.sleep(1)
            return ds_files

    async def collect_granules(self, when):
        r = self.build_inventory_request_url(when)
        inventory = await asyncio.gather(*[self.get_inventory_for_request(r)])
        collected = []
        os.chdir(self.path)
        if len(inventory) > 0:
            # Check the indexed files and don't replicate work!
            # Also check the current download queue to see if the granule is currently being downloaded.
            # split the queue by task status
            grouped_by_date = self.group_queue_by_date(inventory)
            for urls in list(grouped_by_date.values()):
                for url in urls:
                    rok = await self.retrieve_earth_observation_data(url)
                    asyncio.sleep(1)
                    collected.append(rok)
        return collected

    async def get_shaped_timeseries(self, query: ShapeQuery) -> ModelResult:
        logger.debug(
            "\n--->>> Shape Query Called successfully on %s Model!! <<<---" % self.name)
        logger.debug("Spatial Component is: \n%s" % str(query.spatial))
        logger.debug("Temporal Component is: \n%s" % str(query.temporal))
        logger.debug("\nDerived LAT1: %s\nDerived LON1: %s\nDerived LAT2: %s\nDerived LON2: %s" %
                     query.spatial.expanded(0.05))

        sr = await (self.get_shaped_resultcube(query))
        asyncio.sleep(1)
        dps = [self.get_datapoint_for_param(b=sr.isel(time=t), param=self.outputs["readings"]["prefix"])
               for t in range(0, len(sr["time"]))]
        return ModelResult(model_name=self.name, data_points=dps)

    def get_inventory_for_request(self, url_string):
        r = requests.get(url_string)
        queue = []
        if r.status_code == 200:
            granules = r.text.split('\n')
            for line in granules:
                if len(line) > 0 and self.is_acceptable_granule(line):
                    queue.append(line)
        else:
            raise ("[Error] Can't continue. Didn't receive what we expected from USGS / NASA.")
        return queue

    def build_inventory_request_url(self, when):
        """ Uses USGS LPDAAC inventory service to select files.
        Gathers entirety of Australia rather than using query BBOX.
        """
        product, version, bbox = self.modis_meta
        # when = query.temporal.start.strftime("%Y-%m-%d") + ',' + query.temporal.finish.strftime("%Y-%m-%d")
        return "https://lpdaacsvc.cr.usgs.gov/services/inventory?product=" + product + "&version=" + \
               version + "&bbox=" + bbox + "&date=" + when + "&output=text"