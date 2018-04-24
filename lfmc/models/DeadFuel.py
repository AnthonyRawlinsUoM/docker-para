import os, os.path
import numpy as np
from lfmc.results.Author import Author
import datetime as dt
import xarray as xr
import rasterio
from pathlib2 import Path
import pathlib
import multiprocessing, subprocess
import urllib.request
from urllib.request import Request, urlopen
from urllib.error import URLError
import asyncio

from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery, SpatioTemporalQuerySchema
from lfmc.models.dummy_results import DummyResults
import math

import logging
logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger(__name__)

class DeadFuelModel(Model):

    def __init__(self):
        
        # TODO - Proper metadata!
        self.name = "dead_fuel"
        
        authors = []
        authors.append(Author(name="Test1", email="test1@example.com", organisation="Test Organisation"))
        authors.append(Author(name="Test2", email="test2@example.com", organisation="Test Organisation"))
        authors.append(Author(name="Test3", email="test3@example.com", organisation="Test Organisation"))
        
        pub_date = dt.datetime(2015,9,9)
                
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"], doi="http://dx.doi.org/10.1016/j.rse.2015.12.010")
        
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
    
    
    async def dataset_files(self, when):
        dataset_file_list = []
        if (self.date_is_cached(when)):
            return self.netcdf_name_for_date(when)
        else:
            dataset_file_list.append(await asyncio.gather( *[self.collect_parameter_data(param, when) for param in self.parameters] ))
        return self.do_compilation(dataset_file_list, when)


    def round_nearest(self, x, a):
        return round(round(x / a) * a, -int(math.floor(math.log10(a))))

    async def answer(self, query:SpatioTemporalQuery):
        dps = []
        fs = await asyncio.gather( *[self.dataset_files(when) for when in query.temporal.dates()] )
        if len(fs) > 0:
            with xr.open_mfdataset(fs) as ds:
                if "observations" in ds.dims:
                    DS = ds.squeeze("observations")
                else:
                    DS = ds                    
                DS.load()
                
                logger.debug("lat1 (%s) is type: %s " % (query.spatial.lat1, type(query.spatial.lat1)))
                logger.debug("lon1 (%s) is type: %s " % (query.spatial.lon1, type(query.spatial.lon1)))
                
                # spatially restrict to extents/bounds
                lat1 = self.round_nearest(np.float64(query.spatial.lat1), 0.05)
                lon1 = self.round_nearest(np.float64(query.spatial.lon1), 0.05)
                lat2 = self.round_nearest(np.float64(query.spatial.lat2), 0.05)
                lon2 = self.round_nearest(np.float64(query.spatial.lon2), 0.05)
                logger.debug("ROUNDED query=\n\tlat1:%s,\n\tlon1:%s,\n\t,\n\tlat2:%s,\n\tlon2:%s\n\n" % (lat1, lon1, lat2, lon2))
                
                sr = DS.sel(latitude=slice(lat1, lat2), longitude=slice(lon1, lon2))
                logger.debug("Spatially restricted dataset is:\n\n%s\n" % (sr))
                # An array of DataPoints
                dps = [self.get_datapoint_for_param(b=sr.isel(time=t), param="DFMC") for t in range(0, len(sr["time"]))]
                ds.close()        
        return ModelResult(self.name, dps)


    def get_datapoint_for_param(self, b, param):
        logger.debug("b is:\n%s" % (b))
        bin_ = b.to_dataframe()
        logger.debug("bin_ is:\n%s" % (bin_) )
        tvalue = str(b["time"].values).replace('.000000000', '.000Z')
        avalue = bin_[param].mean()
        
        logger.debug(">>>> Datapoint creation. (time={}, value={})".format( tvalue, avalue ))
        # aggregates over the space
        return DataPoint(observation_time=tvalue,
                    value=avalue,
                    mean=bin_[param].mean(),
                    minimum=bin_[param].min(),
                    maximum=bin_[param].max(),
                    deviation=bin_[param].std())
        

    async def do_download(self, url, resource, storable):
        uri = url + resource
        logger.debug("\n> Downloading...\n--> Using: {} \n--> to retrieve: {} \n--> Saving to: {}\n".format(url, resource, storable))
        try:
            urllib.request.urlretrieve(uri, storable)
            await asyncio.sleep(0.1)
        except URLError as e:
            emsg = '500 - An unspecified error has occured.'
            if hasattr(e, 'reason'):
                emsg = 'We failed to reach a server.'
                emsg += 'Reason: ' % (e.reason)
            elif hasattr(e, 'code'):
                emsg = 'The server couldn\'t fulfill the request.'
                emsg += 'Error code: ' % (e.code)
            return emsg
            
        logger.debug('\n----> Download complete.\n')
        return storable


    def do_expansion(self, archive_file):
        logger.debug("\n--> Expanding: " + str(archive_file))
        try:
            subprocess.run(["uncompress", "-k", archive_file], stdout=subprocess.PIPE)
        except FileNotFoundError as e:
            logger.warning("\n--> Expanding: %s, failed.\n%s" % (e))
            return False
        try:
            os.remove(archive_file)
        except OSError as e:
            logger.warning("\n--> Removing: %s, failed.\n%s" % (e))
            return False
        return True
    
    def do_conversion(self, fname, param, when):
        """ Converts Arc Grid input files to NetCDF4 """
        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")
        logger.debug("\n--> Processing data for: %s-%s-%s\n--> Converting: %s" % (d, m, y, fname))
        nc_version = "%s.nc" %(fname)    
        arr = xr.open_rasterio("%s" % (fname))
        arr = arr.to_dataset(name="observations", dim=param["prefix"])
        arr = arr.rename({'y':'latitude', 'x':'longitude', 'band':'time'})
        arr.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
        arr.attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y,m,d)
        arr.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
        arr.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
        arr.to_netcdf(nc_version, mode='w', format='NETCDF4')
        arr.close()
        
        return nc_version


    def do_compilation(self, param_datasets, when):
        DFMC_file = self.netcdf_name_for_date(when)

        if len(param_datasets) > 0:
            with xr.open_mfdataset(*param_datasets, concat_dim="observations") as ds:
                
                vp = ds["VP3pm"].isel(time=0)
                tmx = ds["Tmx"].isel(time=0)
                
                dfmc = (DeadFuelModel.calculate(vp, tmx)).expand_dims('time')
                
                y = when.strftime("%Y")
                m = when.strftime("%m")
                d = when.strftime("%d")
                logger.debug("Processing data for: %s-%s-%s" % (d, m, y))
                
                DFMC = dfmc.to_dataset('DFMC')
                DFMC.to_netcdf('/tmp/temp%s-%s-%s.nc' % (d, m, y), format='NETCDF4')
                logger.debug(DFMC)
            
            param_datasets[0].append('/tmp/temp%s-%s-%s.nc' % (d, m, y))
            
            logger.debug(param_datasets)
            
            with xr.open_mfdataset(*param_datasets) as combined:
            
                # DFMC.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
                combined['DFMC'].attrs['DFMC:units'] = "Percentage wet over dry by weight."
                combined['DFMC'].attrs['long_name'] = "Dead Fuel Moisture Content"
                combined['DFMC'].attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y,m,d)
                combined['DFMC'].attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
                combined.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
                combined.attrs['output_frequency'] = "daily"
                combined.attrs['convention'] = "CF-1.4"
                combined.attrs['references'] = "#refs"
                combined.attrs['comment'] = "#comments"
                
                logger.debug(combined)
                
                combined.to_netcdf(DFMC_file, mode='w', format='NETCDF4')
                combined.close()
            
            os.remove('/tmp/temp%s-%s-%s.nc' % (d, m, y))
        # Send file to SWIFT Storage here?

        return DFMC_file
    
    def netcdf_name_for_date(self, when):
        return "{}{}_{}{}".format(self.outputs["readings"]["path"],
                                self.outputs["readings"]["prefix"],
                                when.strftime("%Y%m%d"),
                                self.outputs["readings"]["suffix"])

    def date_is_cached(self, when):
        file_path = Path(self.outputs["readings"]['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)
            
        return Path(self.netcdf_name_for_date(when)).is_file()

    async def collect_parameter_data(self, param, when):
        """ Collects input parameters for the model as determined by the metadata. """
        param = self.parameters[param]
        file_path = Path(param['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)
        
        parameter_dataset_name = file_path / (param['prefix'] + "_" +\
                param['dataset'])
        if parameter_dataset_name.is_file():
            return parameter_dataset_name
        else:
            data_file = file_path / (param['prefix'] + "_" +\
                    when.strftime("%Y%m%d") +\
                    param['suffix'])
                
            archive_file = Path(str(data_file) + param['compression_suffix'])
        
            if data_file.is_file():
                parameter_dataset_name = self.do_conversion(data_file, param, when)
                
            elif not data_file.is_file() and archive_file.is_file():
                data_file= self.do_expansion(archive_file)
                parameter_dataset_name = self.do_conversion(data_file, param, when)
                # Remove the archive?
                
            elif not data_file.is_file() and not archive_file.is_file():
                datestring = when.strftime("%Y%m%d")
                resource = datestring + datestring + param['suffix'] +\
                            param['compression_suffix']
                
                if (await self.do_download(param["url"], resource, archive_file)):
                    # has implicit await?
                    self.do_expansion(archive_file)
                    parameter_dataset_name = self.do_conversion(data_file, param, when)
            
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

