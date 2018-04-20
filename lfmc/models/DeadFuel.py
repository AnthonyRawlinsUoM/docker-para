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
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults

import logging
logging.basicConfig(filename='/tmp/myapp.log', level=logging.DEBUG, 
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
            dataset_file_list.append(netcdf_name_for_date(when))
        else:
            dataset_file_list.append(await asyncio.gather( *[self.collect_parameter_data(param, when) for param in self.parameters] ))
        return self.do_compilation(dataset_file_list, when)
    
    async def answer(self, query: SpatioTemporalQuery,):
        
        dps = [DataPoint(0,0,0,0,0,0)]
        fs = await asyncio.gather( *[self.dataset_files(when) for when in query.dates()] )
        print(fs)
        with xr.open_mfdataset(fs) as ds:
            
            print(ds)
            # spatially restrict to extents/bounds
            # sr = ds["dfmc"].sel(latitude = slice(query.spatial.lat1, query.spatial.lat2), longitude = slice(query.spatial.lon1, query.spatial.lon2))
            
            # temporal restriction to extents/bounds should already have happened
            # ts = sr.sel(time=slice(query.temporal.start.strftime('%Y-%m-%d'), query.temporal.finish.strftime('%Y-%m-%d')))
            # ts = ds["dfmc"].sel(time=slice(query.temporal.start.strftime('%Y-%m-%d'), query.temporal.finish.strftime('%Y-%m-%d')))
            # An array of DataPoints
            
            
            # loop through time
            for t in ds["dfmc"]:
                dps.append(DataPoint(t.time,
                                      t.mean(),
                                      t.mean(),
                                      t.min(),
                                      t.max(),
                                      t.std()
                                      ))
                # aggregate over space
                # get stats
            ds.close()
        
        return ModelResult(self.name, dps)

    async def do_download(self, url, resource, storable):
        print(">> Downloading...")
        uri = url + resource
        print(">> Using: {} \n>> to retrieve: {} \n>> Saving to: {}".format(url, resource, storable))
        try:
            urllib.request.urlretrieve(uri, storable)
            await asyncio.sleep(1)
        except URLError as e:
            if hasattr(e, 'reason'):
                print('We failed to reach a server.')
                print('Reason: ', e.reason)
            elif hasattr(e, 'code'):
                print('The server couldn\'t fulfill the request.')
                print('Error code: ', e.code)
            return False
            
        print('>>> Download complete.')
        return storable


    def do_expansion(self, archive_file):
        print(">> Expanding: " + str(archive_file))
        return subprocess.run(["uncompress", "-k", archive_file], stdout=subprocess.PIPE)
    
    def do_conversion(self, fname, param, when):
        """ Converts Arc Grid input files to NetCDF4 """
        y = when.strftime("%Y")
        m = when.strftime("%m")
        d = when.strftime("%d")
        print("Processing data for: %s-%s-%s" % (d, m, y))
        print("Converting: %s" % (fname))
        nc_version = "%s.nc" %(fname)    
        arr = xr.open_rasterio("%s" % (fname))
        arr.name = param["prefix"]
        
        arr = arr.to_dataset(dim='time')
        
        arr = arr.rename({'y':'latitude', 'x':'longitude'})
        arr.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
        arr.attrs['units'] = "Percent wet over dry by weight."
        arr.attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y,m,d)
        arr.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
        arr.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
        arr.to_netcdf(nc_version, mode='w', format='NETCDF4')
        arr.close()
        
        return nc_version


    def do_compilation(self, param_datasets, when):
        DFMC_file = self.netcdf_name_for_date(when)
        
        
        with xr.open_mfdataset(*param_datasets, concat_dim="band") as ds:
        
            vp = ds.sel({band: "VP3pm"})
            tmx = ds.sel({band: "Tmx"})
            
            dfmc = DeadFuelModel.calculate(vp, tmx)
            
            print(dfmc)
            
            y = when.strftime("%Y")
            m = when.strftime("%m")
            d = when.strftime("%d")
            print("Processing data for: %s-%s-%s" % (d, m, y))
            
            
            DFMC = xr.Dataset({'dfmc': (('latitude', 'longitude', 'time'), dfmc)})
            # DFMC.name = 'dfmc'
            # DFMC.coords['time'] = [dt.datetime(int(y), int(m), int(d))]
            DFMC.attrs['units'] = "Percentang wet over dry by weight."
            DFMC.attrs['long_name'] = "Dead Fuel Moisture Content"
            DFMC.attrs['time:units'] = "Days since %s-%s-%s 00:00:00" % (y,m,d)
            DFMC.attrs['crs'] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs "
            DFMC.attrs['created'] = "%s" % (dt.datetime.now().strftime("%d-%m-%Y"))
            DFMC.to_netcdf(DFMC_file, mode='w', format='NETCDF4')
            DFMC.close()
        
        # Send file to SWIFT Storage here?

        return DFMC_file
    
    def netcdf_name_for_date(self, when):
        return "{}_{}{}".format(self.outputs["readings"]["prefix"],
                               when,
                               self.outputs["readings"]["suffix"])

    def date_is_cached(self, when):
        file_path = Path(self.outputs["readings"]['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)
            
        return Path(self.outputs["readings"]["path"] + self.netcdf_name_for_date(when)).is_file()

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
                
            elif not data_file.is_file() and not archive_file.is_file():
                datestring = when.strftime("%Y%m%d")
                resource = datestring + datestring + param['suffix'] +\
                            param['compression_suffix']
                
                if (self.do_download(param["url"], resource, archive_file)):
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

