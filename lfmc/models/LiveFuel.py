import pandas as pd
import os, os.path
import numpy as np
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults


class LiveFuelModel:
    
    def __init__(self):
        self.name = "live_fuel"
    
    def calculate(df:pd.DataFrame):
        
        return 0
    
    # def check_for_netrc(self):
    #     cmdline("cat /home/arawlins/.netrc")
    # 
    # def used_granules(self):
    # """ Generates a list of tuples describing HV coords for granules that are used
    # to generate a MODIS composite covering Australia.
    # """
    # acceptable = []
    # for h in range(27,31):
    #     for v in range(9, 13):
    #         acceptable.append((h,v))
    # return acceptable
    # 
    # def is_acceptable_granule(self, granule):
    #     return hv_for_modis_granule(granule) in used_granules()
    # 
    # def hv_for_modis_granule(self, granule):
    #     """ Extracts HV grid coords from naming conventions of HDF-EOS file.
    #     Assumes input is a file name string conforming to EOS naming conventions."""
    #     parts = granule.split('.')
    #     hv_component = parts[2].split('v')
    #     h = int(hv_component[0].replace('h', ''))
    #     v = int(hv_component[1])
    #     return (h, v)
    # 
    # def date_for_modis_granule(self, granule) -> datetime:
    #     """ Extracts the observation date from the naming conventions of a HDF-EOS file"""
    #     # unravel naming conventions
    #     parts = granule.split('.')
    # 
    #     # set the key for subgrouping to be the date of observation by parsing the Julian Date
    #     return datetime.strptime((parts[1].replace('A', '')), '%Y%j')
    # 
    # 
    # def get_hv(self, url):
    #     """ Parses a HDF_EOS URI to extract HV coords """
    #     uri_parts = granule.split('/')
    #     return hv_for_modis_granule(uri_parts[-1])
    # 
    # def retrieve_earth_observation_data(url):
    #     """ Please note: Requires a vaild .netrc file in users home directory! """
    #     os.chdir(inputs)
    # 
    #     file_name = url.split('/')[-1]
    # 
    #     xml_name = file_name+'.xml'
    #     hdf5_name = file_name+'_lfmc.nc'
    # 
    #     hdf_file = Path(file_name)
    #     xml_file = Path(xml_name)
    #     hdf5_name = Path(hdf5_name)
    # 
    #     if not swift_check_lfmc(hdf5_name):
    #         # No LFMC Product for this granule
    #         if not swift_check_modis(file_name):
    #             # No Granule held in cloud
    #             if (not hdf_file.is_file()) or (os.path.getsize(hdf_file) == 0):
    #                 # No local file either!
    #                 print("[Downloading]" + file_name)
    #                 # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url, file_name))
    #                 os.system("wget -L --accept hdf --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (url, file_name))
    #             if hdf_file.is_file():
    #                 # Local file now exists
    #                 # TODO -> Process the file and calc the Live FM here!
    #                 with convert_modis_granule_file_to_lfmc(hdf_file) as xlfmc:
    #                     # Upload the LFMC HDF5 file to swift API as well.
    #                     swift_put_lfmc(xlfmc)
    #                 else:
    #                     raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))
    # 
    #                 # Make sure to save the original source
    #                 if swift_put_modis(file_name):
    #                     os.remove(file_name)
    #         else:
    #             # MODIS Source exists but derived LFMC HDF5 does not!
    #             swift_get_modis(file_name) 
    # 
    #             # TODO -> Process the file and calc the Live FM here!\
    #             with convert_modis_granule_file_to_lfmc(hdf_file) as xlfmc:
    #                 # Upload the LFMC HDF5 file to swift API as well.
    #                 swift_put_lfmc(xlfmc)
    #             else:
    #                 raise CalculationError('Processing LFMC for Granule: %s failed!' % (hdf_file))
    # 
    #         print("[OK] %s" % (file_name))
    # 
    #         if(not swift_check_modis(xml_name)):
    #             if (not xml_file.is_file())  or (os.path.getsize(xml_file) == 0):
    #                 print("[Downloading] " + xml_name)
    #                 os.system("wget -L --accept xml --reject html --load-cookies=cookiefile --save-cookies=cookiefile %s -O %s" % (url, xml_name))
    #                 # cmdline("curl -n -L -c cookiefile -b cookiefile %s --output %s" % (url+'.xml', xml_name))
    #             if xml_file.is_file():
    #                 if swift_put_modis(xml_name):
    #                     os.remove(xml_name)
    #         print("[OK] %s" % (xml_name))
    # 
    #     else:
    #         # LFMC exists for this granule in Nectar Cloud already!
    #         print('LFMC exists for this granule in Nectar Cloud already!')
    # 
    # 
    # def group_queue_by_date(queue):
    #     grouped = {}
    #     # Sort the queue and group by date/granule HV coords  
    #     for elem in queue:
    #         fname = elem.split('/')[-1]
    #         if fname.lower().endswith('.hdf'):
    #             key = date_for_modis_granule(fname).strftime('%Y-%m-%d')
    #             grouped.setdefault(key, []).append(elem)
    # 
    #     return grouped
    # 
    # def convert_modis_granule_file_to_lfmc(fobj):
    #     b1 = read_hdfeos_df_as_xarray(fobj, 'sur_refl_b01')
    #     b3 = read_hdfeos_df_as_xarray(fobj, 'sur_refl_b03')
    #     b4 = read_hdfeos_df_as_xarray(fobj, 'sur_refl_b04')
    #     vari = ((b4 - b1) / (b4 + b1 - b3 )).clip(-1,1)
    # 
    #     # Calc spectral index
    #     vari_max = vari.max()
    #     vari_min = vari.min()
    #     vari_range = vari_max - vari_min
    #     rvari = ((vari - vari_min / vari_range)).clip(0, 1)  # SI
    #     data = np.reshape(np.array(52.51 **(1.36*rvari)), (2400,2400)).astype(np.float64)
    # 
    #     xrd = xr.DataArray(data, coords=b1.coords, dims=b1.dims)
    #     xrd.name = 'lfmc'
    #     xrd = xrd.to_dataset().expand_dims('time')
    #     xrd.coords['time'] = [datetime.datetime(captured.year, captured.month, captured.day)]
    #     xrd.attrs['created'] = "%s" % (datetime.datetime.now().strftime("%d-%m-%Y"))
    #     xrd.attrs['crs'] = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs '
    #     xrd.attrs['time:units'] = 'days since %s' % (captured.strftime("%Y-%m-%d"))
    #     xrd.load()
    # #           xrd.to_netcdf(fobj['name']+'_lfmc.nc')
    # 
    #     lons = np.array(xrd.variables['longitude'][:])
    #     lats = np.array(xrd.variables['latitude'][:])
    # 
    #     # print("Length of all longs is: %d" % len(lons))
    # 
    #     fm = np.array(xrd['lfmc'].data)
    #     return fm
    # 
    # 
    # def read_hdfeos_df_as_xarray(FILE_NAME, DATAFIELD_NAME):
    # 
    #     GRID_NAME = 'MOD_Grid_500m_Surface_Reflectance'
    #     gname = 'HDF4_EOS:EOS_GRID:"{0}":{1}:{2}'.format(FILE_NAME,
    #                                                      GRID_NAME,
    #                                                      DATAFIELD_NAME)
    #     gdset = gdal.Open(gname)
    #     data = gdset.ReadAsArray().astype(np.float64)
    # 
    #     # Construct the grid.
    #     x0, xinc, _, y0, _, yinc = gdset.GetGeoTransform()
    #     nx, ny = (gdset.RasterXSize, gdset.RasterYSize)
    #     x = np.linspace(x0, x0 + xinc*nx, nx)
    #     y = np.linspace(y0, y0 + yinc*ny, ny)
    #     xv, yv = np.meshgrid(x, y)
    # 
    #     # In basemap, the sinusoidal projection is global, so we won't use it.
    #     # Instead we'll convert the grid back to lat/lons.
    #     sinu = pyproj.Proj("+proj=sinu +R=6371007.181 +nadgrids=@null +wktext")
    #     wgs84 = pyproj.Proj("+init=EPSG:4326") 
    #     lon, lat= pyproj.transform(sinu, wgs84, xv, yv)
    # 
    #     # Read the attributes.
    #     meta = gdset.GetMetadata()
    #     long_name = meta['long_name']        
    #     units = meta['units']
    #     _FillValue = np.float64(meta['_FillValue'])
    #     scale_factor = np.float64(meta['scale_factor'])
    #     valid_range = [np.float64(x) for x in meta['valid_range'].split(', ')] 
    # 
    #     del gdset
    # 
    #     invalid = np.logical_or(data > valid_range[1],
    #                             data < valid_range[0])
    #     invalid = np.logical_or(invalid, data == _FillValue)
    #     data[invalid] = np.nan
    #     data = data / scale_factor
    # 
    #     # TODO - Reinstate data masking!
    #     # data = np.ma.masked_array(data, np.isnan(data))
    # 
    #     df = pd.DataFrame(data, index=lat, columns=lon)
    #     xrd = xr.DataArray(df)
    #     xrd.name=DATAFIELD_NAME
    #     xrd = xrd.rename({'dim_0':'latitude'})
    #     xrd = xrd.rename({'dim_1':'longitude'})
    # 
    #     return xrd
    
    
    def answer(self, query: SpatioTemporalQuery):
        # MAGIC HAPPENS HERE

        # An array of mockup DataPoints for testing only
        dr = DummyResults()
        dps = dr.dummy_data(query)
        return ModelResult(self.name, dps)