import asyncio
import subprocess
import urllib.request
from urllib.error import URLError

import hug
import os
import xarray as xr
from abc import abstractmethod
from marshmallow import Schema, fields
from pathlib2 import Path

from lfmc.query import ShapeQuery
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery


import logging
logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class Model():
    def __init__(self):
        self.name = "Base Model Class"
        self.metadata = {}
        self.parameters = {}
        self.outputs = {}
        self.tolerance = 0

    def __init__(self, model):
        """ Copy-constructor """
        self.name = model.name
        self.metadata = model.metadata
        self.parameters = model.parameters
        self.outputs = model.outputs
        self.tolerance = model.tolerance

    @staticmethod
    def path():
        return '/FuelModels/'

    # @abstractmethod
    # def get_metadata(self):
    #     return self.metadata
    #
    # @abstractmethod
    # def get_parameters(self):
    #     return self.parameters
    #
    # @abstractmethod
    # def get_outputs(self):
    #     return self.outputs
    #
    # @abstractmethod
    # def get_tolerance(self):
    #     return self.tolerance

    # @abstractmethod
    # def get_timeseries(self, query: SpatioTemporalQuery) -> ModelResult:
    #     return None
    #
    # @abstractmethod
    # def get_resultcube(self, query: SpatioTemporalQuery) -> xr.DataArray:
    #     return None



    def date_is_cached(self, when):

        # TODO -Swift Object Storage Checking

        file_path = Path(self.outputs["readings"]['path'])
        if not file_path.is_dir():
            os.makedirs(file_path)

        ok = Path(self.netcdf_name_for_date(when)).is_file()
        logger.debug("\n--> Checking for existence of NetCDF @ %s for %s: %s" %
                     (file_path, when.strftime("%d %m %Y"), ok))

        # TODO -if OK put the file into Swift Storage

        return ok

    @staticmethod
    async def do_download(url, resource, path):
        uri = url + resource
        logger.debug(
            "\n> Downloading...\n--> Using: {} \n--> to retrieve: {} \n--> Saving to: {}\n".format(url, resource,
                                                                                                   path))
        try:
            urllib.request.urlretrieve(uri, path)
            await asyncio.sleep(0.1)
        except URLError as e:
            msg = '500 - An unspecified error has occurred.'
            if hasattr(e, 'reason'):
                msg = 'We failed to reach a server.'
                msg += 'Reason: %s' % e.reason
            elif hasattr(e, 'code'):
                msg = 'The server could not fulfill the request.'
                msg += 'Error code: %s' % e.code
            return msg

        logger.debug('\n----> Download complete.\n')
        return path

    @staticmethod
    def do_expansion(archive_file):
        logger.info("\n--> Expanding: %s" % archive_file)
        try:
            subprocess.run(["uncompress", "-k", archive_file], stdout=subprocess.PIPE)
        except FileNotFoundError as e:
            logger.warning("\n--> Expanding: %s, failed.\n%s" % (archive_file, e))
            return False
        try:
            os.remove(archive_file)
        except OSError as e:
            logger.warning("\n--> Removing: %s, failed.\n%s" % (archive_file, e))
            return False
        return True

    @staticmethod
    async def get_datapoint_for_param(b, param):
        """
        Takes the mean min and max values for datapoints at a particular time slice.
        :param b:
        :param param:
        :return:
        """

        bin_ = b.to_dataframe()

        # TODO - This is a quick hack to massage the datetime format into a markup suitable for D3 & ngx-charts!
        tvalue = str(b["time"].values).replace('.000000000', '.000Z')
        avalue = bin_[param].mean()

        logger.debug(
            "\n>>>> Datapoint creation. (time={}, value={})".format(tvalue, avalue))

        return DataPoint(observation_time=tvalue,
                         value=avalue,
                         mean=bin_[param].mean(),
                         minimum=bin_[param].min(),
                         maximum=bin_[param].max(),
                         deviation=bin_[param].std())

    pass


class ModelSchema(Schema):
    name = hug.types.Text
    # metadata = fields.Nested(ModelMetaDataSchema, many=False)
    parameters = fields.String()
    outputs = fields.String()
    # tolerance = fields.Decimal(as_string=True)
