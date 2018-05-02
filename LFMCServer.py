import hug
import asyncio

from marshmallow import fields, pprint
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery, SpatioTemporalQuerySchema
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.results.ModelResult import ModelResultSchema
from lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema

import numpy as np
import pandas as pd
import xarray as xr

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

api = hug.get(on_invalid=hug.redirect.not_found)


@hug.cli()
@hug.get('/fuel', examples='lat1=-10&lon1=110&lat2=-45&lon2=145&start=20180101&finish=20180201', versions=0)
def fuel(lat1: hug.types.number, lon1: hug.types.number, lat2: hug.types.number, lon2: hug.types.number,
         start: fields.String(), finish: fields.String()):
    """ While under development currently just JSON Encodes the query. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    schema = SpatioTemporalQuerySchema()
    response, errors = schema.dump(query)
    return response


@hug.cli()
@hug.get('/fuel', examples='lat1=-10&lon1=110&lat2=-45&lon2=145&start=20180101&finish=20180201&models=dead_fuel,live_fuel', versions=1, output=hug.output_format.pretty_json)
async def fuel(lat1: fields.Decimal(as_string=True),
               lon1: fields.Decimal(as_string=True),
               lat2: fields.Decimal(as_string=True),
               lon2: fields.Decimal(as_string=True),
               start: fields.String(),
               finish: fields.String(),
               models: hug.types.delimited_list(',')):

    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    schema = ModelResultSchema(many=True)
    model_subset = ['dead_fuel']
    if models is not None:
        model_subset = models

    mr = ModelRegister()
    logger.debug("Answering fuel time-series now...")

    response, errors = schema.dump(await asyncio.gather(*[mr.get(model).get_timeseries(query) for model in model_subset]))
    logger.debug(response)

    # Default Response
    query.logResponse()
    return response


@hug.cli()
@hug.post('/fuel', versions=1, output=hug.output_format.pretty_json)
async def fuel(geo_json,
               start: fields.String(),
               finish: fields.String(),
               lat1: fields.Decimal(as_string=True),
               lon1: fields.Decimal(as_string=True),
               lat2: fields.Decimal(as_string=True),
               lon2: fields.Decimal(as_string=True),
               models: hug.types.delimited_list(',')):

    stq = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    query = ShapeQuery(spatio_temporal_query=stq, geo_json=geo_json)

    schema = ModelResultSchema(many=True)
    model_subset = ['dead_fuel']
    if models is not None:
        model_subset = models

    mr = ModelRegister()
    logger.debug("Answering fuel time-series now...")

    response, errors = schema.dump(await asyncio.gather(*[mr.get(model).get_timeseries(query) for model in model_subset]))
    logger.debug(response)

    # Default Response
    query.logResponse()
    return response


@hug.format.content_type('application/x-netcdf4')
@hug.get('/netcdf', versions=1, output=hug.output_format.file)
async def get_netcdf(lat1: fields.Decimal(as_string=True),
                     lon1: fields.Decimal(as_string=True),
                     lat2: fields.Decimal(as_string=True),
                     lon2: fields.Decimal(as_string=True),
                     start: fields.String(),
                     finish: fields.String(),
                     model: hug.types.Text()):
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    model_ = ['dead_fuel']  # Default value! TODO - Remove and return null set
    if model is not None:
        model_ = model

    mr = ModelRegister()
    logger.debug("Answering video query now...")

    return (await asyncio.gather(*[mr.get(model_).get_netcdf(query)]))[0]


@hug.cli()
@hug.get('/mpg', examples='lat1=-10&lon1=110&lat2=-45&lon2=145&start=20180101&finish=20180201&models=dead_fuel,live_fuel', versions=1, output=hug.output_format.mp4_video)
async def mpg(lat1: fields.Decimal(as_string=True),
              lon1: fields.Decimal(as_string=True),
              lat2: fields.Decimal(as_string=True),
              lon2: fields.Decimal(as_string=True),
              start: fields.String(),
              finish: fields.String(),
              model: hug.types.Text()):
    """ Creates an MP4 video of the spatially and temporally restricted query for this model. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)

    model_ = ['dead_fuel']  # Default value! TODO - Remove and return null set
    if model is not None:
        model_ = model

    mr = ModelRegister()
    logger.debug("Answering video query now...")
    return (await asyncio.gather(*[mr.get(model).mpg(query)]))[0]


@hug.cli()
@api.urls('/monitors', versions=1)
def monitors():
    return {"monitors": ["processes", "requests"]}


@hug.cli()
@api.urls('/monitors/requests', versions=1)
def monitor_requests():
    return {"requests": []}


@hug.cli()
@api.urls('/monitors/processes', versions=1)
def monitor_processes():
    return pprint({"processes": asyncio.Task.all_tasks()})


@hug.cli()
@api.urls('/models', versions=1)
def get_models():
    model_register = ModelRegister()
    models_list_schema = ModelsRegisterSchema()
    resp = models_list_schema.dump(model_register)
    return resp


if __name__ == '__main__':
    fuel.interface.cli()
    mpg.interface.cli()
    get_models.interface.cli()
    monitors.interface.cli()
    monitor_processes.interface.cli()
    monitor_requests.interface.cli()
