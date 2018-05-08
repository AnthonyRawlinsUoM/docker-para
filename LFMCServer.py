import hug
import asyncio

from marshmallow import fields, pprint
from lfmc.query.ShapeQuery import ShapeQuery
from lfmc.results.ModelResult import ModelResultSchema
from lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema
from lfmc.monitor.RequestMonitor import RequestMonitor

import numpy as np
import pandas as pd
import xarray as xr

import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


api_ = hug.API(__name__)
api_.http.add_middleware(hug.middleware.CORSMiddleware(api_, max_age=10))

api = hug.get(on_invalid=hug.redirect.not_found)


suffix_output = hug.output_format.suffix({'.json': hug.output_format.pretty_json,
                                          '.mp4': hug.output_format.mp4_video,
                                          '.mov': hug.output_format.mov_video,
                                          '.nc': hug.output_format.file})

content_output = hug.output_format.on_content_type({'application/x-netcdf4': hug.output_format.file})


@hug.cli()
@hug.post(('/fuel', '/fuel.json', '/fuel.mp4', '/fuel.mov', '/fuel.nc'), versions=1, output=suffix_output)
async def fuel(geo_json,
               start: fields.String(),
               finish: fields.String(),
               weighted: fields.Bool(),
               models: hug.types.delimited_list(','),
               response_as: hug.types.number):
    query = ShapeQuery(start=start, finish=finish,
                       geo_json=geo_json, weighted=weighted)

    rm = RequestMonitor()
    rm.log_request(query)

    logger.debug(query)

    # Which models are we working with?
    model_subset = ['dead_fuel']
    if models is not None:
        model_subset = models

    mr = ModelRegister()
    # logger.debug("Answering fuel geojson shaped time-series now...")

    # Default case
    response = None

    # Switch schema on response_as
    # 0. Timeseries JSON
    # 1. MP4
    # 2. NetCDF
    if response_as == 0:
        logger.debug("Responding to JSON query...")
        schema = ModelResultSchema(many=True)
        response, errors = schema.dump(
            await asyncio.gather(*[mr.get(model).get_shaped_timeseries(query) for model in model_subset]))
        logger.debug(response)

    elif response_as == 1:
        logger.debug("Responding to MP4 query...")
        # TODO - only returns first model at the moment
        response = (await asyncio.gather(*[mr.get("dead_fuel").mpg(query)]))[0]
        # response = await mr.get("dead_fuel").mpg(query)
        logger.debug(response)

    elif response_as == 2:
        logger.debug("Responding to NETCDF query...")
        # TODO - only returns first model at the moment
        response = (await asyncio.gather(*[mr.get(model).get_netcdf(query) for model in model_subset]))[0]
        logger.debug(response)

    # Default Response
    query.logResponse()
    return response


@hug.cli()
@api.urls('/monitors', versions=1)
def monitors():
    return {"monitors": ["processes", "requests"]}


@hug.cli()
@api.urls('/monitors/requests/complete', versions=1)
def monitor_complete_requests():
    rm = RequestMonitor()
    return rm.completed_requests()


@hug.cli()
@api.urls('/monitors/requests/all', versions=1)
def monitor_all_requests():
    rm = RequestMonitor()
    return rm.all_requests()


@hug.cli()
@api.urls('/monitors/requests/active', versions=1)
def monitor_active_requests():
    rm = RequestMonitor()
    return rm.open_requests()


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
    get_models.interface.cli()
    monitors.interface.cli()
    monitor_processes.interface.cli()
    monitor_all_requests.interface.cli()
    monitor_active_requests.interface.cli()
    monitor_complete_requests.interface.cli()
