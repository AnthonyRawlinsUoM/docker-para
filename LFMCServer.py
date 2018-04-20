import hug
import asyncio

from marshmallow import fields, pprint
import datetime as dt
# from marshmallow.validate import Range
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery, SpatioTemporalQuerySchema
from lfmc.query.Query import Query
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult, ModelResultSchema
from lfmc.models.ModelRegister import ModelRegister, ModelsRegisterSchema

import numpy as np
import pandas as pd

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
    """ While under development currently just JSON Encodes the query. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    
    schema = ModelResultSchema(many=True)
    model_subset = ['dead_fuel']
    if models is not None:
        model_subset = models
    
    mr = ModelRegister()
    print("Answering now...")
    successes = await asyncio.gather( *[ mr.get(model).answer(query) for model in model_subset] )
    results = successes.result()

    # Success
    response, errors = schema.dump(results)
    # pprint(response)

    # Default Response
    query.logResponse()
    return response


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
    return {"processes": asyncio.Task.all_tasks()}


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
    monitor_requests.interface.cli()
