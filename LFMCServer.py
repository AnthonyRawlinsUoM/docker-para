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
def fuel(lat1: fields.Decimal(as_string=True),
         lon1: fields.Decimal(as_string=True),
         lat2: fields.Decimal(as_string=True),
         lon2: fields.Decimal(as_string=True),
         start: fields.String(),
         finish: fields.String(),
         models: hug.types.delimited_list(',')):
    """ While under development currently just JSON Encodes the query. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    query.logResponse()
    schema = ModelResultSchema(many=True)
    model_subset = ['live_fuel', 'dead_fuel']
    if models is not None:
        model_subset = models
        

    loop = asyncio.get_event_loop()
    future = asyncio.Future()
    asyncio.ensure_future(lodge(query, future, model_subset))
    loop.run_until_complete(future)
    results = future.result()
    # pprint(results)

    # Success
    response, errors = schema.dump(results)
    # pprint(response)

    # Default Response
    return response


# Just a testing method
@asyncio.coroutine
async def lodge(st_query: Query, future: asyncio.Future, filters=None):
    rc = []
    # for model in ['noland_dead', 'nolan_live']:
    model_register = ModelRegister()
    reg_models = model_register.get_models()
    selected_models = reg_models

    # reg_models matching Filters in Query
    if filters is not None:
        selected_models = filters

    print(selected_models)
    print(reg_models)

    for item in selected_models:
        if item in reg_models:

            DEBUG = True
            if not DEBUG:
                model = model_register.get(item)
                a = 0
                # task_analyze = Task(model, query)
                # task_process = Task(model, query)
                # task_respond = Task(model, query)
                # 
                # ProcessingQueue.enqueue(task_analyze, callback)
                # ProcessingQueue.enqueue(task_process, callback)
                # ProcessingQueue.enqueue(task_respond, callback)
            else:
                response = []
                diff = st_query.temporal.finish - st_query.temporal.start
                for i in range(diff.days):
                    
                    # Dummy data for testing...
                    # value, mean, min, max, std
                    five_values = [np.random.random_sample() for i in range(5)]
                    
                    five_values = pd.DataFrame(five_values)
                    # print(five_values)
                    response.append(DataPoint(st_query.temporal.start + dt.timedelta(days=i),
                                              five_values[0].mean(),
                                              five_values[0].mean(),
                                              five_values[0].min(),
                                              five_values[0].max(),
                                              five_values[0].std()
                                              ))
                    # print(response)
            rc.append(ModelResult(item, response))
    # if DEBUG:
    #     await asyncio.sleep(10)
    future.set_result(rc)


@hug.cli()
@api.urls('/monitors/requests', versions=1)
def monitor_requests():
    return {"requests": []}


@hug.cli()
@api.urls('/monitors/processes', versions=1)
def monitor_processes():
    return {"processes": []}


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
    monitor_processes.interface.cli()
    monitor_requests.interface.cli()
