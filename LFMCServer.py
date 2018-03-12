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

api = hug.get(on_invalid=hug.redirect.not_found)


@hug.cli()
@api.urls('/fuel', examples='lat1=-10&lon1=110&lat2=-45&lon2=145&start=20180101&finish=20180201', versions=0)
def fuel(lat1: hug.types.number, lon1: hug.types.number, lat2: hug.types.number, lon2: hug.types.number,
         start: fields.String(), finish: fields.String()):
    """ While under development currently just JSON Encodes the query. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    schema = SpatioTemporalQuerySchema()
    response, errors = schema.dump(query)
    return response


@hug.cli()
@api.urls('/fuel', examples='lat1=-10&lon1=110&lat2=-45&lon2=145&start=20180101&finish=20180201', versions=1,
          output=hug.output_format.pretty_json)
def fuel(lat1: hug.types.number,
         lon1: hug.types.number,
         lat2: hug.types.number,
         lon2: hug.types.number,
         start: fields.String(),
         finish: fields.String(),
         subset: fields.String()):
    """ While under development currently just JSON Encodes the query. """
    query = SpatioTemporalQuery(lat1, lon1, lat2, lon2, start, finish)
    query.logResponse()
    schema = ModelResultSchema(many=True)

    if subset is not None:
        model_subset = subset.split(',')
    else:
        model_subset = ['live_fuel', 'dead_fuel']

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
                    response.append(DataPoint(st_query.temporal.start + dt.timedelta(days=i),
                                              np.random.random_sample(),
                                              np.random.random_sample(),
                                              np.random.random_sample(),
                                              np.random.random_sample(),
                                              np.random.random_sample()
                                              ))
            rc.append(ModelResult(item, response))
    # if DEBUG:
    #     await asyncio.sleep(10)
    future.set_result(rc)


@hug.cli()
@api.urls('/monitors/requests', versions=0)
def monitor_requests():
    return {"requests": []}


@hug.cli()
@api.urls('/monitors/processes', versions=0)
def monitor_processes():
    return {"processes": []}


@hug.cli()
@api.urls('/models', versions=0)
def get_models():
    return {"models": []}


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
