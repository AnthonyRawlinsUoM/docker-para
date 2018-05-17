import datetime as dt

import rx
from marshmallow import fields, Schema
from rx import Observable
from lfmc.config import debug as dev
from lfmc.models.JASMIN import JasminModel
from lfmc.models.Model import Model, ModelSchema
from lfmc.models.DeadFuel import DeadFuelModel
# from lfmc.models.LiveFuel import LiveFuelModel
from lfmc.models.FFDI import FFDIModel
from lfmc.models.KBDI import KBDIModel
from lfmc.models.GFDI import GFDIModel
from lfmc.models.AWRA import AWRAModel
from lfmc.models.DF import DFModel
# from lfmc.models.Matthews import Matthews
from lfmc.models.dummy_results import DummyResults

from lfmc.models.rx.ObservableModelRegister import ObservableModelRegister
from lfmc.models.Temp import TempModel
from lfmc.query import ShapeQuery
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class ModelRegister(Observable):

    def __init__(self):
        dead_fuel = DeadFuelModel()
        # live_fuel = LiveFuelModel()
        temp = TempModel()
        ffdi = FFDIModel()
        gfdi = GFDIModel()
        kbdi = KBDIModel()
        awra = AWRAModel()
        jasmin = JasminModel()
        drought = DFModel()
        # matthews = Matthews()

        self.models = [
            {'model_name': dead_fuel.name,
             'model': dead_fuel},
            # {'model_name': live_fuel.name,
            #  'model': live_fuel},
            {'model_name': ffdi.name,
             'model': ffdi},
            {'model_name': temp.name,
             'model': temp},
            {'model_name': gfdi.name,
             'model': gfdi},
            {'model_name': awra.name,
             'model': awra},
            {'model_name': jasmin.name,
             'model': jasmin},
            {'model_name': kbdi.name,
             'model': kbdi},
            {'model_name': drought.name,
             'model': drought}
            # ,
            # {'model_name': matthews.name,
            #  'model': matthews}
        ]
        self.model_names = self.get_models()
        pass

    def register_new_model(self, new_model: Model):
        self.models.append({'model_name': new_model.name,
                            'model': new_model})

    def get_models(self):
        names = []
        for m in self.models:
            names.append(m['model_name'])
        return names

    def get(self, model_name):
        for m in self.models:
            if m['model_name'] == model_name:
                return m['model']
        return None

    def apply_shape_for_timeseries(self, query: ShapeQuery) -> ModelResult:

        return None

    def subscribe(self, observer):
        if dev.DEBUG:
            logger.debug("Got subscription. Building response.")

            for model in self.models:
                dps = []
                logger.debug('Building dummy response for model: %s' % model['model_name'])
                for j in range(30):
                    dps.append(DummyResults.dummy_single(j))
                    observer.on_next(ModelResult(model_name=model['model_name'], data_points=dps))
            observer.on_completed()
        else:
            dps = []
            for model in self.models:
                model.subscribe(observer)

            # rx.Observable.merge()
        pass


class ModelsRegisterSchema(Schema):
    models = fields.Nested(ModelSchema, only=('model_name'), many=True)
