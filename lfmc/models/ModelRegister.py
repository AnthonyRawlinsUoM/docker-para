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

        self.models = [dead_fuel,
                       ffdi,
                       temp,
                       gfdi,
                       awra,
                       jasmin,
                       kbdi,
                       drought]

        self.model_names = self.get_models()
        pass

    def register_new_model(self, new_model: Model):
        self.models.append(new_model)

    def get_models(self):
        return [m.name for m in self.models]

    def get(self, model_name):
        for m in self.models:
            if m.name == model_name:
                return m
        return None

    def subscribe(self, observer):
        if dev.DEBUG:
            logger.debug("Got subscription. Building response.")

            for model in self.models:
                dps = []
                logger.debug('Building dummy response for model: %s' % model.name)
                for j in range(30):
                    dps.append(DummyResults.dummy_single(j))
                    observer.on_next(ModelResult(model_name=model.name, data_points=dps))
            observer.on_completed()
        else:
            dps = []
            for model in self.models:
                model.subscribe(observer)

            # rx.Observable.merge()
        pass


class ModelsRegisterSchema(Schema):
    models = fields.Nested(ModelSchema, many=True)
