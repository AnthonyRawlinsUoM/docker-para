from marshmallow import fields, Schema

from lfmc.models.JASMIN import JasminModel
from lfmc.models.Model import Model, ModelSchema
from lfmc.models.DeadFuel import DeadFuelModel
# from lfmc.models.LiveFuel import LiveFuelModel
from lfmc.models.FFDI import FFDIModel
from lfmc.models.KBDI import KBDIModel
from lfmc.models.GFDI import GFDIModel
from lfmc.models.AWRA import AWRAModel
from lfmc.models.DF import DFModel
from lfmc.models.Matthews import Matthews


import pandas as pd

from lfmc.models.Temp import TempModel


class ModelRegister:
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
        matthews = Matthews()

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
             'model': drought},
            {'model_name': matthews.name,
             'model': matthews}
        ]
        self.model_names = self.get_models()

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


class ModelsRegisterSchema(Schema):
    models = fields.Nested(ModelSchema, only=('model_name'), many=True)
