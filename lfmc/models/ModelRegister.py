from marshmallow import fields, Schema

from lfmc.models.Model import Model, ModelSchema
from lfmc.models.DeadFuel import DeadFuelModel
from lfmc.models.LiveFuel import LiveFuelModel

import pandas as pd

class ModelRegister:
    def __init__(self):
        dead_fuel = DeadFuelModel()
        live_fuel = LiveFuelModel()
    
        self.models = [
                        {'model_name': dead_fuel.name,
                         'model': dead_fuel},
                        {'model_name': live_fuel.name,
                         'model': live_fuel}
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