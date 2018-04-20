import os, os.path
import numpy as np
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.results.DataPoint import DataPoint
from lfmc.results.ModelResult import ModelResult
from lfmc.models.ModelMetaData import ModelMetaData
from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.models.dummy_results import DummyResults


class KBDIModel(Model):

    def __init__(self):
        self.name = "kbdi"
        
        self.path = os.path.abspath(
            '/media/arawlins/Backups/DataSources/geoserver_data/FuelModels/KBDI') + '/'
        
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": self.name,
                "suffix": ".nc"
            }
        }
    
    def answer(self, query: SpatioTemporalQuery):
        # MAGIC HAPPENS HERE

        # An array of mockup DataPoints for testing only
        dr = DummyResults()
        dps = dr.dummy_data(query)
        return ModelResult(self.name, dps)