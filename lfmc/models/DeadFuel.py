import os, os.path
import numpy as np
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData

class DeadFuelModel(Model):

    def __init__(self):
        
        # TODO - Proper metadata!
        self.name = "dead_fuel"
        
        authors = []
        authors.append(Author(name="Test1", email="test1@example.com", organisation="Test Organisation"))
        authors.append(Author(name="Test2", email="test2@example.com", organisation="Test Organisation"))
        authors.append(Author(name="Test3", email="test3@example.com", organisation="Test Organisation"))
        
        pub_date = dt.datetime(2017,1,1)
        
        print("Now: ", pub_date.strftime('%d/%m/%Y'))
        
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"])
        
        # Prefixes
        vapour_prefix = 'VP3pm'
        temp_prefix = 'Tmx'
        precipitation_prefix = 'P'
        dead_fuel_moisture_prefix = 'DFMC'

        self.path = os.path.abspath(
            '/media/arawlins/Backups/DataSources/geoserver_data/FuelModels/Dead_FM') + '/'

        vapour_url = "http://www.bom.gov.au/web03/ncc/www/awap/vprp/vprph15/daily/grid/0.05/history/nat/"
        max_avg_temp_url = "http://www.bom.gov.au/web03/ncc/www/awap/temperature/maxave/daily/grid/0.05/history/nat/"
        precipitation_url = "http://www.bom.gov.au/web03/ncc/www/awap/rainfall/totals/daily/grid/0.05/history/nat/"
        vapour_path = self.path + vapour_prefix + "/"
        max_avg_temp_path = self.path + temp_prefix + "/"
        precipitation_path = self.path + precipitation_prefix + "/"

        self.tolerance = 0.06  # As a percentage accuracy
        
        self.parameters = {
            "vapour pressure": {
                "var": "VP3pm",
                "path": vapour_path,
                "url": vapour_url,
                "prefix": vapour_prefix,
                "suffix": ".grid",
                "compression_suffix": ".Z"
            },
            "maximum average temperature": {
                "var": "T",
                "path": max_avg_temp_path,
                "url": max_avg_temp_url,
                "prefix": temp_prefix,
                "suffix": ".grid",
                "compression_suffix": ".Z"
            },
            "precipitation": {
                "var": "P",
                "path": precipitation_path,
                "url": precipitation_url,
                "prefix": precipitation_prefix,
                "suffix": ".grid",
                "compression_suffix": ".Z"
            }
        }

        self.outputs = {
            "fuel moisture": {
                "path": self.path + dead_fuel_moisture_prefix + "/",
                "url": "",
                "prefix": dead_fuel_moisture_prefix,
                "suffix": ".grid",
                "compression_suffix": ".Z"
            }
        }


    def calculate(vp, t, p):
        """Short summary.

        Parameters
        ----------
        vp : type
            Description of parameter `vp`.
        t : type
            Description of parameter `t`.
        p : type
            Description of parameter `p`.

        Returns
        -------
        type
            Description of returned object.

        """
        ea = vp * 0.1
        es = 0.6108 * np.exp(17.27 * t / (t + 237.3))
        d = np.clip(ea - es, None, 0)
        return 6.79 + (27.43 * np.exp(1.05 * d))

