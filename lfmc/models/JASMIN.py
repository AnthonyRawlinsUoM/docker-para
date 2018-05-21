import asyncio
import os
import os.path

from lfmc.models.BomBasedModel import BomBasedModel
from lfmc.results.Abstracts import Abstracts
from lfmc.results.Author import Author
import datetime as dt
from lfmc.models.Model import Model
from lfmc.models.ModelMetaData import ModelMetaData
import logging

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class JasminModel(BomBasedModel):

    def __init__(self):
        self.name = "jasmin"

        # TODO - Proper metadata!
        authors = [
            Author(name="Imtiaz Dharssi", email="",
                   organisation="Bureau of Meteorology, Australia"),
            Author(name="Vinodkumar", email="",
                   organisation="Bureau of Meteorology, Australia")
        ]
        pub_date = dt.datetime(2017, 10, 1)
        abstract = Abstracts("Accurate soil dryness information is essential for the calculation of accurate fire danger \
                ratings, fire behavior prediction, flood forecasting and landslip warnings. Soil dryness \
                also strongly influences temperatures and heatwave development by controlling the \
                partitioning of net surface radiation into sensible, latent and ground heat fluxes. Rainfall \
                forecasts are crucial for many applications and many studies suggest that soil dryness \
                can significantly influence rainfall. Currently, soil dryness for fire danger prediction in \
                Australia is estimated using very simple water balance models developed in the 1960s \
                that ignore many important factors such as incident solar radiation, soil types, vegeta- \
                tion height and root depth. This work presents a prototype high resolution soil moisture \
                analysis system based around the Joint UK Land Environment System (JULES) land \
                surface model. This prototype system is called the JULES based Australian Soil Mois- \
                ture INformation (JASMIN) system. The JASMIN system can include data from many \
                sources; such as surface observations of rainfall, temperature, dew-point temperature, \
                wind speed, surface pressure as well as satellite derived measurements of rainfall, sur- \
                face soil moisture, downward surface short-wave radiation, skin temperature, leaf area \
                index and tree heights. The JASMIN system estimates soil moisture on four soil layers \
                over the top 3 meters of soil, the surface layer has a thickness of 10 cm. The system \
                takes into account the effect of different vegetation types, root depth, stomatal resis- \
                tance and spatially varying soil texture. The analysis system has a one hour time-step \
                with daily updating. For the surface soil layer, verification against ground based soil \
                moisture observations from the OzNet, CosmOz and OzFlux networks shows that the \
                JASMIN system is significantly more accurate than other soil moisture analysis sys- \
                tem used at the Bureau of Meteorology. For the root-zone, the JASMIN system has \
                similar skill to other commonly used soil moisture analysis systems. The Extended \
                Triple Collocation (ETC) verification method also confirms the high skill of the JASMIN \
                system.")
        self.metadata = ModelMetaData(authors=authors, published_date=pub_date, fuel_types=["surface"],
                                      doi="http://dx.doi.org/10.1016/j.rse.2015.12.010", abstract=abstract)

        self.path = os.path.abspath(Model.path() + 'JASMIN') + '/'
        self.ident = "JASMIN"
        self.code = "JASMIN"
        self.outputs = {
            "type": "index",
            "readings": {
                "path": self.path,
                "url": "",
                "prefix": "smd",
                "suffix": ".nc"
            }
        }

    def netcdf_name_for_date(self, when):
        return os.path.abspath(
            Model.path() + "/JASMIN/rescaled/21vls/jasmin.kbdi/cdf temporal/jasmin.kbdi.cdf_temporal.2lvls.{}.nc".format(
                when.strftime("%Y")))
