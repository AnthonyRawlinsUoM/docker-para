from lfmc.query.SpatioTemporalQuery import SpatioTemporalQuery
from lfmc.results.DataPoint import DataPoint
import numpy as np
import pandas as pd
import datetime as dt

class DummyResults:
    def dummy_data(self, query: SpatioTemporalQuery):
        response = []
        diff = query.temporal.finish - query.temporal.start
        for i in range(diff.days):
            
            # Dummy data for testing...
            # value, mean, min, max, std
            five_values = [np.random.random_sample() for i in range(5)]
            
            five_values = pd.DataFrame(five_values)
            # print(five_values)
            response.append(DataPoint(query.temporal.start + dt.timedelta(days=i),
                                      five_values[0].mean(),
                                      five_values[0].mean(),
                                      five_values[0].min(),
                                      five_values[0].max(),
                                      five_values[0].std()
                                      ))
            
        return response