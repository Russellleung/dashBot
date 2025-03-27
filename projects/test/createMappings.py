from mappy import Mappy
import pandas as pd

class TestMappy(Mappy):
    
    name = "test"
    
    def manualMappings(self):
        """Infer Elasticsearch mapping from pandas DataFrame"""
        return {}
        
        
    def additionalColumn(self,df):
        col = "epoch_timestamp"
        df[col] = pd.to_datetime(df[col])