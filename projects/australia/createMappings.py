from mappy import Mappy


class AutraliaMapper(Mappy):
    
    name = "australia"
    
    def manualMappings(self):
        """Infer Elasticsearch mapping from pandas DataFrame"""
        return {
                "mappings": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                        # Add other fields as needed
                    }
                }
            }
        
        
    def additionalColumn(self,df):
        df["location"] = df.apply(lambda row: {"lat": float(row["lat"]), "lon": float(row["lng"])}, axis=1)
