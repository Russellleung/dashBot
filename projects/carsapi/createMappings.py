from mappy import Mappy
import pandas as pd
from datetime import datetime


class CarsApiMappy(Mappy):
    
    name = "carsapi"
    
    def manualMappings(self):
        """Infer Elasticsearch mapping from pandas DataFrame"""
        return {
                "mappings": {
                    "properties": {
                        "Trim Created": {
                            "type": "date",
                            "format": "M/d/yy, h:mm a||strict_date_optional_time||epoch_millis"
                        },
                        "Trim Modified": {
                            "type": "date",
                            "format": "M/d/yy, h:mm a||strict_date_optional_time||epoch_millis"
                        }
                    }
                }
            }
        
        
    def additionalColumn(self,df):
        
        def convert_date_time(date_time_str):
            try:
                # Parse the date-time in format "M/d/yy, h:mm a"
                date_time_obj = datetime.strptime(date_time_str, "%m/%d/%y, %I:%M %p")
                # Return in ISO format
                return date_time_obj.isoformat()
            except ValueError:
                return None
        df['Trim Created'] = df['Trim Created'].apply(convert_date_time)
        df['Trim Modified'] = df['Trim Modified'].apply(convert_date_time)