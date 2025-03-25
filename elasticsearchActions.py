import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np
from dotenv import dotenv_values

config = dotenv_values(".env")
csv_file_path = config["csv_file_path"]

index_name = config["index_name"]

es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=(config["elasticsearch_username"], config["elasticsearch_password"]),  # If authentication is required
    verify_certs=False,  # Set to False to disable SSL certificate verification
)

response = es.indices.delete(index=index_name)
print(response)