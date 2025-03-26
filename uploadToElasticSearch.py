import pandas as pd
from elasticsearch import Elasticsearch, helpers
from mappingFactory import MappingFactory
import numpy as np
from dotenv import dotenv_values

from dateutil import parser
strtime = '2009-03-08T00:27:31.807Z'
epoch = parser.parse(strtime).timestamp()


config = dotenv_values(".env")
csv_file_path = config["csv_file_path"]

index_name = config["index_name"]


mappingFactory = MappingFactory()
mappy = mappingFactory.getMappy(index_name)

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=(config["elasticsearch_username"], config["elasticsearch_password"]),  # If authentication is required
    verify_certs=False,  # Set to False to disable SSL certificate verification
)


def upload_csv_to_elasticsearch(csv_file, index_name):
    # Read CSV
    df = pd.read_csv(csv_file)
    print(df)
    
    # Try to detect and convert date columns
    for col in df.columns:
        # Handle numeric columns that might be epoch timestamps
        if pd.api.types.is_numeric_dtype(df[col]):
            # Check if values are in epoch millisecond range
            if df[col].min() > 1000000000000 and df[col].max() < 9999999999999:
                sample = df[col].dropna().sample(min(100, len(df[col].dropna())))
                converted = pd.to_datetime(sample, unit='ms', errors='coerce')
                if converted.notna().mean() >= 0.9:  # 90% success rate
                    df[col] = pd.to_datetime(df[col])
                    print(f"Converted column '{col}' from epoch milliseconds to datetime")
    
    # # Generate mapping from DataFrame
    # mapping = infer_mapping_from_dataframe(df)
    # print(mapping)
    
    # Create index with mapping
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name,body=mappy.manualMappings())
    
    # Convert DataFrame to list of dicts
    # Replace NaN with None to avoid issues
    df = df.replace({np.nan: None})
    mappy.additionalColumn(df)
    print(df)
    documents = df.to_dict('records')
    # print(documents)
    # Prepare for bulk indexing
    actions = [
        {
            "_index": index_name,
            "_source": doc
        }
        for doc in documents
    ]
    
    # Bulk index the data
    try:
        response = helpers.bulk(es, actions)
    except Exception as e:
        print(f"BulkIndexError: {len(e.errors)} document(s) failed to index.")
        for error in e.errors:
            print(f"Error details: {error}")
    
    print(f"Uploaded {len(documents)} documents to index {index_name}")




# Call the function
upload_csv_to_elasticsearch(csv_file_path, index_name)