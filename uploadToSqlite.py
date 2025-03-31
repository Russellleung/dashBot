import pandas as pd
import sqlite3
from dotenv import dotenv_values

config = dotenv_values(".env")
# csv_file_paths = config["csv_file_paths"]
path = config["path"]
db_file = config["database_name_sqlite"]

csv_file_paths = ["customers.csv","orders.csv","products.csv"]


for csv_file_path in csv_file_paths:
    name = csv_file_path.split(".")[0]
    csv_file_path = path + "/" + csv_file_path
    try:
        df = pd.read_csv(csv_file_path)
        df.columns = df.columns.str.replace(" ", "_")
        if "Order_Date" in df:
            df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%m/%d/%Y', errors='coerce').dt.date
        
        conn = sqlite3.connect(db_file)
        df.to_sql(name, conn, if_exists='replace', index=False) #if_exists can be 'replace', 'append', 'fail'
        print("Data saved to database successfully.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'conn' in locals():
            conn.close()