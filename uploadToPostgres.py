import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from dotenv import dotenv_values


# Load environment variables
config = dotenv_values(".env")
path = config["path"]
db_name = config["database_name_postgres"]
db_user = config["database_user"]
db_password = config["database_password"]
db_host = config["database_host"]
db_port = config["database_port"]

csv_file_paths = ["customers.csv", "orders.csv", "products.csv"]

# Create a connection to PostgreSQL using SQLAlchemy
try:
    # Connect to the default 'postgres' database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        dbname="postgres"
    )
    conn.autocommit = True  # Enable autocommit for database creation

    # Create a cursor object
    cursor = conn.cursor()

    # Prepare and execute the CREATE DATABASE query
    create_db_query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name))
    cursor.execute(create_db_query)
    print(f"Database '{db_name}' created successfully.")

    # Verify that the database was created
    cursor.execute("SELECT datname FROM pg_database;")
    databases = [row[0] for row in cursor.fetchall()]
    if db_name in databases:
        print(f"Database '{db_name}' exists in the PostgreSQL instance.")
    else:
        print(f"Database '{db_name}' does not exist.")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection
    if conn:
        cursor.close()
        conn.close()
        print("Connection closed.")



def dfMutation(df):
    if "Order_Date" in df:
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%m/%d/%Y', errors='coerce').dt.date
                    

try:
    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )
    print("Connection to PostgreSQL established successfully.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit()
    
try:
    with engine.connect() as connection:
        with connection.begin():
            # Process and save CSV data into PostgreSQL
            for csv_file_path in csv_file_paths:
                name = csv_file_path.split(".")[0]
                csv_file_path = path + "/" + csv_file_path
                try:
                    # Read CSV file into a DataFrame
                    df = pd.read_csv(csv_file_path)
                    df.columns = df.columns.str.replace(" ", "_")  # Replace spaces in column names
                    
                    # Format the 'Order_Date' column if it exists
                    dfMutation(df)
                    
                    # Save DataFrame to PostgreSQL table
                    df.to_sql(name, connection, if_exists='replace', index=False)  # Options: 'replace', 'append', 'fail'
                    print(f"Data saved to table '{name}' successfully.")

                except Exception as e:
                    print(f"Error processing file '{csv_file_path}': {e}")           
except Exception as e:
    print(f"Database error: {e}")

# Close the connection (optional since SQLAlchemy handles it automatically)
engine.dispose()
print("Connection closed.")