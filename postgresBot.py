import streamlit as st
import sqlite3
import pandas as pd
import os
import json
from datetime import datetime
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from sql_formatter.core import format_sql



from dotenv import dotenv_values

config = dotenv_values(".env")
path = config["path"]
API_KEY = config["API_KEY"]
API_URL = config["API_URL"]
db_name = config["database_name_postgres"]
db_user = config["database_user"]
db_password = config["database_password"]
db_host = config["database_host"]
db_port = config["database_port"]
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}


# Title of the app
st.title("SQLite Database Viewer and Query Executor")

# Input for SQLite database file
db_file = st.text_input("Enter the SQLite database file path:", db_name)

widgetJsonPath = path + "/" + config["index_name"] + "_postgres_saved_widgets.json"

# Initialize session state for saved widgets
if "saved_widgets" not in st.session_state:
    if os.path.exists(widgetJsonPath):
        with open(widgetJsonPath, "r") as f:
            st.session_state.saved_widgets = json.load(f)
    else:
        st.session_state.saved_widgets = []
        
        
def save_widget(query_name, query_body):
    """Save a widget to the JSON file"""
    query_body = format_sql(query_body)

    widget = {
        "name": query_name,
        "query": query_body,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # Add to session state
    st.session_state.saved_widgets.append(widget)

    # Save to file
    with open(widgetJsonPath, "w") as f:
        json.dump(st.session_state.saved_widgets, f)

    st.success(f"Widget '{query_name}' saved successfully!")
        
        
def displayDashboard(st,conn):
    if st.session_state.saved_widgets:
        st.header("Saved Widgets")
        
        # Define number of columns in your grid
        num_cols = 2  # Adjust based on your preference
        grid_cols = st.columns(num_cols)

        
        for i, widget in enumerate(st.session_state.saved_widgets):
            # Determine which column to place this widget in
            col_idx = i % num_cols
            
            with grid_cols[col_idx]:
            # Create a row for the title and buttons
                title_col, view_btn_col, remove_btn_col = st.columns([4, 1, 1])
                
                # Widget name and save date on the left
                with title_col:
                    st.caption(f"{widget['name']}")
                    
                    
                @st.dialog("Query")
                def show_my_dialog():
                    st.code(widget["query"])
                                    
                # View Query button with icon
                with view_btn_col:
                    if st.button("🔍", key=f"view_{i}", help="View Query"):
                        st.session_state[f"show_query_{i}"] = True
                        show_my_dialog()
                
                # Remove button with icon
                with remove_btn_col:
                    if st.button("🗑️", key=f"remove_{i}", help="Remove Widget"):
                        st.session_state.saved_widgets.pop(i)
                        with open(widgetJsonPath, "w") as f:
                            json.dump(st.session_state.saved_widgets, f)
                        st.rerun()
                
                # Execute query and display results directly below the title row
                result_df = pd.read_sql_query(widget["query"], conn)
                st.dataframe(result_df, use_container_width=True)
                
                st.markdown("---")
                
                
                
def get_chat_response(schemas, request):

    system_prompt = f"""
    You are a senior data analyst. Your client wants to understand more about their data. This is their request.
    Request:
    {request}
    
    From the request, generate 3 useful SQL queries. Return only JSON format with query_name and query_body. Each of them should be on a new line. 
    Column names should be between quotation marks

    
    Below is the Table schema you can use to query on:
    {schemas}
    """
    # print(system_prompt)
    messages_with_context = [
        {
            "role": "system",
            "content": "You are a data analyst specializing in SQL.",
        },
        {"role": "system", "content": system_prompt},
    ]

    # Define the request payload (data)
    data = {
        "model": "deepseek/deepseek-chat:free",
        "messages": messages_with_context,
    }

    # Send the POST request to the DeepSeek API
    response = requests.post(API_URL, json=data, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        print("API Response:", response.json()["choices"][0]["message"]["content"])
        response = response.json()["choices"][0]["message"]["content"]

        json_objects = response.strip().split("\n")
        
        # Parse each line as a JSON object
        queries = []

        for obj in json_objects:
            try:
                response = json.loads(obj)
                queries.append(response)
            except:
                pass
        ans = []
        # Now you can work with the parsed queries
        for query in queries:
            if "query_name" not in query or "query_body" not in query:
                continue
            print(f"Query name: {query['query_name']}")
            print(f"Query body: {query['query_body']}")
            ans.append((query["query_name"], query["query_body"]))
        return ans
    else:
        print("Failed to fetch data from API. Status Code:", response.status_code)
        return []

def getTablesAndSchemas(conn):
    # Query to get all table names in the current database
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """
    tables = pd.read_sql_query(query, conn)
    # Dictionary to store table schemas
    dic = dict()
    
    for selected_table in tables["table_name"]:
        # Query to get column names and data types for each table
        schema_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{selected_table}';
        """
        schema_df = pd.read_sql_query(schema_query, conn)
        # Simplified schema with only column name and data type
        dic[selected_table] = schema_df[['column_name', 'data_type']]
    
    return dic
            
def simpleSideBar(st,conn):
    with st.sidebar:
        st.header("Database Schema Explorer")
        
        dic = getTablesAndSchemas(conn)
        
        for key, table in dic.items():
            st.write(f"Schema for table `{key}`:")
            st.table(table)
        
        
# try:
#     engine = create_engine(
#         f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
#     )
#     print("Connection to PostgreSQL established successfully.")
# except Exception as e:
#     print(f"Error connecting to PostgreSQL: {e}")
#     exit()  
        
# Connect to the SQLite database
try:
    engine = create_engine(
        f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_file}"
    )
    print("Connection to PostgreSQL established successfully.")
    st.success(f"Connected to database: {db_file}")
    with engine.connect() as conn:
        with conn.begin():
            displayDashboard(st,conn)
            # Sidebar for schema-related functionality
            simpleSideBar(st,conn)
            # Main content: SQL Query Executor
            st.subheader("SQL Query Executor")
            
            # Text area for user to input SQL query
            user_query = st.text_area("Enter your SQL query here:")
            is_valid_query = True
            if st.button("Execute Query"):
                try:
                    # Execute the query and fetch results
                    result_df = pd.read_sql_query(user_query, conn)
                    st.success("Query executed successfully!")
                    
                    # Display results in a neat format
                    st.write("Query Results:")
                    st.dataframe(result_df, use_container_width=True)
                    is_valid_query = True
                except Exception as e:
                    st.error(f"Error executing query: {e}")
                    is_valid_query = False
                    
            st.header("Save Query")
            query_name = st.text_input("Enter a name for this query:")
            if st.button(f"Save Widget"):
                if not is_valid_query:
                    st.error("Invalid query")
                elif not query_name:
                    st.error("No name")
                else:
                    save_widget(query_name, user_query)
                    
                    
                    
                    
            st.header("Query Assistant Chatbot")
            user_input = st.text_area(
                "Describe what you're looking for:",
                height=100,
                placeholder="E.g., Show me electric cars under $50,000 with low mileage",
            )

            if st.button("Generate Custom Queries"):
                if user_input:
                    with st.spinner("Generating queries based on your request..."):
                        custom_queries = get_chat_response(getTablesAndSchemas(conn), user_input)
                        st.session_state.custom_queries = custom_queries
                else:
                    st.warning("Please enter a description of what you're looking for.")

            # Display custom generated queries
            if "custom_queries" in st.session_state:
                st.subheader("Generated Queries")
                for i, (query_name, query_body) in enumerate(st.session_state.custom_queries):
                    with st.expander(f"{query_name}"):
                        # Show the query with option to edit
                        query_body = format_sql(query_body)
                        edited_query = st.text_area(
                            f"Edit Query",
                            value=query_body,
                            height=200,
                            key=f"edit_custom_{i}",
                        )

                        try:
                            # Parse the edited query

                            # Execute query button
                            if st.button(f"Run Query", key=f"custom_run_{i}"):
                                with st.spinner("Executing query..."):
                                    result_df = pd.read_sql_query(edited_query, conn)
                                    st.dataframe(result_df, use_container_width=True)

                            # Save widget button
                            if st.button(f"Save Widget", key=f"custom_save_{i}"):
                                save_widget(query_name, edited_query)

                        except json.JSONDecodeError:
                            st.error("Invalid JSON query. Please fix the syntax.")

except Exception as e:
    st.error(f"Error connecting to database: {e}")
    if hasattr(e, "info"):  # Check if the exception has an 'info' attribute
            st.error(f"error details: {json.dumps(e.info, indent=2)}") #print formatted error info

finally:
    if 'conn' in locals():
        conn.close()
        
engine.dispose()