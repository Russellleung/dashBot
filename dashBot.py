import streamlit as st
import json
import os
from elasticsearch import Elasticsearch
from datetime import datetime
from dotenv import dotenv_values
import requests
import pandas as pd
from createTable import createTableInStreamlit


config = dotenv_values(".env")

dir = config["path"]
# set up deepseek
API_KEY = config["API_KEY"]
API_URL = config["API_URL"]
headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
widgetJsonPath = dir + "/" + config["index_name"] + "_saved_widgets.json"
es = Elasticsearch(
    hosts=["https://localhost:9200"],
    basic_auth=(
        config["elasticsearch_username"],
        config["elasticsearch_password"],
    ),  # If authentication is required
    verify_certs=False,  # Set to False to disable SSL certificate verification
)

mapping = es.indices.get_mapping(index=config["index_name"])


# Sidebar with table that follows as you scroll
with st.sidebar:
    st.header("Mapping")
    
    # Sample data
    properties = mapping[config["index_name"]]["mappings"]["properties"]
    
    field_data = []
    for field_name, field_info in properties.items():
        field_type = field_info.get("type", "unknown")
        field_data.append({
            "Field Name": field_name,
            "Data Type": field_type,
        })
    
    # Convert to DataFrame and display as a styled table
    df = pd.DataFrame(field_data)
    
    # Display the table
    st.dataframe(df)

# Initialize session state for saved widgets
if "saved_widgets" not in st.session_state:
    if os.path.exists(widgetJsonPath):
        with open(widgetJsonPath, "r") as f:
            st.session_state.saved_widgets = json.load(f)
    else:
        st.session_state.saved_widgets = []


def get_chat_response(mapping, request):

    system_prompt = f"""
    You are a senior data analyst. Your client wants to understand more about their data. This is their request.
    Request:
    {request}
    
    From the request, generate 3 useful Elasticsearch queries. Return only JSON format with query_name and Elasticsearch query_body. Each of them should be on a new line. 
    
    Below is the index mapping you can use to query on:
    {mapping}
    """

    messages_with_context = [
        {
            "role": "system",
            "content": "You are a data analyst specializing in Elasticsearch.",
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
            print(f"Query body: {json.dumps(query['query_body'], indent=2)}")
            ans.append((query["query_name"], query["query_body"]))
        return ans
    else:
        print("Failed to fetch data from API. Status Code:", response.status_code)
        return []

    # return queries


def execute_query(query_body):
    """Execute an Elasticsearch query and return results"""

    try:
        results = es.search(index=config["index_name"], body=query_body)
        print(query_body)
        print(results)
        print(results["hits"]["hits"])
        return results
    except Exception as e:
        st.error(f"Elasticsearch query error: {e}")
        return []


def save_widget(query_name, query_body):
    """Save a widget to the JSON file"""
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


def load_saved_widgets():
    """Load saved widgets from the JSON file"""
    if os.path.exists(widgetJsonPath):
        with open(widgetJsonPath, "r") as f:
            return json.load(f)
    return []


# def createTableInStreamlit(st, results):
#     # Extract relevant data from hits
#     hits = results["hits"]["hits"]
#     extracted_data = [hit["_source"] for hit in hits]

#     # Convert to a pandas DataFrame
#     df = pd.DataFrame(extracted_data)

#     # Streamlit app
#     # st.title("Elasticsearch Hits Table")

#     # Display the DataFrame as a table
#     st.write("Below is the table created from Elasticsearch hits:")
#     st.dataframe(df)

#     # # You can also use st.table for a static table
#     # st.subheader("Static Table View")
#     # st.table(df)


def displayDashboard(st):
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
                    st.code(json.dumps(widget["query"], indent=2), language="json")
                                    
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
                results = execute_query(widget["query"])
                createTableInStreamlit(st, results)
                
                st.markdown("---")
                
                
                
def displayWidgetsInDropdown(st):
# Display saved widgets at the top
    if st.session_state.saved_widgets:
        st.header("Saved Widgets")
        for i, widget in enumerate(st.session_state.saved_widgets):
            with st.expander(f"{widget['name']} (Saved on {widget['saved_at']})"):
                st.json(widget["query"])
                results = execute_query(widget["query"])
                createTableInStreamlit(st, results)

                # Option to remove from saved widgets
                if st.button(f"Remove Widget", key=f"remove_{i}"):
                    st.session_state.saved_widgets.pop(i)
                    with open(widgetJsonPath, "w") as f:
                        json.dump(st.session_state.saved_widgets, f)
                    st.rerun()                
                
                
                
                
def main():
    st.title(config["index_name"] + " Analytics Dashboard")
    st.subheader("Elasticsearch-powered insights with smartRussell assistance")
    
    displayDashboard(st)

    # Generate top 3 queries section
    st.header("Top 3 Suggested Queries")
    if st.button("Generate Top Queries"):
        with st.spinner("Generating queries with smartRussell..."):
            top_queries = get_chat_response(
                mapping, "Suggest the top queries that will be useful for the data"
            )
            print(top_queries)
            st.session_state.top_queries = top_queries

    # Display generated queries
    if "top_queries" in st.session_state:
        for i, (query_name, query_body) in enumerate(st.session_state.top_queries):
            with st.expander(f"{query_name}"):
                # Show the query with option to edit
                edited_query = st.text_area(
                    f"Edit Query",
                    value=json.dumps(query_body, indent=2),
                    height=200,
                    key=f"edit_top_{i}",
                )
                valid = True
                try:
                    # Parse the edited query
                    parsed_query = json.loads(edited_query)
                    valid = True
                except json.JSONDecodeError:
                    st.error("Invalid JSON query. Please fix the syntax.")
                    valid = False

                # Execute query button
                if st.button(f"Run Query", key=f"top_run_{i}") and valid:
                    with st.spinner("Executing query..."):
                        try:
                            results = execute_query(parsed_query)
                            createTableInStreamlit(st, results)
                        except:
                            valid = False

                # Save widget button
                if st.button(f"Save Widget", key=f"top_save_{i}"):
                    if not valid:
                        st.error("Cannot save. Error in query")
                    else:
                        save_widget(query_name, parsed_query)

    # Test your own query
    st.header("Test your own query")
    query_body_text = st.text_area(
        "Edit your Elasticsearch query below:",
        value="""{
            "query": {
                "match_all": {}
            }
        }""",
        height=200,
    )

    try:
        parsed_query = json.loads(query_body_text)
        is_valid_json = True
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON: {str(e)}")
        is_valid_json = False
    # Execute query button
    if st.button("Execute Query") and is_valid_json:
        with st.spinner("Executing query..."):
            try:
                results = execute_query(parsed_query)
                createTableInStreamlit(st, results)
            except Exception as e:
                st.error(f"Error executing query: {str(e)}")
                is_valid_json = False

    # Save query section
    st.header("Save Query")
    query_name = st.text_input("Enter a name for this query:")
    if st.button(f"Save Widget"):
        if not is_valid_json:
            st.error("Invalid query")
        elif not query_name:
            st.error("No name")
        else:
            save_widget(query_name, parsed_query)

    # Chatbot section
    st.header("Query Assistant Chatbot")
    user_input = st.text_area(
        "Describe what you're looking for:",
        height=100,
        placeholder="E.g., Show me electric cars under $50,000 with low mileage",
    )

    if st.button("Generate Custom Queries"):
        if user_input:
            with st.spinner("Generating queries based on your request..."):
                custom_queries = get_chat_response(mapping, user_input)
                st.session_state.custom_queries = custom_queries
        else:
            st.warning("Please enter a description of what you're looking for.")

    # Display custom generated queries
    if "custom_queries" in st.session_state:
        st.subheader("Generated Queries")
        for i, (query_name, query_body) in enumerate(st.session_state.custom_queries):
            with st.expander(f"{query_name}"):
                # Show the query with option to edit
                edited_query = st.text_area(
                    f"Edit Query",
                    value=json.dumps(query_body, indent=2),
                    height=200,
                    key=f"edit_custom_{i}",
                )

                try:
                    # Parse the edited query
                    parsed_query = json.loads(edited_query)

                    # Execute query button
                    if st.button(f"Run Query", key=f"custom_run_{i}"):
                        with st.spinner("Executing query..."):
                            results = execute_query(parsed_query)
                            createTableInStreamlit(st, results)

                    # Save widget button
                    if st.button(f"Save Widget", key=f"custom_save_{i}"):
                        save_widget(query_name, parsed_query)

                except json.JSONDecodeError:
                    st.error("Invalid JSON query. Please fix the syntax.")


if __name__ == "__main__":
    main()
# response = get_chat_response(mapping, "give me some queries to start with")
