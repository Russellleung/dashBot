import pandas as pd


def process_nested_aggs(agg_data, current_path=None, current_row=None):
    """
    Recursively process nested aggregations and create rows for each combination.
    
    Args:
        agg_data: The current aggregation data to process
        current_path: List tracking the current path in the aggregation hierarchy
        current_row: Dictionary representing the current row being built
        
    Returns:
        List of dictionaries, each representing a complete row
    """
    if current_path is None:
        current_path = []
    if current_row is None:
        current_row = {}
            
    result_rows = []
    
    # Get the current aggregation name
    if current_path:
        # agg_name = current_path[-1]
        agg_name = ".".join(current_path[1:])     
        just_name = current_path[-1] 
        current_row_name = agg_name if agg_name else just_name   
    else:
        # Fallback name if path is empty (shouldn't happen in practice)
        agg_name = ""
    
    
    # Handle bucket aggregations
    if "buckets" in agg_data:
        buckets = agg_data["buckets"]
        
        if isinstance(buckets, list):
            
            # Process each bucket
            for bucket in buckets:
                # Create a new row with the current bucket's data
                new_row = current_row.copy()
                new_row[current_row_name] = bucket.get("key_as_string", "") if "key_as_string" in bucket else bucket.get("key", "")
                new_row[f"{current_row_name}_count"] = bucket.get("doc_count", 0)
                
                # Collect all metric aggregations at this level
                metric_values = {}
                sub_agg_keys = []
                
                for sub_key, sub_value in bucket.items():
                    if sub_key not in ["key", "doc_count", "key_as_string"]:
                        if "value" in sub_value:
                            # This is a metric aggregation - add to current row
                            row_key = agg_name + "." + sub_key if agg_name else sub_key
                            metric_values[row_key] = sub_value["value"]
                        elif "values" in sub_value:
                            # Handle percentiles
                            name=f"{sub_key}_{percentile}"
                            row_key = agg_name + "." + name if agg_name else name
                            for percentile, value in sub_value["values"].items():
                                metric_values[row_key] = value
                        elif "buckets" in sub_value:
                            # This is a sub-bucket aggregation - process recursively later
                            sub_agg_keys.append(sub_key)
                
                # Add all metrics to the row
                new_row.update(metric_values)
                
                if sub_agg_keys:
                    newCurrentPath=[]
                    if len(current_path)>1 or len(sub_agg_keys)>1:
                        newCurrentPath = current_path
             
                    # Process sub-bucket aggregations
                    for sub_key in sub_agg_keys:
                        sub_value = bucket[sub_key]
                        sub_rows = process_nested_aggs(
                            sub_value,
                            newCurrentPath + [sub_key],
                            new_row.copy()  # Pass the row with metrics already added
                        )
                        result_rows.extend(sub_rows)
                else:
                    # No sub-buckets, just add this row
                    result_rows.append(new_row)
        
        elif isinstance(buckets, dict):
            # Handle composite or filters aggregation
            for key, bucket in buckets.items():
                new_row = current_row.copy()
                new_row[current_row_name] = key
                new_row[f"{current_row_name}_count"] = bucket.get("doc_count", 0)
                
                # Collect all metric aggregations at this level
                metric_values = {}
                sub_agg_keys = []
                
                for sub_key, sub_value in bucket.items():
                    if sub_key not in ["doc_count"]:
                        if "value" in sub_value:
                            # This is a metric aggregation
                            metric_values[agg_name + "." + sub_key] = sub_value["value"]
                        elif "values" in sub_value:
                            # Handle percentiles
                            for percentile, value in sub_value["values"].items():
                                metric_values[agg_name + "." + f"{sub_key}_{percentile}"] = value
                        else:
                            # Likely a sub-bucket
                            sub_agg_keys.append(sub_key)
                
                # Add all metrics to the row
                new_row.update(metric_values)
                
                if sub_agg_keys:
                    newCurrentPath=[]
                    if len(current_path)>1 or len(sub_agg_keys)>1:
                        newCurrentPath = current_path
                    # Process sub-bucket aggregations
                    for sub_key in sub_agg_keys:
                        sub_value = bucket[sub_key]
                        sub_rows = process_nested_aggs(
                            sub_value,
                            newCurrentPath + [sub_key],
                            new_row.copy()
                        )
                        result_rows.extend(sub_rows)
                else:
                    # No sub-buckets, just add this row
                    result_rows.append(new_row)
    
    # Handle the case where we're at a leaf node (metric aggregation)
    elif "value" in agg_data:
        # agg_name = ".".join(current_path) if current_path else "value"
        new_row = current_row.copy()
        new_row[current_row_name] = agg_data["value"]
        result_rows.append(new_row)
    
    # Handle percentiles
    elif "values" in agg_data:
        # agg_name = ".".join(current_path) if current_path else "percentile"
        new_row = current_row.copy()
        for percentile, value in agg_data["values"].items():
            new_row[f"{current_row_name}_{percentile}"] = value
        result_rows.append(new_row)
    
    # If we didn't match any specific aggregation type but have a row, return it
    elif current_row:
        result_rows.append(current_row)
        
    return result_rows


def createTableInStreamlit(st, results):
    """Display Elasticsearch aggregation results in Streamlit."""
    
    if "aggregations" in results:
        for agg_name, agg_data in results["aggregations"].items():
            # st.subheader(f"Aggregation: {agg_name}")
            
            # Process this aggregation and all its nested aggregations
            rows = process_nested_aggs(agg_data, [agg_name])
            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df)
            else:
                st.write("No data in aggregation")

    # Check if we have hits
    if "hits" in results and results["hits"]["hits"]:    
        # TODO decide isinstance(results["hits"]["hits"], list)    
        # st.write("Search Results")
        hits = results["hits"]["hits"]
        extracted_data = [hit["_source"] for hit in hits]

        # Convert to a pandas DataFrame
        df = pd.DataFrame(extracted_data)

        # Display the DataFrame as a table
        # st.write(f"Found {results['hits']['total']['value']} documents:")
        st.dataframe(df)


# def visualize_aggregation(st, agg_name, agg_data):
#     """Create visualizations for aggregation results"""

#     if "buckets" in agg_data and isinstance(agg_data["buckets"], list):
#         buckets = agg_data["buckets"]

#         # Check if we have enough data to visualize
#         if len(buckets) > 1:
#             # Create a simple bar chart
#             bucket_df = pd.DataFrame(
#                 [{"key": b["key"], "count": b["doc_count"]} for b in buckets]
#             )

#             st.write(f"### {agg_name} Distribution")

#             # Bar chart
#             st.bar_chart(bucket_df.set_index("key")["count"])

#             # Pie chart option
#             if len(buckets) <= 10:  # Pie charts work best with fewer categories
#                 fig, ax = plt.pyplot.subplots()
#                 ax.pie(bucket_df["count"], labels=bucket_df["key"], autopct="%1.1f%%")
#                 ax.axis(
#                     "equal"
#                 )  # Equal aspect ratio ensures that pie is drawn as a circle
#                 st.pyplot(fig)
