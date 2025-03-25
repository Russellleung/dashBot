import pandas as pd


def createTableInStreamlit(st, results):

    # Check if we have aggregations
    if "aggregations" in results:
        st.subheader("Aggregation Results")

        for agg_name, agg_data in results["aggregations"].items():
            st.write(f"**{agg_name}**")

            # Handle different aggregation types
            if "buckets" in agg_data:
                # Bucket aggregation (terms, date_histogram, etc.)
                buckets = agg_data["buckets"]

                # Convert buckets to DataFrame
                if isinstance(buckets, list):
                    # Standard buckets list
                    bucket_data = []
                    for bucket in buckets:
                        bucket_item = {
                            agg_name: bucket["key"],
                            "doc_count": bucket["doc_count"],
                        }

                        # Add any sub-aggregations
                        for sub_key, sub_value in bucket.items():
                            if sub_key not in ["key", "doc_count", "key_as_string"]:
                                if "value" in sub_value:
                                    bucket_item[f"{sub_key}"] = sub_value["value"]
                                elif "buckets" in sub_value:
                                    # Nested buckets - we'll simplify by showing count
                                    bucket_item[f"{sub_key}_count"] = len(
                                        sub_value["buckets"]
                                    )

                        bucket_data.append(bucket_item)

                    bucket_df = pd.DataFrame(bucket_data)
                    st.dataframe(bucket_df)

                elif isinstance(buckets, dict):
                    # Composite aggregation or filters
                    bucket_data = []
                    for key, bucket in buckets.items():
                        bucket_item = {"key": key, "doc_count": bucket["doc_count"]}
                        bucket_data.append(bucket_item)

                    bucket_df = pd.DataFrame(bucket_data)
                    st.dataframe(bucket_df)

            elif "value" in agg_data:
                # Metric aggregation (avg, sum, min, max, etc.)
                st.write(f"Value: {agg_data['value']}")

            elif "values" in agg_data:
                # Percentiles aggregation
                st.write("Percentiles:")
                percentiles_df = pd.DataFrame(
                    {
                        "Percentile": list(agg_data["values"].keys()),
                        "Value": list(agg_data["values"].values()),
                    }
                )
                st.dataframe(percentiles_df)

            elif all(key in agg_data for key in ["count", "min", "max", "avg", "sum"]):
                # Stats aggregation
                stats_df = pd.DataFrame([agg_data])
                st.dataframe(stats_df)

            else:
                # Other aggregation types
                st.json(agg_data)

            st.markdown("---")

    # Check if we have hits
    if "hits" in results and results["hits"]["hits"]:
        st.subheader("Search Results")
        hits = results["hits"]["hits"]
        extracted_data = [hit["_source"] for hit in hits]

        # Convert to a pandas DataFrame
        df = pd.DataFrame(extracted_data)

        # Display the DataFrame as a table
        st.write(f"Found {results['hits']['total']['value']} documents:")
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
