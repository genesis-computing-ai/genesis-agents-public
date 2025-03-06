import streamlit as st
import pandas as pd
from utils import ( get_metadata)
import json
from .components import config_page_header

def db_harvester():
    config_page_header("Harvester Status")
    harvest_control = get_metadata("harvest_control")
    harvest_summary = get_metadata("harvest_summary")

    if harvest_control == []:
        harvest_control = None
    if harvest_summary == []:
        harvest_summary = None

    # Initialize empty DataFrames with appropriate columns if no data is present
    if harvest_control:
        harvest_control_df = pd.DataFrame(harvest_control).rename(columns=str.lower)
        harvest_control_df["schema_exclusions"] = harvest_control_df[
            "schema_exclusions"
        ].apply(lambda x: ["None"] if not x else x)
        
        # Add safe JSON parsing for schema_inclusions
        def safe_parse_inclusions(x):
            if isinstance(x, list):
                return x
            if not x:
                return ["All"]
            if isinstance(x, str):
                try:
                    parsed = json.loads(x)
                    return parsed if isinstance(parsed, list) else ["All"]
                except json.JSONDecodeError:
                    return ["All"]
            return ["All"]
            
        harvest_control_df["schema_inclusions"] = harvest_control_df[
            "schema_inclusions"
        ].apply(safe_parse_inclusions)
    else:
        harvest_control_df = pd.DataFrame(
            columns=[
                "source_name",
                "database_name",
                "schema_name",
                "schema_exclusions",
                "schema_inclusions",
                "status",
                "refresh_interval",
                "initial_crawl_complete",
            ]
        )

    column_order = [
        "source_name",
        "database_name",
        "schema_name",
        "role_used_for_crawl",
        "last_change_ts",
        "objects_crawled",
    ]

    if harvest_summary:
        harvest_summary_df = pd.DataFrame(harvest_summary).rename(columns=str.lower)
        # Reordering columns instead of sorting rows
        harvest_summary_df = harvest_summary_df[column_order]
        # Calculate the sum of objects_crawled using the DataFrame
        total_objects_crawled = harvest_summary_df["objects_crawled"].sum()
        # Find the most recent change timestamp using the DataFrame
        most_recent_change_str = str(harvest_summary_df["last_change_ts"].max()).split(
            "."
        )[0]
    else:
        harvest_summary_df = pd.DataFrame(columns=column_order)
        total_objects_crawled = 0
        most_recent_change_str = "N/A"

    harvester_status = "Active" if harvest_control and harvest_summary else "Inactive"
    # Display metrics at the top
    col0, col1, col2 = st.columns(3)
    with col0:
        st.metric(label="Harvester Status", value=harvester_status)
    with col1:
        st.metric(label="Total Objects Crawled", value=total_objects_crawled)
    with col2:
        st.metric(label="Most Recent Change", value=most_recent_change_str)

    st.subheader("Sources and Databases being Harvested")
    st.markdown(
        "Note: It may take 2-5 minutes for newly-granted data to appear here, and 5-10 minutes to be available to the bots"
    )

    # Display the DataFrame without additional JSON parsing
    if not harvest_control_df.empty:
        st.dataframe(harvest_control_df, use_container_width=True)

    st.subheader("Database and Schema Harvest Status")
    if not harvest_summary_df.empty:
        st.dataframe(harvest_summary_df, use_container_width=True, height=300)
    else:
        st.write("No data available for Database and Schema Harvest Status.")