import streamlit as st
from connectors.snowflake_connector import SnowflakeConnector
import pandas as pd
#from st_aggrid import AgGrid

# with st.echo():
#     st.write(st.__version__)
#     st.help(st.dataframe)


# Function to run SQL query and return results
def use_run_query(query, column_names:list=["*"], column_types:dict={}, group_by_columns=[], filter_conditions=None):
    open_paths = st.session_state.get('open_paths', [])

    print(f"use_run_query - open_paths = {open_paths}")
    
    # Initialize SnowflakeConnector
    snowflake_connector = SnowflakeConnector(connection_name='Snowflake')
    
    # Ensure the original query is wrapped correctly and ends with a semicolon
    base_query = query.strip().rstrip(';')
    
    # Construct the CTE
    cte_query = f"WITH subquery AS ({base_query})"
    
    # Remove group_by_columns from column_names
    if group_by_columns and len(group_by_columns) > 0:
        column_names = [
            f"sum({col})" if column_types.get(col,'') in ['int', 'float'] else f"min({col})"
            for col in column_names if col not in group_by_columns
        ]
    # Start the main query
    group_by_str = ','.join(group_by_columns) if group_by_columns else ''
    if len(group_by_columns) == 1:
        group_by_str = group_by_columns[0]
    column_names_str = ','.join(column_names) if len(column_names) > 1 else ''.join(column_names)
    if group_by_str:
        main_query = f"SELECT {group_by_str}, {column_names_str} FROM subquery"
    else:
        main_query = f"SELECT {column_names_str} FROM subquery"
    
    if filter_conditions:
        main_query += " WHERE " + " AND ".join(filter_conditions)
    
    if group_by_columns:
        main_query += " GROUP BY (" + ", ".join(group_by_columns) + ")"
        main_query += " ORDER BY " + ", ".join([f"{col} NULLS LAST" for col in reversed(group_by_columns)])
    
    final_query = f"{cte_query} {main_query};"
    
    print(final_query)
    # Use the run_query method from SnowflakeConnector to execute the final query
    result_set = snowflake_connector.run_query(final_query, max_rows=1000, max_rows_override=True)
    # Handle case where result_set is a dict with Success key = False
    column_names = []
    if isinstance(result_set, dict) and not result_set.get("Success", True):
        st.error(f"Query execution failed: {result_set.get('Error', '')}")
        df = pd.DataFrame()  # Return an empty DataFrame in case of failure
        column_types = {}
    else:
        if result_set:
            column_names = list(result_set[0].keys())
            df = pd.DataFrame(result_set, columns=column_names)
            column_types = {col: type(result_set[0][col]).__name__ for col in column_names}
        else:
            df = pd.DataFrame()  # Return an empty DataFrame if result_set is empty
            column_types = {}
    return df, column_names, column_types

# Function to fetch column names for a given SQL query
def fetch_column_names(query):
    # Modify query to fetch no data but column names only
    modified_query = "SELECT * FROM ({}) limit 1".format(query)
    # Use use_run_query with fetch_data=False to get column names
    df, column_names, column_types = use_run_query(modified_query) #, fetch_data=False)
    return column_names, column_types

# Main app
def main():
    # Parse query parameter from URL
    query_params = st.query_params
    sql_query = query_params.get("sql_query", "")#[0]  # Default to empty string if not found

    if sql_query:
        # Fetch column names for the SQL query
        column_names, column_types = fetch_column_names(sql_query)
        
        # Widget to select group by columns
        selected_group_by = st.multiselect("Select columns to group by:", options=column_names)
        
        # Widget to input filter conditions
        filter_input = st.text_input("Enter filter conditions (e.g., column_name > 100):")
        filter_conditions = [filter_input] if filter_input else None
        
        # Run query and display results
        result_df, output_column_names, output_column_types = use_run_query(sql_query, column_names=column_names, column_types=column_types, 
                                                group_by_columns=selected_group_by, filter_conditions=filter_conditions)
                                                #open_paths = st.session_state.get('open_paths', []))
        
        st.write("Select rows to expand:")
        
        # Display the dataframe with a single expand button
        event = st.dataframe(result_df, use_container_width=True, on_select="rerun", selection_mode="multi-row") #, 
        if event.selection.rows != st.session_state.get('selected_rows', []):
             st.session_state.selected_rows = event.selection.rows
             st.session_state.open_paths = [result_df.iloc[r] for r in event.selection.rows]
             st.rerun()

        #handle_expand()
        
        st.write(f"{len(result_df)} rows returned.")
    else:
        st.write("Please provide a SQL query in the URL parameter `sql_query`.")
if __name__ == "__main__":
    main()
