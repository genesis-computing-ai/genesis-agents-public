import streamlit as st
from connectors.snowflake_connector import SnowflakeConnector
import pandas as pd
#from st_aggrid import AgGrid

# with st.echo():
#     st.write(st.__version__)
#     st.help(st.dataframe)

def get_expand_paths_filter(df:pd.DataFrame, column_names:list[str], open_paths:list[pd.DataFrame], group_by_columns:list[str]) -> list[tuple[str, int]]:
    filter_strings_with_locations = []
    for path_df in open_paths:
        path_filter_strings = []
        for col in group_by_columns:
            #col_index = column_names.index(col) if col in column_names else -1
            value = path_df[col]
            path_filter_strings.append(f"{col} = '{value}'")
        if path_filter_strings:
            filter_string = " AND ".join(path_filter_strings)
            location = df.index[df[group_by_columns].isin(path_df[group_by_columns].to_list()).all(axis=1)].tolist()
            if location:
                filter_strings_with_locations.append((filter_string, location[0]))
    return filter_strings_with_locations

def expand_paths(df:pd.DataFrame, open_paths:list[pd.DataFrame], base_query:str, column_names:list, column_types:dict, group_by_columns:list[str], filter_conditions:str,
                 expand_path_level:int) -> pd.DataFrame:
    if expand_path_level > len(group_by_columns):
        return df
    filters_with_locations = get_expand_paths_filter(df, column_names, open_paths, group_by_columns[:expand_path_level])
    expanded_dfs = []
    inner_group_by_columns = group_by_columns[expand_path_level:]
    for filter, location in filters_with_locations:
        if filter_conditions is None:
            filter_conditions_per = filter
        else:
            filter_conditions_per = f"({filter_conditions}) AND ({filter})"
        df2, df2_col_names, df2_col_types = use_run_query(base_query, column_names, column_types, group_by_columns=inner_group_by_columns, filter_conditions=[filter_conditions_per],
                            expand_open_paths=True, expand_path_level=expand_path_level + 1)
        df2[group_by_columns] = '+'
        expanded_dfs.append((filter, location, df2))

    output_df = df.copy()
    for filter, location, df2 in expanded_dfs:
        # Insert each df2 into the location in output_df, adding each incremental offset to the next location
        offset = 0
        for filter, location, df2 in expanded_dfs:
            insert_location = location + offset + 1
            output_df = pd.concat([output_df.iloc[:insert_location], df2, output_df.iloc[insert_location:]]).reset_index(drop=True)
            offset += len(df2)
    return output_df

# Function to run SQL query and return results
def use_run_query(query, column_names:list=["*"], column_types:dict={}, group_by_columns=[], filter_conditions=None,
                  expand_open_paths=False, expand_path_level=0):
    open_paths = st.session_state.get('open_paths', [])
    base_column_names = column_names
    
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
            if open_paths and expand_open_paths:
                df = expand_paths(df, open_paths, base_query, base_column_names, column_types, group_by_columns, 
                                  filter_conditions, expand_path_level+1)
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
                                                group_by_columns=selected_group_by, filter_conditions=filter_conditions,
                                                expand_open_paths=True)
                                                #open_paths = st.session_state.get('open_paths', []))
        
        st.write("Select rows to expand/collapse:")
        
        # Display the dataframe with a single expand button
        event = st.dataframe(result_df, use_container_width=True, on_select="rerun", selection_mode="single-row") #, 
        if event.selection.rows != st.session_state.get('selected_rows', []) and len(result_df) == st.session_state.get('result_df_len',0):
            st.session_state.open_paths = [result_df.iloc[r] for r in event.selection.rows]
            st.session_state.selected_rows = event.selection.rows
            # selected_row_indices = event.selection.rows
            # open_paths = st.session_state.get('open_paths', [])
            
            # for row_index in selected_row_indices:
            #     row_data = result_df.iloc[row_index]
            #     if row_data in open_paths:
            #         open_paths.remove(row_data)
            #     else:
            #         open_paths.append(row_data)
            
            # st.session_state.open_paths = open_paths
            # st.session_state.selected_rows = selected_row_indices
            st.rerun()

        #event.selection.rows = st.session_state.get('selected_rows', [])

        st.session_state.result_df_len = len(result_df)
        
        st.write(f"{len(result_df)} rows returned.")
    else:
        st.write("Please provide a SQL query in the URL parameter `sql_query`.")
if __name__ == "__main__":
    main()
