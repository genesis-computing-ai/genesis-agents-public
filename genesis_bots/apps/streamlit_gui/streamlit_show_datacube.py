from decimal import Decimal
import streamlit as st
import pandas as pd
from ...genesis_bots.core.logging_config import logger
# with st.echo():
#     st.write(st.__version__)
#     st.help(st.dataframe)

NativeMode = False
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
    NativeMode = True
except:
    from ...genesis_bots.connectors.snowflake_connector.snowflake_connector import SnowflakeConnector
    snowflake_connector = SnowflakeConnector(connection_name='Snowflake')

def run_query(sql:str, max_rows:int):
    if st.session_state.get('generated_queries'):
        st.session_state.generated_queries += sql + '\n\n'
    if NativeMode:
        result = session.sql(sql).limit(max_rows).to_pandas() #collect()
        return result
    else:
        result = snowflake_connector.run_query(sql, max_rows=max_rows, max_rows_override=True)
        try:
            column_names = [col for col in result[0].keys()]
            return pd.DataFrame(result, columns=column_names)
        except Exception:
            return result

def get_expand_paths_filter(df:pd.DataFrame, column_names:list, open_paths:list, group_by_columns:list) -> list:
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

def expand_paths(df:pd.DataFrame, open_paths:list, base_query:str, column_names:list, column_types:dict, group_by_columns:list, filter_conditions:list,
                 expand_path_level:int) -> pd.DataFrame:
    if expand_path_level > len(group_by_columns)+1:
        return df
    inner_group_by_columns = group_by_columns[expand_path_level:expand_path_level+1]
    outter_group_by_columns= group_by_columns[:expand_path_level]
    filters_with_locations = get_expand_paths_filter(df, column_names, open_paths, outter_group_by_columns) #group_by_columns[:expand_path_level])
    expanded_dfs = []
    for filter, location in filters_with_locations:
        if filter_conditions is None:
            filter_conditions_per = [filter]
        else:
            filter_conditions_per = filter_conditions + [filter]
        df2, df2_col_names, df2_col_types = use_run_query(base_query, column_names, column_types, group_by_columns=inner_group_by_columns, filter_conditions=filter_conditions_per,
                            expand_open_paths=True, expand_path_level=expand_path_level)
        df2[outter_group_by_columns] = '├──'
        expanded_dfs.append((filter, location, df2))

    output_df = df.copy()
    for filter, location, df2 in expanded_dfs:
        # Insert each df2 into the location in output_df, adding each incremental offset to the next location
        offset = 0
        for filter, location, df2 in expanded_dfs:
            insert_location = location + offset + 1
            output_df.at[insert_location-1, 'is_expanded'] = True
            output_df = pd.concat([output_df.iloc[:insert_location], df2, output_df.iloc[insert_location:]]).reset_index(drop=True)
            offset += len(df2)
    return output_df

# Function to run SQL query and return results
def use_run_query(query, column_names:list=["*"], column_types:dict={}, group_by_columns=[], filter_conditions=None,
                  expand_open_paths=False, expand_path_level=0):
    open_paths = st.session_state.get('open_paths', [])
    base_column_names = column_names
    base_group_by_columns = group_by_columns

    #logger.info(f"use_run_query - open_paths = {open_paths}")

    # Initialize SnowflakeConnector
    #snowflake_connector = SnowflakeConnector(connection_name='Snowflake')

    # Ensure the original query is wrapped correctly and ends with a semicolon
    base_query = query.strip().rstrip(';')

    # Construct the CTE
    cte_query = f"WITH subquery AS ({base_query})"

    # Truncate the group_by_columns list to only include columns up to the current expand_path_level
    group_by_columns = group_by_columns[:expand_path_level+1]

    # Remove group_by_columns from column_names
    column_names = [col for col in column_names if col != '__UNIQUE_ID__']

    # Reorder column_names to match the order in base_column_names
    column_names = sorted(column_names, key=lambda x: base_group_by_columns.index(x) if x in base_group_by_columns else len(base_group_by_columns))

    if group_by_columns and len(group_by_columns) > 0:
        agg_l = lambda col: f"sum({col})" if str(column_types.get(col, '')).lower() in ['int', 'int64', 'float', 'double', 'decimal'] else f"min({col})"

        column_names_with_aliases = [f"{agg_l(col)} as {col}" for col in column_names if col not in group_by_columns]
        column_names              = [f"{agg_l(col)}" for col in column_names if col not in group_by_columns]
    else:
        column_names_with_aliases = column_names

    # Start the main query
    group_by_str = ', '.join(group_by_columns) if group_by_columns else ''
    if len(group_by_columns) == 1:
        group_by_str = group_by_columns[0]
    column_names_str = ', '.join(column_names) if len(column_names) > 1 else ''.join(column_names)
    if '*' in column_names:
        column_names_str_with_aliases = column_names_str
    else:
        column_names_str_with_aliases = ', '.join(column_names_with_aliases)
    unique_id_expr = f"HASH(TO_VARIANT(CONCAT({column_names_str}))) as __UNIQUE_ID__"
    if group_by_str:
        main_query = f"SELECT {group_by_str}, {column_names_str_with_aliases}, {unique_id_expr} FROM subquery"
    else:
        main_query = f"SELECT {column_names_str_with_aliases}, {unique_id_expr} FROM subquery"
    if filter_conditions:
        main_query += " WHERE " + " AND ".join(filter_conditions)

    if group_by_columns:
        main_query += " GROUP BY (" + ", ".join(group_by_columns) + ")"
        main_query += " ORDER BY " + ", ".join([f"{col} NULLS LAST" for col in reversed(group_by_columns)])

    final_query = f"{cte_query} {main_query}"#;"

    logger.info(final_query)
    # Use the run_query method from SnowflakeConnector to execute the final query
    result_set = run_query(final_query, max_rows=1000)
    # Handle case where result_set is a dict with Success key = False
    column_names = []
    if isinstance(result_set, dict) and not result_set.get("Success", True):
        st.error(f"Query execution failed: {result_set.get('Error', '')}")
        df = pd.DataFrame()  # Return an empty DataFrame in case of failure
        column_types = {}
    else:
        if len(result_set) > 0:
            df = result_set
            column_names = df.columns.tolist()
            df.insert(0, 'is_expanded', False)
            column_types = df.dtypes.to_dict()
            for col in df.columns:
                if df[col].dtype == 'object' and isinstance(df[col].iloc[0], Decimal):
                    column_types[col] = 'decimal'

            if open_paths and expand_open_paths:
                # This code filters the open_paths list to include only those paths whose '__UNIQUE_ID__' exists in the DataFrame 'df'.
                matching_open_paths = [open_path for open_path in open_paths if open_path['__UNIQUE_ID__'] in df['__UNIQUE_ID__'].values]
                if matching_open_paths:
                    df = expand_paths(df, matching_open_paths, base_query, base_column_names, column_types, base_group_by_columns,
                                      filter_conditions, expand_path_level+1)
        else:
            df = pd.DataFrame()  # Return an empty DataFrame if result_set is empty
            column_types = {}
    return df, column_names, column_types

# Function to fetch column names for a given SQL query
def fetch_column_names(query):
    # Modify query to fetch no data but column names only
    modified_query = "SELECT * FROM ({}) limit 1".format(query.strip().rstrip(';'))
    # Use use_run_query with fetch_data=False to get column names
    df, column_names, column_types = use_run_query(modified_query) #, fetch_data=False)
    return column_names, column_types

# Main app
def main():
    st.set_page_config(layout="wide")

    # Parse query parameter from URL
    query_params = st.query_params
    #query_params = st.experimental_get_query_params()

    #st.write(query_params)
    sql_query = query_params.get("sql_query", "")#[0]  # Default to empty string if not found
    #sql_query = "select * from spider_data.baseball.all_star"
    if sql_query:
        sql_query = st.text_area(label="Base Query", value=sql_query)

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
        #result_df = result_df.sort_values(by=['is_expanded'] + selected_group_by, ascending=[False] + [True] * len(selected_group_by))
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("Select rows to expand/collapse:")
        with col2:
            def reset_cube():
                st.session_state.open_paths = []
            st.button("Reset", on_click=reset_cube)

        def data_change():
            logger.info(st.session_state["my_data_editor_key"])
            expanded_rows = st.session_state.get('open_paths', [])

            edited_rows = st.session_state["my_data_editor_key"].get('edited_rows', {})
            for row_index, changes in edited_rows.items():
                if 'is_expanded' in changes:
                    row = result_df.iloc[int(row_index)]
                    if changes['is_expanded']:
                        if row['__UNIQUE_ID__'] not in [r['__UNIQUE_ID__'] for r in expanded_rows]:
                            expanded_rows.append(row)
                    elif not changes['is_expanded']:
                        expanded_rows = [row for row in expanded_rows if row['__UNIQUE_ID__'] != result_df.iloc[int(row_index)]['__UNIQUE_ID__']]
            st.session_state.open_paths = expanded_rows

        editable = st.data_editor(result_df, use_container_width=True, key="my_data_editor_key", on_change=data_change)
        row_count = len(result_df)
        st.write(f"{row_count} rows returned.")
        if row_count == 1000:
            st.warning("Warning: The dataset may not be complete. The result set is limited to 1000 rows.")

        if not st.session_state.get('generated_queries'):
            st.session_state.generated_queries = " "
        st.session_state.generated_queries = st.text_area(label="Queries Generated", value=st.session_state.generated_queries, height=200)
    else:
        st.write("Please provide a SQL query in the URL parameter `sql_query`.")
if __name__ == "__main__":
    main()
