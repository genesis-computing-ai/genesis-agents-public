import streamlit as st
from .components import config_page_header

def grant_data():
    config_page_header("Grant Data Access")
    import codecs

    st.subheader("Grant Data Access")
    st.write(
        "The Genesis bots can help you analyze your data in Snowflake. To do so, you need to grant this application access to your data. The helper procedure below can help you grant access to read all tables and views in a database to this application."
    )
    st.write(
        "Note! Any bot with the Database Tools will be able to access this data, and when such a bot is deployed to Slack, some bots may be accessible by all Slack users, unless they are configured by Eve to only be usable by select Slack users."
    )
    st.write(
        "So grant data in this manner only to non-sensitive data that is ok for any Slack user to view, or first have Eve limit the access to the Database Tools-enabled bots to only select users on Slack."
    )

    proc_header = """\nCREATE OR REPLACE PROCEDURE GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app(database_name STRING, APP_NAME STRING)    RETURNS STRING LANGUAGE JAVASCRIPT  AS """

    wh_text = (
        f"""
    -- use an authorized role
    
    -- set the name of the installed application
    set APP_DATABASE = '{st.session_state.get("app_name", "")}';

    CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB;
    CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;
    USE SCHEMA GENESIS_LOCAL_DB.SETTINGS;
    USE WAREHOUSE XSMALL; -- or use your warehouse if not XSMALL
    """
        + proc_header
        + chr(36)
        + chr(36)
        + """
        var dbName = `"${DATABASE_NAME.replace(/"/g, '')}"`;
        var connection = snowflake.createStatement({
            sqlText: `SELECT '"' || REPLACE(replace(SCHEMA_NAME,'"',''), '.', '"."') || '"' as SCHEMA_NAME FROM ${dbName}.INFORMATION_SCHEMA.SCHEMATA`
        });
        var result = connection.execute();
        
        while (result.next()) {
            var schemaName = result.getColumnValue(1);
            if (schemaName === '"INFORMATION_SCHEMA"') {
                continue;
            }
            var sqlCommands = [
                `GRANT USAGE ON DATABASE ${dbName} TO APPLICATION ${APP_NAME}`,
                `GRANT USAGE ON SCHEMA ${dbName}.${schemaName} TO APPLICATION ${APP_NAME}`,
                `GRANT SELECT ON ALL TABLES IN SCHEMA ${dbName}.${schemaName} TO APPLICATION ${APP_NAME}`,
                `GRANT SELECT ON ALL VIEWS IN SCHEMA ${dbName}.${schemaName} TO APPLICATION ${APP_NAME}`,
            ];
            
            for (var i = 0; i < sqlCommands.length; i++) {
                try {
                    var stmt = snowflake.createStatement({sqlText: sqlCommands[i]});
                    stmt.execute();
                } catch(err) {
                    // Return error message if any command fails
                    return `Error executing command: ${sqlCommands[i]} - ${err.message}`;
                }
            }
        }  
        return "Successfully granted USAGE and SELECT on all schemas, tables, and views to role " + APP_NAME;
    """
        + chr(36)
        + chr(36)
        + """;

    -- see your databases
    show databases;

    -- To use on a local database in your account, call with the name of the database to grant 
    -- 
    -- Note! any bot with the Database Tools will be able to access this data, and when such a bot is deployed to Slack, 
    -- some bots may be accessible by all Slack users, unless they are configured by Eve to only be usable by select Slack
    -- users. So grant data in this manner only to non-sensitive data that is ok for any Slack user to view, or first have 
    -- Eve limit the access to the Database Tools-enabled bots to only select users on Slack.

    -- Replace <your db name> with the name of your database you want to grant. Note, the database name is case-sensitive
    call GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('<your db name>',$APP_DATABASE);

    -- If you want to grant data that has been shared to you via Snowflake data sharing, use this process below instead
    -- the above:

    -- see inbound shares 
    show shares;

    -- to grant an inbound shared database to the Genesis application 
    -- (uncomment this by removing the // and put the right shared DB name in first)
    // grant imported privileges on database <inbound_share_db_name> to application IDENTIFIER($APP_DATABASE);


    -- If you want to to grant access to the SNOWFLAKE share (Account Usage, etc.) to the Genesis application 
    -- uncomment this by removing the // and run it:
    // grant imported privileges on database SNOWFLAKE to application IDENTIFIER($APP_DATABASE);

    --- once granted, Genesis will automatically start to catalog this data so you can use it with Genesis bots
    """
    )
    st.markdown('<div class="code-box">', unsafe_allow_html=True)
    st.code(wh_text, language="sql")
    st.markdown('</div>', unsafe_allow_html=True)
    # st.text_area("Commands to allow this application to see your data:", wh_text, height=800)