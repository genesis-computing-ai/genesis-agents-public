

# Title
Genesis Bots are AI-powered workers that can perform jobs for your company.

## Permissions
In the setup guide, you'll be asked to grant additional privileges from your account.

Once you install enesis, you will be directed to a Streamlit app, which will walk you through running commands
in your Snowflake account to grant the application access to the following resources:

1. A Snowflake Virtual Warehouse to power Snowflake queries run by Genesis
2. A Snowflake Compute Pool to run the Genesis Server containers
3. A Network Rule and External Access Integration, to allow Genesis to access the following external endpoints:
    - OpenAI API
    - Slack
4. Optionally, access to any of your existing Databases, Schemas, and Tables you'd like to use with Genesis.

### Account level privileges

`BIND SERVICE ENDPOINT` on **ACCOUNT**
To allow Genesis to open two endpoints, one for Slack to authorize new Apps via OAuth, and one for inbound
access to the Streamlit Genesis GUI

`CREATE COMPUTE POOL` on **ACCOUNT**
To allow Genesis to create a Small Snowflake Compute Pool to run the application"

`IMPORTED PRIVILEGES` ON **SNOWFLAKE DB**
(Optional) Allow GenBots access to query account_usage views

### Privileges to objects
`USAGE` on **COMPUTE POOL**
To run the Genesis Server containers in Snowpark Conrainer Services

`USAGE` on **WAREHOUSE**
For Genesis to run queries on Snowflake

`USAGE` on **EXTERNAL ACCESS INTEGRATION**
To allow Genesis to access external OpenAI and Slack API endpoints

`USAGE` on **DATABASES, SCHEMAS**
To optionally allow Genesis to work with some of your data

`SELECT` on **TABLES, VIEWS**
To optionally allow Genesis to work with some of your data

---

## Object creation
In the setup guide, you'll be asked to create the following object(s) in your account. 

`WAREHOUSE`XSMALL
For Genesis to use to run queries on Snowflake

`COMPUTE POOL`GENESIS_POOL
For Genesis to use to run its Genesis Server containers

`DATABASE`GENESIS_LOCAL_DB
To store the network rule

`SCHEMA`GENESIS_LOCAL_DB.SETTINGS
To store the network rule

`NETWORK RULE`GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
To allow Genesis to access to required external APIs (OpenAI and Slack)

`EXTERNAL ACCESS INTEGRATION`GENESIS_EAI
To allow Genesis to access to required external APIs (OpenAI and Slack)


---

## Setup code

-- Note: Please use the default Streamlit App for a full walkthrough of these steps

-- use a role with sufficient privileges for the

use role <authorized role>;

-- set the name of the installed application and warehouse to use

set APP_DATABASE = 'GENESIS_BOTS';
set APP_WAREHOUSE = 'XSMALL';  -- ok to use an existing warehouse

-- create the warehouse if needed

CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($APP_WAREHOUSE)
 MIN_CLUSTER_COUNT=1 MAX_CLUSTER_COUNT=1
 WAREHOUSE_SIZE=XSMALL AUTO_RESUME = TRUE AUTO_SUSPEND = 60;

-- allow Genesis to use the warehouse

GRANT USAGE ON WAREHOUSE  IDENTIFIER($APP_WAREHOUSE) TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- remove an existing pool, if you've installed this app before

DROP COMPUTE POOL IF EXISTS GENESIS_POOL;

-- create the compute pool and associate it to this application

CREATE COMPUTE POOL IF NOT EXISTS GENESIS_POOL FOR APPLICATION IDENTIFIER($APP_DATABASE)
 MIN_NODES=1 MAX_NODES=1 INSTANCE_FAMILY='CPU_X64_S' AUTO_SUSPEND_SECS=3600 INITIALLY_SUSPENDED=FALSE;

-- give Genesis the right to use the compute pool

GRANT USAGE ON COMPUTE POOL GENESIS_POOL TO APPLICATION  IDENTIFIER($APP_DATABASE);

-- create a local database to store the network rule (you can change these to an existing database and schema if you like)

CREATE DATABASE IF NOT EXISTS GENESIS_LOCAL_DB; 
CREATE SCHEMA IF NOT EXISTS GENESIS_LOCAL_DB.SETTINGS;

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack 

-- create a network rule that allows Genesis Server to access OpenAI's API, and optionally Slack API and Azure Blob (for image generation) 
CREATE OR REPLACE NETWORK RULE GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE
 MODE = EGRESS TYPE = HOST_PORT
VALUE_LIST = ('api.openai.com', 'slack.com', 'www.slack.com', 'wss-primary.slack.com',
'wss-backup.slack.com',  'wss-primary.slack.com:443','wss-backup.slack.com:443',
'oaidalleapiprodscus.blob.core.windows.net:443', 'downloads.slack-edge.com', 'files-edge.slack.com', 'slack-files.com',
'files-origin.slack.com', 'files.slack.com', 'global-upload-edge.slack.com','universal-upload-edge.slack.com');

-- create an external access integration that surfaces the above network rule

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION GENESIS_EAI
   ALLOWED_NETWORK_RULES = (GENESIS_LOCAL_DB.SETTINGS.GENESIS_RULE) ENABLED = true;

-- Allows Slack to access the Genesis server to approve new Genesis Slack Applications

GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO APPLICATION  IDENTIFIER($APP_DATABASE); 

-- grant Genesis Server the ability to use this external access integration

GRANT USAGE ON INTEGRATION GENESIS_EAI TO APPLICATION   IDENTIFIER($APP_DATABASE);

## Setup instructions

Please use the default Streamlit provided with this native application for a fully-guided setup experience.

## Usage Snippets

Please use the default Streamlit to interact with the Genesis application.

