# Genesis App

### Local Deployement
1. Get OpenAPI key, ngrok auth token (from ngrok.com)

2. Download and install cursor.sh from https://cursor.sh

3. Open Github app, clone codes to local directory: 
```
git clone https://github.com/genesis-gh-jlangseth/genesis.git
```
4. CD into the app folder:
```
cd genesis
```
5. Setup env variables. you can export these env variables in Terminal when you run. But I added in .zprofile file
```
export SNOWFLAKE_ACCOUNT_OVERRIDE=mmb84124
export SNOWFLAKE_USER_OVERRIDE=GENESIS_RUNNER_JL
export SNOWFLAKE_PASSWORD_OVERRIDE=Gen12349esisBotTest3837
export SNOWFLAKE_DATABASE_OVERRIDE=GENESIS_TEST
export SNOWFLAKE_WAREHOUSE_OVERRIDE=XSMALL
export SNOWFLAKE_ROLE_OVERRIDE=ACCOUNTADMIN
export GENESIS_SOURCE=Snowflake
export GENESIS_INTERNAL_DB_SCHEMA=GENESIS_TEST.<your schema>   #make sure change to your test schema. <genesis_new_jf>
export AUTO_HARVEST=FALSE
export GENESIS_LOCAL_RUNNER=TRUE
export RUNNER_ID=snowflake-1
export NGROK_AUTH_TOKEN=<Ngrok Auth token>  #<2gqB9uPsT5bntp5wHslyK0eh5Dn_48DAg6jQimfWoh3SaNboH>  
export AUTO_HARVEST=false
export OPENAI_API_KEY=<OpenAI api key>      #<sk-proj-6gCIKLsHM3FVh3ioB60dT3BlbkFJIh7ot6cpiwZbBQMKLcE9>
export PYTHONPATH=$PYTHONPATH:"$PWD"
```
** Make sure you have following variables set to correct values
```
GENESIS_INTERNAL_DB_SCHEMA 
OPENAI_API_KEY
NGROK_AUTH_TOKEN
```

6. Open cursor app. Click 'Open a folder' and point to the folder that you cloned from Github respository

- Step 7-11, you can run either in Cursor terminal or native Mac terminal.

7. check and install modules/packages listed in requirements.txt file. 
   - open a terminal window in cursor:
   ``` 
   pip install -r requirements.txt
   ```
   - check the log to see if any missing modules/packages. you can output the log to a file to help you 

8. Run backend: open a terminal window:
```
python demo/bot_os_multibot_1.py
```
9. Run Frontned: once #8 completed, run in another terminal window. This step will bring up 'Genesis Bots Configuration' page in Browser.
```
streamlit run ./streamlit_gui/streamlit_sis_v1.py
```
10. You can go to http://localhost:8501/ in a browser and this will bring you to 'Genesis Bots Configuration' page.

11. Select 'Chat with Bots' to talk to the app.
