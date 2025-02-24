[![GitHub CI](https://github.com/genesis-gh-jlangseth/genesis/actions/workflows/unittests.yml/badge.svg)](https://github.com/genesis-gh-jlangseth/genesis/actions/workflows/unittests.yml/badge.svg)


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
export SNOWFLAKE_ACCOUNT_OVERRIDE=eqb52188
export SNOWFLAKE_USER_OVERRIDE=GENESIS_RUNNER_**
export SNOWFLAKE_PASSWORD_OVERRIDE=<your runner password>
export SNOWFLAKE_DATABASE_OVERRIDE=GENESIS_TEST
export SNOWFLAKE_WAREHOUSE_OVERRIDE=XSMALL
export SNOWFLAKE_ROLE_OVERRIDE=<authorized role>
export GENESIS_SOURCE=Snowflake
export GENESIS_INTERNAL_DB_SCHEMA=GENESIS_TEST.<your schema>   #make sure change to your test schema. <genesis_new_jf>
export AUTO_HARVEST=FALSE
export GENESIS_LOCAL_RUNNER=TRUE
export RUNNER_ID=snowflake-1
export NGROK_AUTH_TOKEN=<Ngrok Auth token>
export NGROK_BASE_URL=http://localhost:8080
export AUTO_HARVEST=false
export OPENAI_API_KEY=<OpenAI api key>
export PYTHONPATH=$PYTHONPATH:"$PWD"
```

\*\* Make sure you have following variables set to correct values

```
GENESIS_INTERNAL_DB_SCHEMA
OPENAI_API_KEY
NGROK_AUTH_TOKEN
```

6. Open cursor app. Click 'Open a folder' and point to the folder that you cloned from Github respository

- Step 7-11, you can run either in Cursor terminal or native Mac terminal.

7. Check and install modules/packages listed in requirements.txt file.

   - Create a virtual environment:

     - Conda:

     ```
     conda create -n genesis_env python=3.11
     conda activate genesis_env
     ```

     - Pip:

     ```
     python3 -m venv genesis_env
     source genesis_env/bin/activate
     ```

   - Install libraries - open terminal:

   ```
   pip install -r requirements.txt
   ```

   - For local deployment, you need to install additional libraries:

   ```
   pip install ngrok
   pip install aiohttp
   ```

   - Check the log to see if any missing modules/packages. you can output the log to a file to help you

8. Create a new directory for Git files
  - In a terminal, run the following:
```
sudo mkdir /opt/bot_git
sudo chmod o+w /opt/bot_git
git config --global --add safe.directory /opt/bot_git
```

9. Run backend: open a terminal window:

```
python demo/bot_os_multibot_1.py
```

10. Run Frontend: once #8 completed, run in another terminal window. This step will bring up 'Genesis Bots Configuration' page in Browser.

```
streamlit run streamlit_gui/Genesis.py
```

11. You can go to http://localhost:8501/ in a browser and this will bring you to 'Genesis Bots Configuration' page.

12. Select 'Chat with Bots' to talk to the app.

### Notes:

In Windows an extra application is needed for ngrok:

- Download ngrok.exe from https://ngrok-downloads.ngrok.com/ngrok.exe
- Open terminal and run `./ngrok.exe config add-authtoken <ngrok_token>`
- In terminal run `./ngrok.exe http http://localhost:8080`

- If you get this error:

```
ERROR - Failed to send a request to Slack API server: <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: # unable to get local issuer certificate (\_ssl.c:1000)>
```

you may be mising a .pem file. You can do the following:

```
bash
curl --etag-compare etag.txt --etag-save etag.txt --remote-name https://curl.se/ca/cacert.pem
export SSL_CERT_FILE=path/to/cacert.pem
```

Now check to make sure env variable is correct:

```
bash
echo $SSL_CERT_FILE
python -c "import ssl; print(ssl.get_default_verify_paths().cafile)"
```

Another possible issue when deploying the native app in Snowflake using Windows

- if you get an error in native app sayinh entrypoint file not found
  - Open entrypoint.sh in Notepad++ -> Edit -> EOL Conversion -> Unix (LF) -> Save
  - Recreate image using new entrypoint.sh file
