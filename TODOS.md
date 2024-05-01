

GENESIS BOT TODOS
=================

CLEANUP:
Make Eliza demo video on baseball
Change accountadmin 
Change endpoints query to the framework version
Eve deploy on fresh install complained about file types for null files
Check resubmitting key via streamlit with a valid key alresady in place , gives timeout errot, also clarify page that you dont need to do this again
MR - (in progress) upgrades of native app, Make patch upgrade of app work in NA/SPCS
MR - (in progress) add log sharing back to provider
MR - Expose harvest tables to app public so user can read write backup and restore
MR - add Spider baseball and f1 schemas as share-thru app w/pre-made harvest results for demo data on start
MR - (soon) add tab to see service logs for service and harvester
MR - (soon) add tab to see chat logs from messages table
Harvest semantic models and return in get_metadata dynamically
Give semantic index modify a way to add and modify multiple things at the same time
Updating bot instrucrions when wrong / invialid botid provided not sending error back to Eve 
Try harvester with mistral or yak model to save costs 
Add update message back to slack thread if tools are still running for more than a minute or if the run is still thinking.. (update her Thinking message)
Make thread map save to local database to survive container restart 
In install of Eliza make and grant an external workspace database and tell Eliza about it in her prompt (include grants on future objects to accountadmin)
(test in NA, and add a grant example w/imported privs) Test new harvester ddl on a shared database 
(test in na) Fix vision-chat-analysis, add to available_tools, give to eliza and stuart by default
(testing) Figure out why chat_vision_analysis isn't seeing the files provided via slack upload 
(soon) Give them 100 OpenAI turns a day or someting using our key, then have it switch to their own key
(have a rough one) make a metadata backup and recovery script so we have it ready
(soon) Make sure deploy button before slack keys activated tells you what to do (e.g. put in slack config keys first)
(soon) Add undeploy from Slack button on bot config
(test) Make sure harvester works ok with mixed case table and database and schema names (and system in general)
(soon) harvester dont crash if cant access schemas for a database listed in control file
(soon) make sure endpoint is not the empty message, if so wait until its provisioned before updating any callback URLS, if there are any bots that needs them
(soon) fix wait spinner on api key page
(soon) Make the thinking message go away when a bot decides not to respond
(soon) Add a sevice restart button to SiS
(later) Add a way for Eve for example to add another bot to a channel and then not process that thread anymore unless tagged again
(later) add a place in Streamlit to see the files in stage add a file, remove a file
(soon) add files list to bot configurations page in SiS
(later) Make the queries in bot_os_memory.py parameterized
(later) app deploy tokens are user specific, how to add a collaborator so another user can configure it?
(later) Hide harvestable data if autoharvest is on 
(later) add refresh button to harvester_status
(later) Add logo to streamlit GUI
(later) remove bots vector indexes when removing a bot
(later) Share baseball and formula 1 tables from provider
(later) Give a way for local streamlit to upload and download files from the botos server
(later) Go back to snowflake secure mode for harvester too if it works

! Persistancey:
1. have it proactively suggest tasks to do if it doesn't have enough, and recurring ones, and bootstrap ones
2. have it do those automatically (give it the system time)
3. give it a tool to find stuff its working on and the status
4. give it a tool to see what else its been talking about with that person and with other people
Tasks to database, json describing the task (let it decide its own structure for this json, give ideas like todo, current status, etc.).. let it decide the structure
Have next check time in the database, and prompt it to work on it more at that time
Example task, monitor the harvester for a new database and let me know when its done or if there are any issues (with related discussion context)
Add some knowledge of past or parallel tasks
Add knowledged of past and parallel conversations, and a tool to get past transcriptips
Have it proactively suggest future tasks
Have Eve suggest making tools 
Haves bots reach out to you to see if you need help

CLEANUPS AND NEEDED TESTS:
(later) Make the file downloader more indepenent of the Slackbot, right now it assumes files come from Slackbot, but they could come from Email as well, for example
(test) Test Asynch tool calling: for long tunning database queries for example
Make all queries use bind variables to prevent SQL injection

BIG THINGS:
x Add a semantic YAML steward tool... the AI feeds it piece by piece, asks it whats missing, and fills it in until its complete ...& make stuart bot and demo 
(in progress) Semantic CoPilot tools, Semantic model creation and maintenance, related demos, add to Elsa flows and to metadata search results
Harvester: robustness and improvements
Memory system: revamp and improve (go beyond simple RAG, back to KB?, post-chat capture, ongoing refinement, local indexing)
More advanced tool generation and testing system, and with Zapier hooks via Zapier API 
Data engineering use case: define it and make it really work and be robust, with Iceberg migration as a core real-world-needed example
Reflect/Validation: revamp and rework to allow bots to review/revise/critique their work autonomously before presenting to the user (maybe spawn critique threads)
Planning: tasks/reminders allowing bots to have long running projects
Allow a bot session to reason over all of its threads (e.g., should be able to ask Eve on Streamlit about what she is doing in one of her Slack threads)
Wrap snow_udf as a sdk to be integrated within python apps like State Street Alpha
Bot health monitoring (is this )
Bot cost management, reporting, optimization
Add initial message, tasks and reminders to make genbots proactive
Parallel sub-bot runs (take a task, divide it by x dimension, trigger sub-bots in parallel)

MEDIUM THINGS:
!-> Harvester: Have harvester only get sample data for known simple data types (data_harvest table tripps it up for example), and add error handling on sample data not available, and on any other issues
(try reka-core 128k in april) Model backends: Test & figure out when we can use Cortex-native models (Mistral, Reka, etc)
Azure OpenAI support
(done??) SiS - don't crash sis app if you submit another line while its thinking
(is this done?) Images/docs: Add image analysis and image production using openai vision mode, need to add as a separate tool
(done??) Images/docs: Add/test document retreival for documents that the AI produces (images and non images) so they show up in Slack
Harvester add error handling and logging
Harvester test it in various ways 
(asked) SiS how to get "open in worksheet" button for setup scripts
Prevent bots from deleting themselves
Have Harvester use Mistral for descriptions and embeddings
Make an example of another native app embedding Genesis via UDF calls

SMALL THINGS:
Streamlit: move the bot selector somewhere easier to find after it scrolls off (left nav for example when on the chat tab) 
Have the bot authorization redirect provide a pretty webpage confirming the bot has been installed, and telling you how to 
Add remove tool tool
Make available_tools table rows runner-dependent
allow for a mode without slack API keys where users get a manifest and they create the bots themselves

NICE TO HAVES:
Use openai and/or database-hosted vector search (snow when fast, bq)
Monetization, billing, etc.
Post tool progress to streamlit/snow_udf adapters (not applicable to slack/email adapters)
x SiS app - Eve walks you through the setup steps including script runs
Files and images and non-text stuff in Streamlit
Harvester: give sample data to table summarizer before it generates its blurb, now only getting columns
Harvester: first put things to crawl into the table but marked as to_crawl, so elsa can talk about its progress, crawl from that
SiS app, see past chats (like chatgpt gui)
Harvester: handle removals of databases, schemas, tables (validate stuff in index still belongs there basically w.r.t. control table)
Add images and files in/out to chat log table
Handle more elegantly when a message comes in on a thread that already has a run running (keep them in a queue to include all at once on the next run?)
Add a deletion protection flag column in the bot_serving table
Streamlit: add a tools page to see tools and code
Add a mechanism for license control based on current_account()... share a table with a filtered secure view with license info for the account
    If nothing shared, then after some period of time, display a warning to contact, then eventually go into limited mode
    Have a trial period where you can use it in trial mode before it goes into limited mode

DONE:
x (soon) Spider data loader, fix nil and '' numeric loading to get full baseball data in (or go back to strings)
x (soon) Harvest all spider data once nil/'' fix is in / Make harvester work on all Spider tables
x (soon) Test Upgrades & backup (made manual version) Add a backup and restore metadata function to SiS
x (soon) Make harvester fall back to "describe table" if get_ddl doesn't work (for shared objects) then test on weather data 
x Have Stuart put semantic stages to a standard place and CREATE STAGE my_int_stage ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
x Trap openai errors during execute_run such as out of credits, rate limit exceeded, and provide a message back to the user 
x (ready to test) Native app, Streamlit, & onboarding: make it seamless and easy
x Store thread_id as metadata in slack messages so we can recover on restarts
x Don't let it update the same semantic model in multiple threads at the same time
x Have semantic creator make sure sample values are in quoted strings
x Long messages sent to Eve via Streamlit don't seem to work (like updatig Stuarts instructions) (was quote bug i think)
x dont add file_search tool onto attachments that are unsupported (PNG)
x Put LLM keys into a table at the runner level, with a default llm, put llm choice and keys into bot_serving table to override default llm
n Native app needs create table permissions to make its harvester tracking stuff
n Native app share view of harvesting table to app_public 
n Try this search system: https://community.openai.com/t/new-assistants-browse-with-bing-ability/479383/5
x Make sure documents uploaded to openai are durable day to day if not store them in database and re-upload them for each assistant session run?
n Add support for ngrok static domains, and don't update Slack when it hasn't changed
x Add Allison and Eve both up front both, give tools to them respectively
x now n/a Ngrok auto-provision, optionally we provide the ngrok key to customers
x (not as good as 1106...) Switch openai vision and image stuff to new GA gpt4-turbo model
x Make snowflake_connector and snowflake_harvester (uid/pwd and token-in-app-based)
x Test harvester with natapp and token
x What happens if you try to send a direct message from a bot that does not have slack enabled, or a server that does not have slack enabled?
x (havent seen in a while) Handle TPM rate limit errors on job responses from OpenAI, tell the user, back-off and wait, etc.
x Metadata search tool: give the metadata search tool the ability to specify a database and schema optionally
x (try April 15-16) -> Sis - check consumer loading streamlit
x Add an eve bootstrapper to insert an Eve row on startup if no bots at all
x (test) test the deploy button in Sis with fresh install 
x (later) send new versions for review today 
x Give each eve a unique bot_id on creation so they dont share assistants if keys are shared 
x Make URLs displayed in SiS (on bots page and in messages from Eve) non-clickable as they dont work if clicked in Slack
x make eliza start right away too like eve on install
x (removed harvester tools for now) Remove the harvester edit tools for now, remove the available data to harvest page in SiS for now
x (change to my key, test in sis_test, change back) test SiS deploy to slack process in SiS in Natapp (added fn?)
x Make bot configuration SiS page's deploy to slack button work again
x fixed respond only when talked to in Slack
x make sure adding database tools to a new bot works in sockets 
x test natapp, enable slack, make bot with db tools, then give stage tools, see if stage tools work
x test if eve can be deployed to slack 
x finish up files: on add new file, teach it how ot get the new file right away, and on load of system
x Fix adding new bot with files, see if it works with and without files, immediately and on restart
x Fix adding docs to bots function, and check files inclusion in make baby bot
x have get file from stage upload teh file to openai and give it the file id in the tool reponse (can't do?)
x (not needed, file names decoupled) give it a way to rename files in stage
x test native app in 3rd account not in our org
x Add a Websocket mode to do w/o ngrok
x why are threads getting reset
x put files in and out in directories based on thread_id
x MAKE IT WORK WITH LATEST VERSION OF OPENAI API... SOMETHING IS IN WRONG LOCATION, NOW BEING VALIDATED
x update for Assistants API 2.0
x (no need?) Make clean set of instructions for mounting and activating container in SPCS w/o Natapp 
x (test) Is it running some functions twice?
x (testing as we go...) Test socket mode more fully 
x test in nat app
x update readme and submit new version for sec review
x SiS app - Remove NGROK entry tab when running in SiS
x update session ep lookup with correct service name (hardcoded now)
x auto replace {{app name}} in Sis App with correct name (why worked for other table tho?)
x remove api.slack.com
x test semantic api
x on start-stop fix the quotes for USE DATABASE IDENTIFIER(GENESIS_BOTS_ALPHA);
x fix error when no grantable databases in streamlit
x fix grant data script (app name dynamic, $$'s use ::: trick)
x Fix activating first bot on slack
x Put in new streamlit, rebuild app
x Make Submit slack tokens in snowmode handle response properly
x Make sure external Streamlit still works with new changes 
x Merge Matt's new code
x Use snowflake token inside native app
x Have the instance creator sproc specify the instance schema as the GENESIS_INTERNAL DB location
x Have the system create and populate all the metadata tables if they are not created and populated
x Test fresh deployment of all into consumer account
x Test/fix service startup before OpenAI and Ngrok secrets provided
x (seems ok.. monitor) Snowflake token refresh needed on timeout errors?
x Native app consumer SiS onboarding (warehouse?, pool, eai) 
x Test streamlit with manually created app and hard-wired function names while waiting for 8.15
x Fix rest of streamlit GUI
x Test/fix Eve creating bots when deployed inside a native app to slack
x (still need for Slack) Remove the public endpoints, and remove the 120second delay on instance init sproc
x (still need for Slack) Get rid of bind service requirement, test with private udf endpoints
x Move OpenAI credential store to database table like ngrok
x Harvester in Natapp test
x Make initialize drop any existing instance that has same name
x now n/a If you activate Slack without Ngrok, note that Ngrok is also needed in the messages about Slack, and dont offer Deploy button yet in Bot Details
x MAke slack api page only ask for refresh token
x Make SiS harvester tab say "no data harvested yet" vs error when no data in control and or harvest
x Add instructions to grant data to application to SiS
x Have harvester include first 20 columns as a separate field
x Add a flag for database, schema limits for search, and for all fields vs top fields
x now n/a Make ngrok update turn green at the tops 
x Streamlit - harvest status page, harvest manager page
x Add Azure openai caller to spcs harness w/assistant api 
x (csv, but needs recrawl) 7. Harvester: express sample data more compactly (csv, spaces, etc)
x Harvester: crawling refresh automatically trigger on a schedule
x SiS app, multiple bot tabs, multiple chats, 
x Harvester: manually upgrade the existing table with ddl_hash, test new tables, test ddl change, then crawl more with it
x Harvester: crawl the FEC stuff
x Harvester: test summaries using gpt-4-turbo vs gpt-3.5
x Harvester: have it be a separate container
x Harvester: have a flag on the control table for how often to check for new/changed tables (0=Off)
x Harvester control: allow eve to set up the harvester for Elsa
x Harvester Elsa see its status and add things to it that it has access to, 
x Harvester functions to kick off and remove things from the crawl (cause just sql access is too hard / risky)
x Add "Source name" for the metadata as well, Bigquery, Snowflake, etc. to allow for cross-source querying
x rebuild local vector index for newly-crawled stuff on the fly
x Log all convos into database
x Give Eve control over harvester, and Elsa ability to add things to harvester and insight into its status 
x Update baby_bot creater and multi-bot system to not be dependent on BQ, move queries into BQ connector
x Give Eve ability to list current bots, pause them, remove them, add tools to them, change them
Zapier - offical platform integration with proactive instant hook calls, so Eve can wrap any tool from Zapier
Make Semantic YAMLs automatically / smartly
x Auto rotate slack app config tokens every hour, otherwise only rotate when expired (check age before rotating, and store age in database)
x Fix/complete activation of new bots on UDF before they are autorized in Slack, and set Slack_active to N until they are authorized and activated 
x    .. need to have them light up sessions and set the map on the server before the slack-auth is done (decouple these) 
x    .. have the get all bots function in main look for missing udf adapters and then start sessions and adapters for them
x    .. and have the slack activator not start sessions and udf adapters for bots if they are already active
    .. and return the fact that there is a newly activated bot so the streamlit can alert the user and offer to take them to is
x STREAMLIT - ADD A BOTS PAGE WITH A NEW BUTTON THAT GOES TO EVE, ADD AN API KEYS PAGE for llm, ngrok, and slack, 
x Add a new chat button on streamlit GUI
x Add add file to bot tool
