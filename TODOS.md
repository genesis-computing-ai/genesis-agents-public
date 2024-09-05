
GENESIS BOT TODOS
=================

today:
try mini again
reza images
fast mode also give link to direct streamlit gui
smarter / faster toggle in streamlit
switch to  "OPENAI_MODEL_NAME": "gpt-4o-2024-08-06", see how we like it
need handler for long cortex threads
need a place to gather and test the email integrations, add a step for email to the welcome wizard
get mini bot avatars into streamlit
x make sure session switcher still works with 🤖 emoojii instead of chat with 
x is streamlit sending correct user id on sis
x make !model with work streamlit
x highlight the current chat session in the list so you know which one you're on
x check baseball data in the morning on alpha
n link to direct streamlit app via web not sis at bottom 
x try calling http endpoints from sis
x deploy new streamlit
n "assistant found" on key insertion, is it naming based on bot id which arent unique anymore, is that ok... could confuse things.
x upgrade alpha
JD-add process list and info to default prompts if any
JD-give bots process scheduler an action to show past runs and status
n kevins thread re runs
n sanders feedback in general
x test openai key insertion on new install
x test openai key switch with metadata re: embeddings on natapp
(test) remove eliza as a default bot 
n make eliza talk about abseball more cleanly 
x test rezas cortex knowledge logging
x make hybrid table fallback to regular for llm results
x test new streamlit on sis
x add cross region calls to sis 
x test cortex tps
x see how it works in 70b
x fix !model in cortex
JD-make system processes non-editable so we can upgrade them but suggest the user makes a copy if they want to change it
x warehouse test button seems to say success even if not 
x broken link shows on config step pages
x turn on knowledge on Alpha
n make user knowledge message nicer "[Reviewing our past interactions...]"

next version:
make various EAIs for openai , slack, both
add a link to the email to get back into a convo with that user 
make knowledge based on email address so its consistent between slack and streamlit


new streamlit:
use cortex 8b for the welcome message?
thread history in db for streamlit, thread loader from database for slack and streamlit
streamlit takes a long time before submitting welcome job for each bot.. loading avatars?
add bot details caching to new streamlit 

before republishing on natapp:
add stuff for send_email tool to streamlit installation script
add error handling for send_email to tell people how to turn it on if not yet turned on
make send_email use dynamic variables to prevent sql injection
have send_email add stuff on the end to say why the message was sent and from who
add handler for long contexts (if getting close, have the llm condense it)
MR - DEV has missing NATIVE embeddings for shared-in data?
add the cross region stuff to the readme
clean up logging and cust data in logs and run_query
x make streamlit show tool calls better 
add logging to the openai finalizer and the slack finalizer to see who is zapping the history in the messages.. seems to just be on single ..
message threads... it's actually doing it on the message 1 /2 separation step it looks like
processes clearing out their history during slack output during finalization (maybe when it ends with a function call?)
x (seems ok) is the last tool call getting eaten by the openai finalizer?
x see if Janice can see her currency table on Alpha once upgraded
on openai error, retry twice
x give supervisor current date and time, and runner too
x tell supervisor not to nitpick values like names etc
n add an alternate way to wake up a bot on a thread for Jeff's Testy bot
test the password process to make sure it doesnt skip steps on concise
x change silent mode to concise mode
x (test) test out the slack bug, marty sending DMs for Janice 2.0... why .. see if my fix fixes it and if local ones still work
x add cross region calling
x have bots send alerts via email:
streamlit requests for stuff
    https://docs.snowflake.com/en/developer-guide/native-apps/container-about
    https://docs.snowflake.com/en/developer-guide/native-apps/container-compute-pool
JS/KJ/MG/JS: default processes
n (go with janice instead) add sandy bot?
n make sure Eliza doesnt talk about the baseball etc data without running search metadata (ask about allstars she makes it up)sla
put in a check to not allow repetitive tools calls more than n times
x figure out the query calling issue on cortex
test baby bot with new bot id logic
x make send slack message suggest that maybe check the process again for the slack name
x also in process runner add hint to make sure to send to the right channels
x make cortex tool calls not stream when function definitions are being sent out 
n make the process tidy-er aware of the bots tools so it doesnt suggest other ways of doing things
x keep tool calls in the slack messages even upon finalization
x (test) make send email more robust if addresses are not in an array (cortex does this)
x if LLM_RESULTS cant be created as a hybrid table, try as a regular table
  
soon-jl:
x (test) make workspace schema names safe even if have dots in them (sandy 0.1 is causing probs)
x (seems ok) give more hints to the process tidy thing
test openai stop during a process kickoff
update sqllite manage process and schedule process
x make sure openai running process eliza random numbers is !stop-able
(in progress) process scheduler and task server, test with cortex bots 
x comment out semantic tools 
x test an openai bot eve updating instructions and adding and removing tools for a cortex bot
x make file add tools give error back for cortex bots
x update janice , remove semantic, tasks, add process tools
x Kevin - retest
add a bot remove from slack tool (stub added to babybot)
x Kevin - put a new slack token into Alpha and test rotation on startup
x (test w/cortex) redo task system as just as scheduler for processes
x re-test with small changes made friday 11am, commented out semantics fully from db tools
n Test cortex COMPLETE mode more with tool calling
Eve is talking a lot about the uploaded files.  No vector store unless needed?  And some prompt notes?
x make !stop work on OpenAI
x make stop work better on cortex .. try on run process, keeps going.. make sure run is fully cancelled 
if a process is stuck on a step after three tries, have it cancel the process
x have !stop on openai just cancel the run on the thread directly
x make process list not return instructions to avoid cheating, make that SHOW
don't allow baby bot tools to be removed from Eve
x see if a process can stop and get input basedon change I made

willow testing:
eve cant deploy existing bots to slack
bots do things when a differnt bot is directly tagged

Cortex:
make python interpreter work
x (not seen recently) try for error on submit: Failed to connect to Cortex API. Status code: 422 Response: {"message":"required field 'content' is zero value.","request_id":"f88c2e5a-6747-4a4e-a132-79273c1067ad"}
x trying to run a query with run_query with a single quoted string goofs up the tool call, omits the string
x Make add tools to bot and change instructions work for Cortex mode bots
x Streaming mode 
x adding to a thread in progress, the system message isn't included 
x add a test cortex function
x default system to cortex on startup unless openai key is present
MR (test) allow adding of openai key via streamlit after startup
x (test on new install) have initial bots be on cortex if thats whats active
x fix/test on the fly bot engine changing w/relaunch
x MR - harvester system, make it work with cortex
n (refactoring it) - task system, make it get the right llm keys for cortex like multibot does now, and the right instructions for cortex
(test) streamlit screen update for llm key not needed cortex 
(test/fix) allow it to update openai key via streamlit in general
(test) switching to openai and relaunching bots on it 
(test) default all bots to no specified llm
x make task system relaunch when something in a bot chanes (like the llm, instructions, etc.)
x MR - make system start without an openai key, the annoy lookup thing needs one now 
x make update_bot_instructions work ok with cortext bots
x make update_files etc not fail if run on cortex bots (check first)
x if cortex api not pupr, default back to complete()
handle > 128k tokens
x make add bot tools to cortex 
x work on tweaking prompt for suggesting to run tools vs actually running them
x (not needed-aug pupr for REST API) if going with COMPLETE, make it send the structure of the array properly not just string dumped


processes:
update manage_processes in sql_lite connector and test processes on sql_list creating and running and scheduling 
test if update process works
x? make the globals thread id mapped
x fix list processes
(in process) make the task system just a scheduler, use processes for the actual work
make sure that DMs sent from processes can be responded to and bot will know context 
x make it start a new run for each process step and/or when it gets close to 10 min 
make get_next_step make sure process is already kicked off to avoid + error Nonetype and int
make sure the same bot doesnt run the same process at the same time (or make it possible for it to do so, track threads better?)

July:
x Eves stuff test 
x JL-Have task server only reuse/reference existing assistant, not recreate/update it on startup
JL-Why are bots doing other bots tasks?
JL-Why is it losing tools after running for a while? (keep expected tools, and if not there, reload them?)
add error trapping on fail to find or call functions
x test last message of a multi-part message
x returning images
returning images and files on long messages 
tic tax toe not fulid on mini, bot dont respond enough.. perhaps in advice prompt add, or in multi-chain ignore from other bots?

SOON:
x on task runs, have it put the task_id in the DMs and Channel messages that it sends
(test, they shouldn't unless they missed a run) on task server restart, bots re-run tasks
make them not talk so much about the uploaded documents (default prompt try first)
make slack user lookup and send direct slack more robust on how its called, and add instructions more
x have task server pickup newly added bots (when sees a task for a missing bot, add a session)..
task server have it refresh sessions if bot is updated (keep a timestamp for when bot was updated)
x streamlit last 2 characters of bots messages sometimes get cut off
generate files cant get added to stage
n Allow files to be uploaded to stage without downloading them to slack
Only upload a file to vector store if its not already there (and is the same.. using md5, track last md5 submitted) 
Add request for imported privs on snowflake db to manifest (now available in EXT patch 85 for pfizer)
Add Janice as default bot - including way to add default files.
x Fix sis streaming on Dev
x Test uploading lots of files to a folder (botos docs)
x Move available_functions (all_functions) to a central object and log it and monitor it
x (added logging on output submission) when calling a single tool like search metadata they dont respond
x JL-Folder of files upload 
x RV-(couple fixes) Add USERS field to messages log table to keep track of the users involved in a thread or dm
n MG-Do stripe setup for monitized listing
x Give the bots the PDFs on the docs to Eve so she can answer stuff on them, with multipdf uploader to stage and grant of folder to bot  
x JL-Give Kevin's docs to Jenny and see if she can answer q's based on them
x JF-add a way to remove tools from bot
JL-Mistral harvester
x (happens anytime an object is created in the workspace by the bot) MR-task to grant the workspaces to app_public periodically
x MR-task to capture what is granted and then re-grant later if needed
x MR - make this work on multicase call GENESIS_LOCAL_DB.SETTINGS.grant_schema_usage_and_select_to_app('MY_DATA',$APP_DATABASE); (fix in SiS script)
MR-SiS app will restart service is suspended (With pool) but doesnt wake up harvester, errors in SiS log harvest screen
Turn on MFA on accounts
Create an account in each region Gensis has been installed with event logging setup to collect logs
MR - Have ability to give a bot its own oauth token or uid/pwd so it has its own RBAC
JD- Make a bot Testy that tests the other bots (excercises and validates all their tools)
JL- Add a way for user to provide new refresh key when making new bot
(later) MR - harvest - formula1 doesnt come back if you delete the harvest rows
MR-Add a service start/stop/restart buttons to SiS
Files in and out of streamlit
catch missing files from stage at startup, and let the bot know they are missing 
JL-Have DMs also get history if they are not threadded, give the past n DMs too 
x (automatically replicates with LAF) MR-add the bot images table and view to the copy program to other regions
analyzing data that is added first 
(soon) add the ability for send_direct and _channel messages to have created files in them (works for images, not for graphs/pdfs - maybe a tool to save file locally and retrigger thread...?) 
(soon) Add undeploy from Slack button on bot config
JL-(test more on spcs) Something blocks the thinking messages or bolt app when doing image analysis and/or file generation/upload to Slack
RV-Learnings service, learns from each thread once its done about data, schema, tables, general stuff, people, etc. Stores and updates background knowledge. 
RV-Injector to inject the right kind of knowledge into thread on these topics
RV-User understanding system of what bot has done with the user recently (with summaries?)
Ability to load whole stage folder to files for a bot
bots that needs them
(later) Try harvester with mistral or yak model to save costs 
(later) add tab to see chat logs from messages table in SiS 
(soon) Harvest semantic models and return in get_metadata dynamically
(soon) Give semantic index modify a way to add and modify multiple things at the same time
(later) add instructions to when you get bot key on how to add the images too and provide the images
(later) Autogenerate images for new bots, add instructions to the user to apply them when getting the tokens
(later) Consider other uses of class level variables--the snowflake session for example, the annoy index, etc.
(later) eventually remove full message metadata save on input and output openai 
(later) Give them 100 OpenAI turns a day or someting using our key, then have it switch to their own key
(later) make a metadata backup and recovery script so we have it ready
(later) block metadata app1 from user query 
(later) add a place in Streamlit to see the files in stage add a file, remove a file
(later) Make the queries in bot_os_memory.py parameterized
(later) app deploy tokens are user specific, how to add a collaborator so another user can configure it?
(later) Hide harvestable data if autoharvest is on  (now its always hidden)
(later) add refresh button to harvester_status
MR- (later) Add logo to streamlit GUI
(later) remove bots vector indexes when removing a bot
(later) Give a way for local streamlit to upload and download files from the botos server
n (later) Go back to snowflake secure mode for harvester too if it works
n (later) Allow user to turn off sample data
n (later) Make thread map save to local database to survive container restart 
n (later) Encrypt all secrete and change col names

PERSISTANCY TOPIC:
x 1. have it proactively suggest tasks to do if it doesn't have enough, and recurring ones, and bootstrap ones
RV-4. give it a tool to see what else its been talking about with that person and with other people
TV-Add knowledged of past and parallel conversations, and a tool to get past transcriptips
Have it proactively suggest future tasks
Haves bots reach out to you to see if you need help

BIG THINGS:
x Semantic CoPilot tools, Semantic model creation and maintenance, related demos, add to Elsa flows and to metadata search results
TRV-Memory system: revamp and improve (go beyond simple RAG, back to KB?, post-chat capture, ongoing refinement, local indexing)
More advanced tool generation and testing system, and with Zapier hooks via Zapier API 
Data engineering use case: define it and make it really work and be robust, with Iceberg migration as a core real-world-needed example
Reflect/Validation: revamp and rework to allow bots to review/revise/critique their work autonomously before presenting to the user (maybe spawn critique threads)
RV-Allow a bot session to reason over all of its threads (e.g., should be able to ask Eve on Streamlit about what she is doing in one of her Slack threads)
Wrap snow_udf as a sdk to be integrated within python apps like State Street Alpha
Bot health monitoring
Bot cost management, reporting, optimization
Parallel sub-bot runs (take a task, divide it by x dimension, trigger sub-bots in parallel)

MEDIUM THINGS:
error handling on sample data not available, and on any other issues
(try reka-core 128k in april) Model backends: Test & figure out when we can use Cortex-native models (Mistral, Reka, etc)
Azure OpenAI support
(asked) SiS how to get "open in worksheet" button for setup scripts
Prevent bots from deleting themselves
Have Harvester use Mistral for descriptions and embeddings
Make an example of another native app embedding Genesis via UDF calls

SMALL THINGS:
(later) Make the file downloader more indepenent of the Slackbot, right now it assumes files come from Slackbot, but they could come from Email as well, for example
(test) Test Asynch tool calling: for long tunning database queries for example
Make all queries use bind variables to prevent SQL injection
Streamlit: move the bot selector somewhere easier to find after it scrolls off (left nav for example when on the chat tab) 
Have the bot authorization redirect provide a pretty webpage confirming the bot has been installed, and telling you how to 
Add remove tool tool
Make available_tools table rows runner-dependent
allow for a mode without slack API keys where users get a manifest and they create the bots themselves
Add links to docs in setup/config steps (e.g. Setup Slack Connection)
Handle openai citations

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
x (soon) When you send a message to a thread that's already running, queue it up and don't submit another, then consolidate all of them when its ready and send them all at once once the run is done.
x (soon) stop bot back and forth with other bots after a few turns
n (soon) have Eliza more proactively suggest analyzing baseball data if there is no other data, once there is change her prompt to suggest 
x (soon) Add a regular checkin task to check in with the person who DMs them, talk back to able (make sure you can stop it)
n (later) make sure endpoint is not the empty message, if so wait until its provisioned before updating any callback URLS, if there are any 
x JD-Combine bot instructions logic from multibot and task services into a common script
x Task server logs emiting a lot of whitespace when annoy index updates
x JL-Have on the fly bot instruction updates append the extra stuff:  
x put use and bot names in messages next to tags, so the bots know who is who 
x add check to only add an event message to a thread once, once its been successfully accepted
x Work more on bots talking to eachother cleanly in streaming mode
x allow bots using streaming mode to talk to eachother
x make sure other bots respond ok to messages that are via Edits vs net new posts from other bots that are streaming
x fix the files output of this prompt (image still doesnt show up, or stage save doesnt work): @
x make tool call re-submission stop when it gets "call already submitted" or "tool call deleted"
x change to hybrid table for SiS streaming retrieval
x for streaming get files out to work
x test read file from stage with streaming (error about empty run_id)
x generating 3 images in parallel only 2 displayed
x make streamlit work with streaming mode, and show tool calls
x make streaming mode not happen when using task system to run jobs, set a global for interactive mode or something 
x add a spinner or other indicator generation is still in progress on a message and have other bots ignore it until 
on task creation clarify if the task is recurring or one-time, asked to send a joke in 5 min and it started sending every 5 min
x cache access check results for some amount of time, flush if changed
LAF support and test (June 19)
x MR - Workspace for each bot with database tools, granted to app_public
x JL - Check bot file upload
x JL - Add error logging to submit tool results for db errors, retry connections too ?
x (sone?) MR-harvester - change the include flag column to exclude and use that field instead of deleting a row to stop auto harvesting
x 2. have it do those automatically (give it the system time)
x 3. give it a tool to find stuff its working on and the status
x Tasks to database, json describing the task (let it decide its own structure for this json, give ideas like todo, current status, etc.).. let it decide the structure
x Have next check time in the database, and prompt it to work on it more at that time
x Add some knowledge of past or parallel tasks
n Have Eve suggest making tools 
x Planning: tasks/reminders allowing bots to have long running projects
x Harvester: robustness and improvements
x Add initial message, tasks and reminders to make genbots proactive
x (later) figure out cortex runner why its costs are nuts
x Have ability to control who a bot is willing to talk to and take direction from to do things on Slack & via SiS
x Have baby bot check to make sure same name bot doesnt already exist in genesis
n When deploying to Slack, check for existing active bot names of the same name and if its there, review it 
n Initial memory system using vector search on message history?
x (test) -> Harvester: Have harvester only get sample data for known simple data types (data_harvest table tripps it up for example), and add x (test)  SiS - don't crash sis app if you submit another line while its thinking
x (test) Harvester add error handling and logging
x (test) Harvester test it in various ways 
x JL- add note to baseball harvest that its till 2015 , in select * from genesisapp_master.harvest_share.harvest_results;
x (test) JL-Does Task Service update its Annoy index when needed?
x (test) Why does "NEURALIFT_DEMO"."DATA"."ACTIVATION_TABLE" not show as available after harvesting?
x (test) JL-test infoschema cache in harvest
x changing openAI key via streamlit when running gives an error: (bots conflicts with existing job--
x Dont allow Bots with same name to be created
x (test) check error handling for stage tools (added to list, check others)
x (test) Add error checking for missing data or grants to harvester so it doesnt crash on that 
x MR-(test) harvester dont crash if cant access schemas for a database listed in control file
x MR-(test) Make sure harvester works ok with mixed case table and database and schema names (and system in general)
x (test Eliza new message is she proactive on baseball and knows its only till 2015?)
x (test) MR-list index out of range on bot config when you refresh directly to it.. did add more retrys fix it?
x (test) This table cant be found once harvested: .. why? CREATE TABLE "RAW_WIKIPEDIA"."EVENTS"."WIKICHANGES" (
x MR-Add a message to the top of the SisChat page suggesting activating via slack, via a temp workspace
x ADD CHECKING FOR ACCESS IN SLACK
x Add to baby bot selection of all access or no access on slack
x Add slack allow list handling info on make baby bot (add ask for make it open or closed?)
x JL- sander feedback on doc 
x (test) Clean up logs (check queries )
n Add a note to Eliza to not just dump data in non-DMs
x (test) adding stage tools to a bot with baby_bot_tools and see if instructions are updated with internal stage location
x new install - (test) on first DM with a user, add some introduction of yourself
x (test Eliza new message is she proactive on baseball and knows its only till 2015?)
x new install - (test) GENESIS_LOCAL_DB.ELIZA_WORKSPACE Create sample workspace by default for Eliza for Eliza.. update her prompt
x (test) Check this function execute_function - _get_visible_tables - {"database":"my_data","schema":"public"}
x (test) fix add_new_tools_to_bots, 2024-05-10 23:32:09,104 - ERROR - callback_closure - _submit_tool_outputs - caught exception: argument of type 
x (test) JL-files issue Chris and Robert are seeing -- use new logging to debug
n (include grants on future objects to accountadmin)
x (test) Harvester log make it less explicit on data 
x (test) deploy bot flow, make sure new message shows up
x (test) adding tools again, adding autonomouss tasks via eve didn't seem to work without restarts
x (test) eve coudlnt update eliza's full instruction string The error occurred due to an unexpected keyword argument bot_instructions in the update_bot_instructions function. It appears that the function does not expect this parameter as provided.
x (test) Make slack active thing only apply to new bot creation, dont block activation of existing bots
x (test) MR - Share default bot images thru app
x (test) add Spider baseball and f1 schemas as share-thru app w/pre-made harvest results for demo data on start
x (test) added new message when no access to data, e.g. when a customer recrates their data daily
x (test) more logging for the slack not showing files thing for chris and robert
x test task update and delete with the new confirmation steps
x test the new thread message notice change in dm and not dm (from breakpoint)
x Fix update bot instructions, and test add tools and add files for the str get nonetype error
x Task system - when needs help, send clarity comments to the user who made the task in a DM
x task server log to sis app, add log calls for it to the start/stop info tab examples
x reword button page on refresh to press this button after first step of bot config
x Add update message back to slack thread if tools are still running for more than a minute or if the run is still thinking.. (update her Thinking message)
x add a 60 second delay on task server startup
x (added message) add another last step to bot deploy that tells you what to do once you have done the link 
x When activating a bot to pay attention to a new thread, include the original message starting the thread, and the last n messages 
x Add to the thing that checks whether to respond to a thread, see if the same bot was the original poster (from a task for example) and if so respons
x (havent seen) Do our services / pools suspend after 3600 sec, and auto restart?  Is restart clean? 
x good enough - JL-(soon) fix wait spinner on api key page when putting in API key on a new install
x on first message in a thread, briefly say what you can do
x threads adds- have past context messages
x when added to a thread that was started based on a task trigger, mention that when the app rejoins
x have the thread adds know that they are the trigger that
x make sure express harvest embeddings get replaced with real ones in runner once available
x add task server as a server to native app and deploy for testing
x (did express harvest instead) Give the bots info on the harvest status, in case they cant find something (a list of tables being harvested, etc.)
x Tell Eliza about how access works , GRANT TO APPLICATION instead GRANT TO PUBLIC
x Chris Jones information schema and data access 
x Tell Eliza to convey specific error messages to users instead of not 
x When a bot can't access something from run_query, have her suggest that you need to grant access to it (grant all)
n cache bot responses for some time and pre-run the intro prompts
x MR - Add bot custom welcome messages on new chats in Streamlit
x Added image generation
x Ask Eve to make a line chart and it sends back ImageFileContentBlock(image_file=ImageFile(file_id='file-kfWyFfbLNRk8R2lfnMhQwPEn'), type='image_file') which we dont handle right now        
x JL - test - after putting in openai key on new install, do you get sent to chat screen via button?
x JL- (test w/new sis) Streamlit after entry of openai key doesnt show Talk to Bots button 
x Snow Sec questionairre
x JL - fix and test API key spinner and reload button in SiS
x push new version 
x (test docker) Remove code generator
x (test) Eve deploy on fresh install complained about file types for null files, make sure deploy button works
x (test) make deploy to slack button in SiS app tell you to setup slack tokens first if not yet set up
x JL- test bots while harvester is running 
x JL- (text) FIGURE OUT slowdown of whole system when harvester runs.. make it single threaded, with delays?
x MR - If app is restarting (pools etc) have Sis give a message and spinner saying that vs a blank screen
x JL- (check for appoval) Share on East2, see if its working in the AM, then share to Chris 
x (test) MR-Add SNOWFLAKE harvest account usage etc to the pre-harvest feed into the app 
x (test) MR - Recreate services if they are missing during a START_APP_INSTANCE call
x  move annoy index 180sec check to the outer server loop vs per bot 
x Make the thinking message go away when a bot decides not to respond
x Change to Slack's new approach for file uploading
x redo vision chat analysis with new vision API and move the function
x add files list to bot configurations page in SiS
x add instructions to data granting on how to grant from a shared database
x Bot_upgrade instructions make it work
x why doesn't eliza respond when youre in a thread with her if shes not tagged?
x updating bot instructions via eve is not working
x (test/fix) Updating bot instrucrions when wrong / invialid botid provided not sending error back to Eve 
x Add llm that its using to bot_config on streamlit
x Add a semantic YAML steward tool... the AI feeds it piece by piece, asks it whats missing, and fills it in until its complete ...& make stuart bot and demo 
x Make sure deploy button before slack keys activated tells you what to do (e.g. put in slack config keys first)
x Images/docs: Add/test document retreival for documents that the AI produces (images and non images) so they show up in Slack
x (is this done?) Images/docs: Add image analysis and image production using openai vision mode, need to add as a separate tool
x (test in NA, and add a grant example w/imported privs) Test new harvester ddl on a shared database 
x  Fix vision-chat-analysis, add to available_tools, give to eliza and stuart by default
x  Figure out why chat_vision_analysis isn't seeing the files provided via slack upload 
x Have harvester not harvest the app database
x (removed endpoint calls) Change endpoints query to the framework version
x llm config page -  clarify page that you dont need to do this again
x MR - upgrades of native app, Make patch upgrade of app work in NA/SPCS
x MR - add log sharing back to provider
x (soon) add tab to see service logs for service and harvester
n Expose harvest tables to app public so user can read write backup and restore
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
x MR-add a few doublechecks before going to the initiall install screen in Sis
x MR-Pre-harvest and share information_schema
x MR-add link to support Slack workspace inviter page on SiS
x MR-Add a message to the top of the SisChat page suggesting activating via slack, via a temp workspace
x MR-harvester - change the include flag column to exclude and use that field instead of deleting a row to stop auto harvesting