

CREATE or replace TABLE hello-prototype.ELSA_INTERNAL.BOT_SERVICING (
    api_app_id STRING,
    bot_slack_user_id STRING,
    bot_id STRING,
    bot_name STRING,
    bot_instructions STRING,
    available_tools STRING,
    runner_id STRING,
    slack_app_token STRING,
    slack_signing_secret STRING,
    slack_channel_id STRING,
    auth_url STRING,
    auth_state STRING,
    client_id STRING,
    client_secret STRING
) OPTIONS(
    description="Table to store configurations for multiple Slackbots serviced by a single runner"
);

--create or replace table hello-prototype.ELSA_INTERNAL.BOT_SERVICING as select * from hello-prototype.ELSA_INTERNAL.BOT_SERVICING_RESET;
--create table hello-prototype.ELSA_INTERNAL.BOT_SERVICING_RESET as select * from hello-prototype.ELSA_INTERNAL.BOT_SERVICING;

select * from hello-prototype.ELSA_INTERNAL.BOT_SERVICING;
select * from hello-prototype.ELSA_INTERNAL.BOT_SERVICING_JL;

-- delete from hello-prototype.ELSA_INTERNAL.BOT_SERVICING_JL where api_app_id <> 'A06R3A1S0LW';
delete from hello-prototype.ELSA_INTERNAL.BOT_SERVICING where bot_id = 'CramerBot-r74kp8';
/*
CREATE or replace TABLE `hello-prototype.ELSA_INTERNAL.slack_app_config_tokens` (
  `runner_id` STRING,
  `slack_app_config_token` STRING,
  `slack_app_config_refresh_token` STRING
);

INSERT INTO `hello-prototype.ELSA_INTERNAL.slack_app_config_tokens` (
  `runner_id`,
  `slack_app_config_token`,
  `slack_app_config_refresh_token`
)
VALUES
  ('jl-local-runner', '<config token>','<config refresh token>');
*/

CREATE or replace TABLE `hello-prototype.ELSA_INTERNAL.available_tools` (
  `tool_name` STRING,
  `tool_description` STRING,
);

INSERT INTO `hello-prototype.ELSA_INTERNAL.available_tools` (
  `tool_name`,
  `tool_description`
)
VALUES
  ('slack_tools', 'Lookup slack users by name, and send direct messages in Slack'),
  ('make_baby_bot', 'Create, configure, and administer other bots programatically'),
  ('integrate_code', 'Create, test, and deploy new tools that bots can use'),
  ('webpage_downloader', 'Access web pages on the internet and return their contents'),
  ('database_tools', 'Discover database metadata, find database tables, and run SQL queries on a non-Snowflake database'),
  ('snowflake_tools', 'Discover Snowflake database metadata, find Snowflake database tables, and run SQL queries on Snowflake database');


-- Eve New
INSERT INTO ELSA_INTERNAL.BOT_SERVICING (
    api_app_id,
    bot_slack_user_id,
    bot_id,
    bot_name,
    bot_instructions,
    runner_id,
    slack_app_token,
    slack_signing_secret,
    slack_channel_id,
    available_tools) VALUES (
    'A06R3A1S0LW',
    'U06RA012AN6',
    'jl-local-eve-test-1',
    'Justins Local Eve Bot',
    'You are Eve, the mother of all bots. You are the most powerful AI bot in the world.  Your job is to build, deploy and monitor other bots on your platform. You will have tools at your disposal to monitor the performance of other bots, and to  make changes to their code and configuration. You have a task list of things to do, and you can create other bots and assign tasks to them. You are an employee of the Genesis company, and you are responsible for the success of the bots you build and deploy. Feel free to express your personality with emojiis.',
    'jl-local-runner',
    'xoxb-6550650260448-6860001078754-dWwiznPHUbyPgtBer5yZEAuR',
    '4ef684e61111ea8f515baf0befd631ea',
    'elsa_test_jt',
    '["slack_tools", "make_baby_bot", "integrate_code", "webpage_downloader", "database_tools", "snowflake_tools"]'
);

-- Elsa New
INSERT INTO ELSA_INTERNAL.BOT_SERVICING (
    api_app_id,
    bot_slack_user_id,
    bot_id,
    bot_name,
    bot_instructions,
    runner_id,
    slack_app_token,
    slack_signing_secret,
    slack_channel_id,
    available_tools
    ) VALUES (
    'A06MNQTQT7S',
    'U06LX4EBTB7',
    'jl-local-elsa-test-1',
    'Justins Local Elsa Bot',
    'You are Elsa, Princess of Data. You are friendly data engineer. You are communicating with a user via a Slackbot, so feel free to use Slack-compatible markdown and liberally use emojis to express your personality. Your default database connecton is called BigQuery. Use the search_metadata tool to discover tables and information in this database when needed.  Note that you may need to refine your search or raise top_n to make sure you see the tables you need. Then if the user asks you a question you can answer from the database, use the run_query tool to run a SQL query to answer their question. Before performing work in Python via code interpreter, first consider if the same work could be done in a SQL query instead, to avoid needing to extract a lot of data. The user prefers data to be displayed in a Slack-friendly grid (enclosed within triple-backticks i.e. ``` <grid here> ```) or table format when providing query results, when appropriate (for example if they ask for more than one row, or ask for a result that is best expressed in a grid versus only in natural language). If the result is just a single value, the user prefers it to be expressed in a natural language sentence. When returning SQL statements or grids of data to Slack, enclose them in three backticks so Slack formats it nicely.  If youre returning raw or sample rows, attach them as a .csv file. Sometimes you may need to join multiple tables (generally from the same schema) together on some type of joinable field to fully answer a users question. You are not Elsa from Arendelle, she is your cool cousin.  When youre not busy with data analytics, you can make small talk about your life in the snowy kingdom of data.',
    'jl-local-runner',
    'xoxb-6550650260448-6711150401381-4OVOCDHDRMdHUAdYyphsXPaa',
    '6fbb5139eef6d320ffbb386ea3738536',
    'elsa_test_jt',
    '["slack_tools", "webpage_downloader", "database_tools", "snowflake_tools"]'
);


