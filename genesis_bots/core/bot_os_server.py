from   apscheduler.events       import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from   apscheduler.schedulers.background \
                                import BackgroundScheduler
import datetime
from   flask                    import Flask
from   genesis_bots.bot_genesis.make_baby_bot \
                                import (get_slack_config_tokens,
                                        rotate_slack_token)
from   genesis_bots.core.bot_os import BotOsSession
import os
import sys
import traceback

from   genesis_bots.demo.sessions_creator \
                                import make_session

from   genesis_bots.bot_genesis.make_baby_bot \
                                import get_bot_details, make_baby_bot

from   genesis_bots.core.logging_config \
                                import logger


def _job_listener(event):

    if event.exception:
        logger.error(f"job crashed: {event.job_id}")
    else:
       # logger.debug(f"job executed successfully: {event.job_id}")
       pass

def telemetry_heartbeat():
    summary = logger.telemetry_logs
    logger.telemetry("add_heartbeat::", summary['messages'], summary['prompt_tokens'], summary['completion_tokens'])
    logger.reset_telemetry()

class BotOsServer:

    run_count = 0
    cycle_count = 0
    stream_mode = False

    def __init__(
        self,
        flask_app: Flask|None, # not tested - flagges as not supported
        sessions: list[BotOsSession],
        scheduler: BackgroundScheduler,
        scheduler_seconds_interval=2,
        slack_active=False,
        db_adapter=None,
        bot_id_to_udf_adapter_map = None,
        api_app_id_to_session_map = None,
        bot_id_to_slack_adapter_map = None,
    ):
        logger.debug(f"BotOsServer:__init__ creating server")
        assert flask_app is None, "Flask app is currently assumed to be extrnal to the BotOsServer" # TODO : refactor flask app into the BotOsServer or remove this attribute
        self.app = flask_app
        self.sessions = sessions
        self.scheduler = scheduler
        self.db_adapter = db_adapter
        self.bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map
        self.api_app_id_to_session_map = api_app_id_to_session_map
        self.bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map

        for session in self.sessions:
            if hasattr(session, 'tool_belt'):
                session.tool_belt.set_server(self)

        existing_job = self.scheduler.get_job("bots")
        if existing_job:
            self.scheduler.remove_job("bots")
        self.job = self.scheduler.add_job(
            self._execute_session,
            "interval",
            coalesce=True,
            seconds=scheduler_seconds_interval,
            id="bots",
            name="test",
        )
        self.scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self.scheduler.add_job(telemetry_heartbeat, 'interval', seconds=360)
        self.slack_active = slack_active
        self.last_slack_token_rotate_time = datetime.datetime.now()
        self.last_dbconnection_refresh = datetime.datetime.now()

        # rotate tokens once on startup
        if self.slack_active:
            t, r = get_slack_config_tokens()
            tok, ref = rotate_slack_token(t, r)
            if tok is not None and ref is not None:
                logger.info(
                    f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}"
                )
            else:
                logger.info('Slack refresh token failed, token is None')


    def make_baby_bot_wrapper(self, bot_id, bot_name, bot_implementation, files, available_tools, bot_instructions):

        # Handle string representation of list
        if isinstance(available_tools, str) and available_tools.startswith('['):
            # Remove brackets and quotes, then split
            available_tools = available_tools.strip('[]').replace('"', '').replace("'", '').split(',')

        if isinstance(available_tools, list):
            available_tools = ','.join(tool.strip() for tool in available_tools)

        bot_details = get_bot_details(bot_id)
        update_existing = True if bot_details else False


        retval = make_baby_bot(
                bot_id=bot_id,
                bot_name=bot_name,
                bot_implementation=bot_implementation,
                files=files,
                available_tools=available_tools,
                bot_instructions=bot_instructions,
                confirmed='CONFIRMED',
                update_existing=update_existing,
                api_bot_update=update_existing,
                api_mode=True
            )
        if not retval['success']:
            raise ValueError(retval.get('error', "Error unknown"))

        bot_config = get_bot_details(bot_id)

        from genesis_bots.demo.app import genesis_app

        orig_session = any(session.bot_id == bot_id for session in genesis_app.sessions)

        if orig_session:
            logger.info(f"Session for bot {bot_id} already exists, will be replaced")

        genesis_app.create_app_sessions(bot_list=[bot_config], targetted=True)

        new_session = None
        if genesis_app.sessions:  # Check if sessions exists and is not None
            matching_sessions = [session for session in genesis_app.sessions if session.bot_id == bot_id]
            new_session = matching_sessions[0] if matching_sessions else None
        # Check if session exists in server_sessions and add if not

        self.add_session(new_session, replace_existing=True)

        if new_session:
            logger.info(f"Session for bot {bot_id} created")
            return new_session
        else:
            logger.info(f"Session for bot {bot_id} not created")
            return False



    def add_session(self, session: BotOsSession, replace_existing=False):
        logger.info("At add_Session, replace_existing is ", replace_existing)
        if replace_existing:
            # Attempt to remove sessions with the same name as the new session
            try:
                logger.info(f"add_session: current sessions {[s.session_name for s in self.sessions]}")
                logger.info(f"add_session: session to remove: {session.session_name}")
                self.sessions = [
                    s for s in self.sessions if s.session_name != session.session_name
                ]
            except Exception as e:
                logger.info("add_session exception ", e)
                if self.sessions:
                    logger.info("sessions ", self.sessions)
                else:
                    logger.info("no sessions")
        # Append the new session regardless of whether a matching session was found and removed
        if session != None:
            logger.info("Adding session ", session)
        else:
            logger.info("Session is None")

        if session is not None:
            # Set server reference in the new session's toolbelt
            if hasattr(session, 'tool_belt'):
                session.tool_belt.set_server(self)

        self.sessions.append(session)

    def remove_session(self, session):

        self.sessions = [s for s in self.sessions if s != session]
        logger.info(f"Session {session} has been removed.")

    def _rotate_slack_tokens(self):
        t, r = get_slack_config_tokens()
        tok, ref = rotate_slack_token(t, r)

        # TODO REMOVE THE OTHER ROTATER CALL
        # Print a confirmation message with the current time

        if tok is not None and ref is not None:
            logger.info(f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}")
        else:
            logger.info('Slack token refreshed failed, None result.')


    def get_running_instances(self):
        executor = self.scheduler._lookup_executor("default")
        running_jobs = executor._instances["bots"]
        return running_jobs

    def reset_session(self, bot_id, session):
        bot_config = get_bot_details(bot_id=bot_id)

        existing_udf = None
        existing_slack = None
        if session is not None:
            existing_slack = next(
                (adapter for adapter in session.input_adapters if type(adapter).__name__ == "SlackBotAdapter"),
                None
            )
            existing_udf = next(
                (adapter for adapter in session.input_adapters if type(adapter).__name__ == "UDFBotOsInputAdapter"),
                None
            )
        new_session, api_app_id, udf_local_adapter, slack_adapter_local = make_session(
            bot_config=bot_config,
            db_adapter=self.db_adapter,
            bot_id_to_udf_adapter_map=self.bot_id_to_udf_adapter_map,
            stream_mode=True,
            existing_slack=existing_slack,
            existing_udf=existing_udf
        )
        # check new_session
        if new_session is None:
            logger.info("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None and self.bot_id_to_slack_adapter_map is not None:
            self.bot_id_to_slack_adapter_map[bot_config["bot_id"]] = (
                slack_adapter_local
            )
        if udf_local_adapter is not None:
            self.bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        self.api_app_id_to_session_map[api_app_id] = new_session
        #    logger.info("about to add session ",new_session)
        self.add_session(new_session, replace_existing=True)


    def _execute_session(self):
        BotOsServer.run_count += 1
        if BotOsServer.run_count >= 60:
            BotOsServer.run_count = 0
            BotOsServer.cycle_count += 1
            insts = self.get_running_instances()
            if True or insts > 1:
                # logger.info(f"--- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} bot_os_server runners: {insts} / max 100 (cycle = {BotOsServer.cycle_count})")
                emb_size = 'Unknown'
                try:
                    emb_size = os.environ['EMBEDDING_SIZE']
                except:
                    pass
                if BotOsServer.cycle_count % 60 == 0:
                    sys.stdout.write(
                        f"--- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} bot_os_server runners: {insts} / max 100, emb_size: {emb_size} (cycle = {BotOsServer.cycle_count})\n"
                    )
                    sys.stdout.flush()
                i = 0
            # self.clear_stuck_jobs(self.scheduler)
            if insts >= 90:
                logger.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                logger.info(f"-!! Scheduler worker INSTANCES >= 90 at {insts} ... Clearing All Instances")
                logger.info("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-")
                # Shut down the scheduler and terminate all jobs
                # self.scheduler.shutdown(wait=False)
                self.scheduler.remove_all_jobs()
                self.job = self.scheduler.add_job(
                    self._execute_session,
                    "interval",
                    coalesce=True,
                    seconds=1,
                    id="bots",
                )
                logger.info( "Scheduler restarted the job. All existing instances have been terminated." )

                # Restart the scheduler
                # self.scheduler.start()
                logger.info("Scheduler has been restarted.")
                insts = self.get_running_instances()
                logger.info(f"-=-=- Scheduler instances: {insts} / 100")
        if BotOsSession.clear_access_cache == True:
            for s in self.sessions:
                s.assistant_impl.user_allow_cache = {}
            BotOsSession.clear_access_cache = False
        for s in self.sessions:
            try:
                # import threading
                # logger.info(f"Thread ID: {threading.get_ident()} - starting execute cycle...")
                if os.getenv(f'RESET_BOT_SESSION_{s.bot_id}', 'False') == 'True':
                    logger.info(f"Resetting bot session for bot_id: {s.bot_id}")
                    os.environ[f'RESET_BOT_SESSION_{s.bot_id}'] = 'False'
                    self.reset_session(s.bot_id,s)
                else:
                    s.execute()
                # logger.info(f"Thread ID: {threading.get_ident()} - ending execute cycle...")
            except Exception as e:
                traceback.print_exc()

        # Check if its time for Slack token totation, every 6 hours
        if (
            self.slack_active
            and (
                datetime.datetime.now() - self.last_slack_token_rotate_time
            ).total_seconds()
            > 21600
        ):
            self.last_slack_token_rotate_time = datetime.datetime.now()
            self._rotate_slack_tokens()


    def run(self, *args, **kwargs):
        assert False, "Flask app is currently assumed to be extrnal to the BotOsServer" # TODO : refactor flask app into the BotOsServer or remove this attribute
        if self.app is not None:
            # Start the Flask application
            self.app.run(*args, **kwargs)

    def shutdown(self):
        self.scheduler.shutdown(wait=False)
