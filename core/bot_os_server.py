import traceback, datetime
from flask import Flask
from core.bot_os import BotOsSession
import threading
import os
from connectors.snowflake_connector import SnowflakeConnector
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from bot_genesis.make_baby_bot import get_slack_config_tokens, rotate_slack_token
from apscheduler.job import Job
from concurrent.futures import ThreadPoolExecutor
from apscheduler.executors.pool import ThreadPoolExecutor
import time, sys
import threading

from demo.sessions_creator import create_sessions, make_session

from bot_genesis.make_baby_bot import (  get_bot_details ) 

import logging

logger = logging.getLogger(__name__)


def _job_listener(event):

    if event.exception:
        logger.error(f"job crashed: {event.job_id}")
    else:
        logger.info(f"job executed successfully: {event.job_id}")


class BotOsServer:

    run_count = 0
    cycle_count = 0
    stream_mode = False

    def __init__(
        self,
        flask_app: Flask,
        sessions: list[BotOsSession],
        scheduler: BackgroundScheduler,
        scheduler_seconds_interval=2,
        slack_active=False,
        db_adapter=None,
        bot_id_to_udf_adapter_map = None, 
        api_app_id_to_session_map = None,
        data_cubes_ingress_url = None,
        bot_id_to_slack_adapter_map = None,
    ):
        logger.debug(f"BotOsServer:__init__ creating server {flask_app.name}")
        self.app = flask_app
        self.sessions = sessions
        self.scheduler = scheduler
        self.db_adapter = db_adapter
        self.bot_id_to_udf_adapter_map = bot_id_to_udf_adapter_map
        self.api_app_id_to_session_map = api_app_id_to_session_map
        self.data_cubes_ingress_url = data_cubes_ingress_url
        self.bot_id_to_slack_adapter_map = bot_id_to_slack_adapter_map

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
        self.slack_active = slack_active
        self.last_slack_token_rotate_time = datetime.datetime.now()
        self.last_dbconnection_refresh = datetime.datetime.now()

        # rotate tokens once on startup
        if self.slack_active:
            t, r = get_slack_config_tokens()
            tok, ref = rotate_slack_token(t, r)
            if tok is not None and ref is not None:
                print(
                    f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}"
                )
            else:
                print('Slack refresh token failed, token is None')

    def add_session(self, session: BotOsSession, replace_existing=False):
        print("At add_Session, replace_existing is ", replace_existing)
        if replace_existing:
            # Attempt to remove sessions with the same name as the new session
            try:
                print(self.sessions)
                print(session.session_name)
                self.sessions = [
                    s for s in self.sessions if s.session_name != session.session_name
                ]
            except Exception as e:
                print("add_session exception ", e)
                if self.sessions:
                    print("sessions ", self.sessions)
                else:
                    print("no sessions")
        # Append the new session regardless of whether a matching session was found and removed
        if session != None:
            print("Adding session ", session)
        else:
            print("Session is None")
        self.sessions.append(session)

    def remove_session(self, session):

        self.sessions = [s for s in self.sessions if s != session]
        print(f"Session {session} has been removed.")

    def _rotate_slack_tokens(self):
        t, r = get_slack_config_tokens()
        tok, ref = rotate_slack_token(t, r)

        # TODO REMOVE THE OTHER ROTATER CALL
        # Print a confirmation message with the current time
       
        if tok is not None and ref is not None:
            print(f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}")
        else:
            print('Slack token refreshed failed, None result.')


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
            data_cubes_ingress_url=self.data_cubes_ingress_url,
            existing_slack=existing_slack,
            existing_udf=existing_udf
        )
        # check new_session
        if new_session is None:
            print("new_session is none")
            return "Error: Not Installed new session is none"
        if slack_adapter_local is not None and self.bot_id_to_slack_adapter_map is not None:
            self.bot_id_to_slack_adapter_map[bot_config["bot_id"]] = (
                slack_adapter_local
            )
        if udf_local_adapter is not None:
            self.bot_id_to_udf_adapter_map[bot_config["bot_id"]] = udf_local_adapter
        self.api_app_id_to_session_map[api_app_id] = new_session
        #    print("about to add session ",new_session)
        self.add_session(new_session, replace_existing=True)


    def _execute_session(self):
        BotOsServer.run_count += 1
        if BotOsServer.run_count >= 60:
            BotOsServer.run_count = 0
            BotOsServer.cycle_count += 1
            insts = self.get_running_instances()
            if True or insts > 1:
                # print(f"--- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} bot_os_server runners: {insts} / max 100 (cycle = {BotOsServer.cycle_count})",flush=True)
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
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-", flush=True)
                print(
                    f"-!! Scheduler worker INSTANCES >= 90 at {insts} ... Clearing All Instances",
                    flush=True,
                )
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-", flush=True)
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
                print(
                    "Scheduler restarted the job. All existing instances have been terminated.",
                    flush=True,
                )

                # Restart the scheduler
                # self.scheduler.start()
                print("Scheduler has been restarted.", flush=True)
                insts = self.get_running_instances()
                print(f"-=-=- Scheduler instances: {insts} / 100", flush=True)
        if BotOsSession.clear_access_cache == True:
            for s in self.sessions:
                s.assistant_impl.user_allow_cache = {}
            BotOsSession.clear_access_cache = False
        for s in self.sessions:
            try:
                # import threading
                # print(f"Thread ID: {threading.get_ident()} - starting execute cycle...")
                if os.getenv(f'RESET_BOT_SESSION_{s.bot_id}', 'False') == 'True':
                    print(f"Resetting bot session for bot_id: {s.bot_id}", flush=True)
                    os.environ[f'RESET_BOT_SESSION_{s.bot_id}'] = 'False'
                    self.reset_session(s.bot_id,s)
                else:
                    s.execute()
                # print(f"Thread ID: {threading.get_ident()} - ending execute cycle...")
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
        # Start the Flask application
        self.app.run(*args, **kwargs)
