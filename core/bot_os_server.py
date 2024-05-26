import traceback, datetime
from flask import Flask
from core.bot_os import BotOsSession
import threading
from connectors.snowflake_connector import SnowflakeConnector
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from bot_genesis.make_baby_bot import get_slack_config_tokens, rotate_slack_token
from apscheduler.job import Job
from concurrent.futures import ThreadPoolExecutor
from apscheduler.executors.pool import ThreadPoolExecutor
import time, sys
import threading


import logging
logger = logging.getLogger(__name__)

def _job_listener(event):

    if event.exception:
        logger.error(f"job crashed: {event.job_id}")
    else:
        logger.info(f"job executed successfully: {event.job_id}")

class BotOsServer:

    run_count=0
    cycle_count=0
    
    def __init__(self, flask_app:Flask, sessions:list[BotOsSession], scheduler:BackgroundScheduler, 
                 scheduler_seoconds_interval=2, slack_active=False
                 ): 
        logger.debug(f"BotOsServer:__iniit__ creating server {flask_app.name}")
        self.app       = flask_app
        self.sessions   = sessions
        self.scheduler = scheduler
        
        existing_job = self.scheduler.get_job('bots')
        if existing_job:
            self.scheduler.remove_job('bots')
        self.job = self.scheduler.add_job(self._execute_session, 'interval', coalesce=True, seconds=scheduler_seoconds_interval, id='bots')
        self.scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        self.slack_active = slack_active
        self.last_slack_token_rotate_time = datetime.datetime.now()
        self.last_dbconnection_refresh = datetime.datetime.now()
        

        # rotate tokens once on startup
        if self.slack_active:
            t, r = get_slack_config_tokens()
            tok, ref = rotate_slack_token(t, r)
            print(f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}")
            print(f"     New Slack token: {tok[:8]}...{tok[-8:]}")
            print(f"     New Slack refresh token: {ref[:8]}...{ref[-8:]}")

    def add_session(self, session:BotOsSession, replace_existing=False):
        print('At add_Session, replace_existing is ',replace_existing)
        if replace_existing:
            # Attempt to remove sessions with the same name as the new session
            try:
                print(self.sessions)
                print(session.session_name)
                self.sessions = [s for s in self.sessions if s.session_name != session.session_name]
            except Exception as e:
                print('add_session exception ',e)
                if self.sessions:
                    print('sessions ',self.sessions)
                else:
                    print('no sessions')
        # Append the new session regardless of whether a matching session was found and removed
        if session != None:
            print("Adding session ",session)
        else:
            print('Session is None')
        self.sessions.append(session)

    def remove_session(self, session):
            
        self.sessions = [s for s in self.sessions if s != session]
        print(f"Session {session} has been removed.")

    def _rotate_slack_tokens(self):
        t, r = get_slack_config_tokens()
        tok, ref = rotate_slack_token(t, r)

        # TODO REMOVE THE OTHER ROTATER CALL
        # Print a confirmation message with the current time
        self.last_slack_token_rotate_time = datetime.datetime.now()
        print(f"Slack Bot Config Token REFRESHED {self.last_slack_token_rotate_time}")
        print(f"     New Slack token: {tok[:8]}...{tok[-8:]}")
        print(f"     New Slack refresh token: {ref[:8]}...{ref[-8:]}")


    def get_running_instances(self):
        executor = self.scheduler._lookup_executor('default')
        running_jobs = executor._instances['bots']
        return running_jobs

    def _execute_session(self):
 
        BotOsServer.run_count += 1
        if (BotOsServer.run_count >= 60):
            BotOsServer.run_count = 0
            BotOsServer.cycle_count += 1
            insts = self.get_running_instances()
            if True or insts > 1:
               # print(f"--- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} bot_os_server runners: {insts} / max 100 (cycle = {BotOsServer.cycle_count})",flush=True)
                sys.stdout.write(f"--- {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} bot_os_server runners: {insts} / max 100 (cycle = {BotOsServer.cycle_count})\n")
                sys.stdout.flush()
                i = 0
            #self.clear_stuck_jobs(self.scheduler)
            if insts >= 90:
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-", flush=True)
                print(f"-!! Scheduler worker INSTANCES >= 90 at {insts} ... Clearing All Instances", flush=True)
                print("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-", flush=True)
                # Shut down the scheduler and terminate all jobs
                #self.scheduler.shutdown(wait=False)
                self.scheduler.remove_all_jobs()
                self.job = self.scheduler.add_job(self._execute_session, 'interval', coalesce=True, seconds=1, id='bots')
                print("Scheduler restarted the job. All existing instances have been terminated.", flush=True)

                # Restart the scheduler
                #self.scheduler.start()
                print("Scheduler has been restarted.", flush=True)
                insts = self.get_running_instances()
                print(f"-=-=- Scheduler instances: {insts} / 100", flush=True)
        for s in self.sessions:
            try:
                #import threading
           #     print(f"Thread ID: {threading.get_ident()} - starting execute cycle...")
                s.execute()
           #     print(f"Thread ID: {threading.get_ident()} - ending execute cycle...")
            except Exception as e:
                traceback.print_exc()

        # Check if its time for Slack token totation
        if self.slack_active and (datetime.datetime.now() - self.last_slack_token_rotate_time).total_seconds() > 1800:
            self._rotate_slack_tokens()

#        if  (datetime.datetime.now() - self.last_dbconnection_refresh).total_seconds() > 60:
#            print('[TEMP MESSAGE FROM BOT_SERVER EVERY 60 SEC] Refreshing Snowflake Tokens...')
#            self.last_dbconnection_refresh = datetime.datetime.now()
#            for session in self.sessions:
#                db_connector = session.get_database_connector()
#                if isinstance(db_connector, SnowflakeConnector):
#                    db_connector.refresh_tokens()

    def run(self, *args, **kwargs):
       # Start the Flask application
        self.app.run(*args, **kwargs)
