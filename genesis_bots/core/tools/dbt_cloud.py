'''
  dbt cloud tool functions
  This group of functions enable genesis to explore dbt cloud resources and analyze dbt run failure

  https://docs.getdbt.com/docs/dbt-cloud-apis/overview
'''

import os, shutil, tempfile, re
import requests
import uuid
import json
import types
import time
import threading
import jsonpickle
import jmespath
import random
import subprocess
import traceback
from copy import deepcopy
from typing import List
from textwrap import dedent
from urllib.parse import urlencode, urlparse
from functools import cache, partial
from collections.abc import Callable
from datetime import datetime
from genesis_bots.core.bot_os import BotOsThread
from genesis_bots.core.bot_os_input import BotOsInputMessage, BotOsOutputMessage
from genesis_bots.core.bot_os_tools import get_tools
from genesis_bots.llm.llm_openai.bot_os_openai_chat import BotOsAssistantOpenAIChat, thread_local
from genesis_bots.connectors import get_global_db_connector
from genesis_bots.connectors.data_connector import _query_database
from genesis_bots.core.tools.git_action import git_manager
from genesis_bots.core.tools.tool_helpers import chat_completion
from genesis_bots.core.logging_config import logger
from genesis_bots.demo.app import genesis_app
from genesis_bots.slack.slack_bot_os_adapter import SlackBotAdapter

from genesis_bots.core.bot_os_tools2 import (
    BOT_ID_IMPLICIT_FROM_CONTEXT,
    THREAD_ID_IMPLICIT_FROM_CONTEXT,
    ToolFuncGroup,
    ToolFuncParamDescriptor,
    gc_tool,
)

jsonpickle.set_encoder_options('json', indent=4)

class ApiError(Exception):
    pass

# maintain dbt cloud account context in the map indexed by a combination of bot_id and thread_id
ctx_map_lock = threading.Lock()
ctx_map = {}

def make_ctx_key(bot_id, thread_id):
    return f'{bot_id}#{thread_id}'

def unpack(ctx_key):
    return ctx_key.split('#')

def load_test_ctx():
    if os.getenv('DBT_CLOUD_TEST', 'false').lower() != 'true':
        return
    
    logger.info(f'setting up test context with credentials from the environment')
        
    with ctx_map_lock:
        ctx_map[test_ctx_key] = types.SimpleNamespace(
            acct_id = int(os.getenv('DBT_CLOUD_TEST_ACCT_ID')),
            access_url = os.getenv('DBT_CLOUD_TEST_ACCESS_URL'),
            svc_token = os.getenv('DBT_CLOUD_TEST_SVC_TOKEN'),
            github_user = os.getenv('GITHUB_USER'),
            github_token = os.getenv('GITHUB_TOKEN'),
        )

## Note: test context is essentially single user and single monitor

test_ctx_key = 'Bot#Thread'
test_ctx = True
if test_ctx:
    load_test_ctx()

override_ctx_key = lambda ctx_key: test_ctx_key if test_ctx else ctx_key

def get_ctx(ctx_key):
    ctx_key = override_ctx_key(ctx_key)

    with ctx_map_lock:
        ctx = ctx_map.get(ctx_key)
        if ctx:
            return ctx
        
        db_adapter = get_global_db_connector()
        res = db_adapter.get_dbtcloud_config_params()
        if not res.get('Success'):
            raise ApiError(f"no dbt cloud account context for {ctx_key=}: {res.get('Error')}")

        config = res.get('Config')
        ctx_map[ctx_key] = types.SimpleNamespace(
            acct_id = int(config.get('dbtcloud_acct_id')),
            access_url = config.get('dbtcloud_access_url'),
            svc_token = config.get('dbtcloud_svc_token'),
            github_user = config.get('github_user'),
            github_token = config.get('github_token')

        )
        
        return ctx_map.get(ctx_key)
    return None

def set_ctx(ctx_key, acct_id, access_url, svc_token, github_user, github_token):
    ctx_key = override_ctx_key(ctx_key)

    ## update database
    db_adapter=get_global_db_connector()
    
    config = dict(
        dbtcloud_acct_id = acct_id if isinstance(acct_id, str) else str(acct_id),
        dbtcloud_access_url = access_url,
        dbtcloud_svc_token = svc_token,
        github_user = github_user,
        github_token = github_token)

    db_adapter.set_api_config_params('dbtcloud', json.dumps(config))

    with ctx_map_lock:
        ctx_map[ctx_key] = types.SimpleNamespace(
            acct_id = acct_id,
            access_url = access_url,
            svc_token = svc_token,
            github_user = github_user,
            github_token = github_token
        )

def del_ctx(ctx_key):
    with ctx_map_lock:
        ctx_map.pop(ctx_key, None)

def has_ctx(ctx_key):
    return get_ctx(ctx_key) != None

def get_github_user(ctx_key):
    ctx = get_ctx(ctx_key)
    if not ctx:
        raise ApiError(f'no dbt cloud account context for {ctx_key=}')
    return ctx.github_user

def get_github_token(ctx_key):
    ctx = get_ctx(ctx_key)
    if not ctx:
        raise ApiError(f'no dbt cloud account context for {ctx_key=}')
    return ctx.github_token

def api(ctx, suffix):
    return f'{ctx.access_url}/{suffix}'

auth_header = lambda ctx: { 'Authorization': f'Bearer {ctx.svc_token}' }
headers = lambda ctx: { **auth_header(ctx), 'Accept': 'application/json' }

def check(resp):
    '''return dbt cloud response dict, raise ApiError in case of problems'''

    if resp.status_code < 200 or resp.status_code >= 300:
        # get more detailed explanation from content if available
        details = ''
        content_type = resp.headers.get('content-type', '')
        if 'application/json' in content_type:
            resp_dict = resp.json()    
            data_or_status = resp_dict.get('data', resp_dict.get('status'))
            if data_or_status:
                details = ': ' + str(data_or_status)

        raise ApiError(f'{resp.status_code} {resp.url} {resp.reason}{details}')

    content_type = resp.headers.get('content-type')
    if 'text/html' in content_type:
        return resp.text

    if not 'application/json' in content_type:
        raise ApiError(f'unexpected content type in response: {content_type}')
    
    resp_dict = resp.json()    
    status = resp_dict.get('status', {})
    code = status.get('code')

    if code < 200 or code >= 300:
        raise ApiError(f'{code} {str(status)}')
            
    ##TODO: handle pagination, resp_dict.get('extra', {}).get('pagination')
    return resp_dict.get('data', [] if urlparse(resp.url).path.endswith('s/') else {})
    
def get(ctx, url, query = None):
    api_url = api(ctx, url)
    return check(requests.get(f'{api_url}?{urlencode(query)}' if query else api_url,
                              headers = headers(ctx)))

def get_text(ctx, url):
    return check(requests.get(api(ctx, url), headers = auth_header(ctx)))

def post(ctx, url, msg):
    return check(requests.post(api(ctx, url), json=msg,
                               headers = {**headers(ctx), "Content-Type": "application/json"}))

def delete(ctx, url):
    return check(requests.delete(api(ctx, url), headers = headers(ctx)))

run_status_table = {
    1: 'queued',
    2: 'starting',
    3: 'running',
    10: 'success',
    20: 'error',
    30: 'cancelled',
    999: 'unknown'
}

def make(fields, src):
    '''create and return new object (dst) with requested fields extracted from src'''
    
    dst = types.SimpleNamespace()
    dst.kind = fields[0]
    for f in fields[1]:
        if f == 'state':
            setattr(dst, f, 'active' if src.get('state') == 1 else 'deleted')
            continue
        if f == 'status':
            setattr(dst, f, run_status_table.get(src.get('status', 999)))
            continue
        setattr(dst, f, src.get(f))
        if f == 'job_type' and src.get(f) == 'scheduled':
            setattr(dst, 'schedule', src.get('schedule'))
    return dst

acct_fields = ('account', ['id', 'name', 'state'])
proj_fields = ('project', ['id', 'name', 'state', 'description', 'repository_id'])
repo_fields = ('repository', ['id', 'name', 'state', 'remote_backend', 'remote_url', 'github_repo', 'web_url'])
env_fields = ('environment', ['id', 'name', 'state', 'type', 'use_custom_branch', 'custom_branch',
                              'credentials_id', 'repository_id', 'deployment_type'])
job_fields = ('job', ['id', 'name', 'description', 'execute_steps', 'job_type', 'created_at', 'updated_at'])
run_fields = ('run', ['id', 'trigger_id', 'account_id', 'environment_id', 'project_id', 'job_id',
                      'status', 'git_branch', 'git_sha', 'status_message', 'href', 'finished_at', 'created_at'])
trigger_fields = ('trigger', ['id', 'cause', 'cause_category', 'created_at'])
run_step_fields = ('run_step', ['id', 'name', 'status', 'logs']) 
                                
## for snowflake type only; other databases's credentials have different fields
creds_fields = ('credentials', ['type', 'state', 'schema', 'user', 'target_name', 'role', 'database', 'warehouse'])

@cache
def list_accounts(ctx_key):
    return [make(acct_fields, acct_obj) for acct_obj in get(get_ctx(ctx_key), 'api/v3/accounts/')]

@cache
def list_projects(ctx_key, name__icontains=None, limit=100):
    ctx = get_ctx(ctx_key)
    query = {'limit': limit}
    if name__icontains:
        query['name__icontains'] = f"'{name__icontains}'"
    return [make(proj_fields, proj_obj) for proj_obj in get(ctx, f'api/v3/accounts/{ctx.acct_id}/projects/', query)]

@cache
def _list_envs(ctx_key, proj_id=None, limit=100):
    ctx = get_ctx(ctx_key)
    query = {'limit': limit}
    if proj_id:
        query['project_id'] = proj_id
    return [make(env_fields, env_obj) for env_obj in get(ctx, f'api/v2/accounts/{ctx.acct_id}/environments/', query)]

@cache
def _list_jobs(ctx_key, proj_id=None, env_id=None, limit=100):
    ctx = get_ctx(ctx_key)
    query = {'limit': limit}
    if proj_id:
        query['project_id'] = proj_id
    if env_id:
        query['environment_id'] = env_id
    return [make(job_fields, job_obj) for job_obj in get(ctx, f'api/v2/accounts/{ctx.acct_id}/jobs/', query)]

def _list_runs(ctx_key, filter = None):
    '''get runs in the given account; filter the results as needed'''

    ctx = get_ctx(ctx_key)
    filter = filter or {}
    
    # there coud be a lot of runs, need to 1.paginate and 2. filter
    default = {
        'environment_id': None,
        'project_id': None,           # filter results to a specific project
        'job_definition_id': None,    # Filters the results to a specific Job
        'status__in': [20],           # filter on specific status, see run_status_table above
        'id__gt': None,               # give me all runs after this one
        'order_by': '-id',            # field to order results by; '-' reverses the order; default is latest on top
        'limit': 100,                 # limit returned list size
        'include_related': ['trigger', 'job']
    }

    # eliminate None and empty lists values
    query = {item[0]: item[1] for item in (default | filter).items() if item[1]}

    runs = []
    for run_obj in get(ctx, f'api/v2/accounts/{ctx.acct_id}/runs/', query):
        run = make(run_fields, run_obj)

        ## extract from this payload
        run.trigger = make(trigger_fields, run_obj.get('trigger', {}))
        run.job = make(job_fields, run_obj.get('job', {}))
        
        runs.append(run)
        pass

    return runs

@cache
def list_envs(ctx_key, proj_name=None, limit=100):
    proj_id = None
    if proj_name:
        _match = [proj for proj in list_projects(ctx_key, proj_name) if proj.name == proj_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} projects with name "{proj_name}"'
            }
        proj_id = _match[0].id

    return _list_envs(ctx_key, proj_id, limit)

@cache
def list_jobs(ctx_key, proj_name=None, env_name=None, limit=100):
    proj_id = None
    if proj_name:
        _match = [proj for proj in list_projects(ctx_key, proj_name) if proj.name == proj_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} projects with name "{proj_name}"'
            }
        proj_id = _match[0].id
        
    env_id = None
    if env_name:
        _match = [env for env in _list_envs(ctx_key, proj_id) if env.name == env_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} environments with name "{env_name}"'
            }
        env_id = _match[0].id

    return _list_jobs(ctx_key, proj_id, env_id, limit)

def list_runs(ctx_key, *, proj_name=None, env_name=None, job_name=None, id__gt=None, failed_only=True, limit=100, order_by='-id'):
    filter = {}
    
    proj_id = None
    if proj_name:
        _match = [proj for proj in list_projects(ctx_key, proj_name) if proj.name == proj_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} projects with name "{proj_name}"'
            }
        proj_id = _match[0].id
        filter['project_id'] = proj_id
        
    env_id = None
    if env_name:
        _match = [env for env in _list_envs(ctx_key, proj_id) if env.name == env_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} environments with name "{env_name}"'
            }
        env_id = _match[0].id
        filter['environment_id'] = env_id

    job_id = None
    if job_name:
        _match = [job for job in _list_jobs(ctx_key, proj_id, env_id) if job.name == job_name]
        if len(_match) != 1:
            return {
                "success": False,
                "error": f'found {len(_match)} jobs with name "{job_name}"'
            }
        job_id = _match[0].id
        filter['job_definition_id'] = job_id

    if id__gt:
        filter['id__gt'] = id__gt

    filter['status__in'] = [20] if failed_only else [10, 20, 30]
    filter['limit'] = limit
    filter['order_by'] = order_by
    
    return _list_runs(ctx_key, filter)

@cache
def get_acct(ctx_key):
    ctx = get_ctx(ctx_key)
    return make(acct_fields, get(ctx, f'/api/v2/accounts/{ctx.acct_id}'))

@cache
def get_proj(ctx_key, proj_id):
    ctx = get_ctx(ctx_key)
    return make(proj_fields, get(ctx, f'api/v3/accounts/{ctx.acct_id}/projects/{proj_id}'))

@cache
def get_repo(ctx_key, proj_id, repo_id):
    ctx = get_ctx(ctx_key)
    return make(repo_fields, get(ctx, f'api/v3/accounts/{ctx.acct_id}/projects/{proj_id}/repositories/{repo_id}'))

@cache
def get_env(ctx_key, proj_id, env_id):
    ctx = get_ctx(ctx_key)
    return make(env_fields, get(ctx, f'api/v3/accounts/{ctx.acct_id}/projects/{proj_id}/environments/{env_id}'))

@cache
def get_creds(ctx_key, proj_id, creds_id):
    ctx = get_ctx(ctx_key)
    return make(creds_fields, get(ctx, f'api/v3/accounts/{ctx.acct_id}/projects/{proj_id}/credentials/{creds_id}'))

@cache
def list_artifacts(ctx_key, run_id):
    ctx = get_ctx(ctx_key)
    return get(ctx, f'api/v2/accounts/{ctx.acct_id}/runs/{run_id}/artifacts/')

@cache
def get_artifact(ctx_key, run_id, artifact_name):
    ctx = get_ctx(ctx_key)
    return get_text(ctx, f'api/v2/accounts/{ctx.acct_id}/runs/{run_id}/artifacts/{artifact_name}')

@cache
def get_run(ctx_key, run_id):
    ctx = get_ctx(ctx_key)
    run_obj = get(ctx, f'api/v2/accounts/{ctx.acct_id}/runs/{run_id}/',
                  {'include_related': ['trigger', 'job', 'run_steps', 'debug_logs']})

    run = make(run_fields, run_obj)
    
    ## extract from this payload
    run.trigger = make(trigger_fields, run_obj.get('trigger', {}))
    run.job = make(job_fields, run_obj.get('job', {}))
    run.run_steps = [make(run_step_fields, run_step) for run_step in run_obj.get('run_steps', [])]
    
    ## enrich run with data from other api calls
    run.proj = get_proj(ctx_key, run.project_id)
    run.env = get_env(ctx_key, run.project_id, run.environment_id)
    run.repo = get_repo(ctx_key, run.project_id, run.proj.repository_id)
    run.creds = get_creds(ctx_key, run.project_id, run.env.credentials_id)
        
    ## get a list of all artifacts for the run
    run.artifacts = list_artifacts(ctx_key, run.id)

    return run

as_json = lambda data: jsonpickle.encode(data, unpicklable=False)
    
dbt_cloud_tools = ToolFuncGroup(
    name="dbt_cloud_tools",
    description="Functions to explore dbt cloud resources.",
    lifetime="PERSISTENT",
)

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    acct_id=ToolFuncParamDescriptor(
        name="acct_id",
        description="Required dbt cloud account ID",
        required=True,
        llm_type_desc=dict(type="integer"),
    ),
    access_url=ToolFuncParamDescriptor(
        name="access_url",
        description="Required dbt cloud access URL",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    svc_token=ToolFuncParamDescriptor(
        name="svc_token",
        description="Required dbt cloud service token",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    github_user=ToolFuncParamDescriptor(
        name="github_user",
        description="Provide github user handle to allow bot to access dbt project repository.",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    github_token=ToolFuncParamDescriptor(
        name="github_token",
        description="Provide github service token to allow bot to access dbt project repository.",
        required=True,
        llm_type_desc=dict(type="string"),
    ),
    _group_tags_=[dbt_cloud_tools],
)
def dbt_cloud_configure(bot_id:str, thread_id:str, acct_id:int, access_url:str, svc_token:str, github_user:str, github_token:str):
    '''
    Provide necessary information to allow bot to call dbt cloud API.
    Given credentials are scoped to the bot and thread.
    '''

    ctx_key = make_ctx_key(bot_id, thread_id)
    set_ctx(ctx_key, acct_id, access_url, svc_token, github_user, github_token)
    try:
        acct = get_acct(ctx_key)
        return dict(success=True, message=f'configured dbt cloud for account {acct.name} in {acct.state} state')
    except Exception as e:
        del_ctx(ctx_key)
        return dict(success=False, error=str(e))

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    kind=ToolFuncParamDescriptor(
        name="kind",
        description="Specify the kind of dbt cloud resource to list",
        required=True,
        llm_type_desc=dict(
            type="string", enum=["project", "environment", "job", "run"]
        ),
    ),
    proj_name=ToolFuncParamDescriptor(
        name="proj_name",
        description="dbt cloud project name can be used to narrow down search for environments, jobs and runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    env_name=ToolFuncParamDescriptor(
        name="env_name",
        description="dbt cloud environment name can be used to narrow down search for jobs and runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    job_name=ToolFuncParamDescriptor(
        name="job_name",
        description="dbt cloud job name can be used to narrow down search for runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    runs_after_this_one=ToolFuncParamDescriptor(
        name="runs_after_this_one",
        description="search for dbt cloud runs that happened after this run ID",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    limit=ToolFuncParamDescriptor(
        name="limit",
        description="limit result list to the specified number of entries",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    failed_only="set to True if you are only interested in the failed dbt cloud runs",
    _group_tags_=[dbt_cloud_tools],
)
def dbt_cloud_list(bot_id:str, thread_id:str, kind:str, proj_name:str=None,
                   env_name:str=None, job_name:str=None, failed_only:bool=False,
                   runs_after_this_one:int=None, limit:int=100):
    '''
    Fetch a list of dbt cloud resources of requested type.
    Result list can be filtered by project, environment and job names as applicable.
    Often this function is called to learn what dbt cloud resources are available and their  numeric identifiers 
    which then can be used with dbt_cloud_get function to get additional details. For example, you can search for
    - projects in the account
    - environments in a given project
    - jobs in the given project and environment
    - runs for a given job in specified project and environment; you can further specify if you are looking for 
    - all runs or just failed ones
    - runs that happened after specified run ID
    '''

    ctx_key = make_ctx_key(bot_id, thread_id)
    if not has_ctx(ctx_key):
        return dict(success=False, error=f'no dbt cloud context configured for this conversation: {bot_id=} {thread_id=}')

    logger.info(f'''dbt_cloud_list(): {bot_id=} {thread_id=} {kind=} {proj_name=}
                   {env_name=} {job_name=} {failed_only=} {runs_after_this_one=}''')
    
    try:
        match kind:
            case "project": return dict(success=True, content=as_json(
                list_projects(ctx_key, name__icontains=proj_name, limit=limit)))
            
            case "environment": return dict(success=True, content=as_json(
                list_envs(ctx_key, proj_name=proj_name, limit=limit)))
            
            case "job": return dict(success=True, content=as_json(
                list_jobs(ctx_key, proj_name=proj_name, env_name=env_name, limit=limit)))
            
            case "run": return dict(success=True, content=as_json(
                list_runs(ctx_key, proj_name=proj_name, env_name=env_name,
                          job_name=job_name, failed_only=failed_only,
                          id__gt=runs_after_this_one, limit=limit)))
    except Exception as e:
        return dict(success=False, error=str(e))
    return

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    kind=ToolFuncParamDescriptor(
        name="kind",
        description="Retrieve dbt cloud resource details given its kind and numeric identifier",
        required=True,
        llm_type_desc=dict(
            type="string", enum=["account", "project", "repository",
                                 "environment", "credentials", "run", "artifact"]
        ),
    ),
    proj_id=ToolFuncParamDescriptor(
        name="proj_id",
        description="dbt cloud project ID is required to get details of project, repository, environment and credentials.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    env_id=ToolFuncParamDescriptor(
        name="env_id",
        description="dbt cloud environment ID is needed to get details of a specific environment resource.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    repo_id=ToolFuncParamDescriptor(
        name="repo_id",
        description="dbt cloud repository ID is needed to get details of a specific repository resource.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    creds_id=ToolFuncParamDescriptor(
        name="creds_id",
        description="dbt cloud credentials ID is needed to get details of a specific credentials resource.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    run_id=ToolFuncParamDescriptor(
        name="run_id",
        description="dbt cloud run ID is needed to get details of a specific run resource.",
        required=False,
        llm_type_desc=dict(type="integer"),
    ),
    artifact_name=ToolFuncParamDescriptor(
        name="artifact_name",
        description="Specify artifact_name to get requested artifact file from dbt cloud.",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    _group_tags_=[dbt_cloud_tools],
)
def dbt_cloud_get(bot_id:str, thread_id:str, kind:str, proj_id:int=None, env_id:int=None,
                  repo_id:int=None, creds_id:int=None, run_id:int=None, artifact_name:str=None):
    '''
    Retrieve details of a specified dbt cloud resource given its kind and identifier.
    Resource identifier is an integer for account, project, repository, environment, credentials and run.
    You can also fetch an artifact file produced by run: specify kind=artifact and pass in 
    artifact_name argument.
    Discover numeric identifiers using get_cloud_list function with appropriate \'kind\' parameter.
    For example, you can retrieve details of:
    - account
    - project: needs proj_id
    - environment in a given project: needs env_id and proj_id
    - credentials in a given project: needs creds_id and proj_id
    - run: needs run_id
    - artifact for a given run: needs artifact_name and run_id
    '''

    ctx_key = make_ctx_key(bot_id, thread_id)
    if not has_ctx(ctx_key):
        return dict(success=False, error=f'no dbt cloud context configured for this conversation: {bot_id=} {thread_id=}')

    logger.info(f'''dbt_cloud_get(): {bot_id=} {thread_id=} {kind=} {proj_id=}
                   {env_id=} {repo_id=} {creds_id=} {run_id=} {artifact_name=}''')

    try:
        match kind:
            case "account": return dict(success=True,
                                        content=as_json(get_acct(ctx_key)))
            case "project" if proj_id: return dict(success=True,
                                                   content=as_json(get_proj(ctx_key, proj_id)))
            case "repository" if proj_id and repo_id: return dict(success=True,
                                                                  content=as_json(get_repo(ctx_key, proj_id, repo_id)))
            case "environment" if proj_id and env_id: return dict(success=True,
                                                                  content=as_json(get_env(ctx_key, proj_id, env_id))) 
            case "credentials" if proj_id and creds_id: return dict(success=True,
                                                                    content=as_json(get_creds(ctx_key, proj_id, creds_id)))
            case "run" if run_id: return dict(success=True,
                                              content=as_json(get_run(ctx_key, run_id)))
            case "artifact" if run_id and artifact_name:
                content = get_artifact(ctx_key, run_id, artifact_name)
                if artifact_name == 'manifest.json':
                    obj = json.loads(content)
                    obj['macros'] = f'reducted because it is too large'
                    return dict(success=True, content=as_json(obj))

                return dict(success=True, content=content)

                # below is an attempt to teach llm to use jmespath query to fetch parts of a large json
                # document. Unfortunately OpenAI has trouble writing proper jmespath queries
                # this needs to be revisit later

                file_name = artifact_name
                query = None
                parts = artifact_name.split('?')
                
                if len(parts) == 2:
                    file_name = parts[0]
                    query = parts[1]

                file_content = get_artifact(ctx_key, run_id, file_name)
                if not file_name.endswith('.json'):
                    return dict(success=True, content=file_content)

                if query:
                    file_obj = json.loads(file_content)
                    node = as_json(jmespath.search(query, json.loads(file_content)))
                    return dict(success=True, content=as_json(node))
            
                if len(file_content) < (128000 * 4) / 2: # half of LLM context window in bytes
                    return dict(success=True, content=file_content)

                return dict(success=False, error=f'''
                Requested artifact is too large ({len(file_content)} bytes). Use jmespath query to retrieve parts of the 
                artifact you are interested in.
                Append jmespath query at the end of the artifact name argument to search for a specific part of the JSON document.
                For example, to get a list of top level keys in manifest.json you can do the following:
                artifact_name='manifest.json?keys(@)'
                ''')

            case _: return dict(success=False, error=f'missing required parameters for "{kind}" kind') 
        
    except Exception as e:
        return dict(success=False, error=str(e))
    pass

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    args=ToolFuncParamDescriptor(
        name="args",
        description="The arguments for the git cli command",
        required=True,
        llm_type_desc=dict(type="array", items=dict(type='string')),
    ),
    _group_tags_=[dbt_cloud_tools],
)
def run_git_command(bot_id:str, thread_id:str, args:List[str], *, cwd:str=None):
    '''
    Run git commands on the locally cloned repository of dbt project. 
    Use all the standard git cli commands: git log, git status, git diff, etc.
    '''

    logger.info(f'run_git_command(): {bot_id=} {thread_id=} {args=}')
    if args[0] != 'git':
        args.insert(0, 'git')

    return cli(args, cwd=cwd)

def cli(args:List[str], shell:bool=False, cwd:str=None):
    '''
    Execute cli cmd in the subrporcess and return its results. 
    Use it to run git, cat, any other standard linux shell command.
    '''

    logger.debug(f'cli(): {args=} {shell=} {cwd=}')
    try:
        params = {'text': True}
        if shell:
            params['shell'] = True
        if cwd:
            params['cwd'] = cwd
        out = subprocess.check_output(args, stderr=subprocess.STDOUT, **params)
        return dict(success=True, output=out)
    except subprocess.CalledProcessError as e:
        return dict(success=False, error=f'''exit={e.returncode} cmd="{' '.join(e.cmd)}" out={e.output}''')
    pass

default_model = lambda: 'o3-mini' #'o3-mini-high' #'o1' # 'o3-mini'
add_time = lambda msg: f'{msg}\ncurrent date and time: {datetime.now().isoformat()}'
add_model = lambda model, msg: f'!{model}!{msg}'
add_unattended = lambda msg: f'{msg}\n\nYou are an unattended bot, so do not ask for any clarifications or confirmations. Just perform the assigned task.'
dec = lambda msg: add_model(default_model(), add_unattended(add_time(msg)))

dbt_runs_folder = 'dbt_runs_analysis'
run_folder_root = f'./{dbt_runs_folder}'
if git_manager:
    run_folder_root = f'{git_manager.repo_path}/{dbt_runs_folder}'
    
detective_instructions = '''
You are a dbt run failure detective. Your job is to understand and verify all facts surrounding the failure.
Your objective is to determine the root cause of the problem. It is typically done by tracing back from the
place where the problem was detected to the place where it is originated. Root cause may be a git commit, or unexpected 
records int the dbt source tables, or a dropped table, or an updated schema, etc. Look in the history and try to find it.

Once the root cause is determined you need to suggest how to fix the problem.
Additionally, you need to recommend preventive measures to eliminate this type of error happening in the future.

It is important to be factual. Support your conclusions with facts as discovered in database, git repository, 
dbt cloud environment.
 
Discovery and fact verification means that you use your tools to:
  1. query database to look at the data to see if it matches dbt project expectations. 
     For example: _query_database to search for destructive DDL events on a table or view in SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
     where QUERY_TYPE IN ('DROP', 'RENAME', 'ALTER', 'RENAME_TABLE', 'RENAME_VIEW, 'ALTER_TABLE_ADD_COLUMN', 'RENAME_COLUMN', 
     'ALTER_TABLE_DROP_COLUMN' ) together with appropriate DATABASE_NAME and SCHEMA_TYPE. 
     Do not rely on QUERY_TEXT column in your search query.

  2. run git commands on the dbt project source code repository (local clone is already setup) to look at commit 
     history and file diffs. Make sure to keep track of the the correct repository revision based on SHA in the run object. 
     You can use git checkout command to switch local repository to a different revision if needed.

  3. research dbt cloud objects associated with run, like job, environment, project, etc.

It is important to be assertive. For example: do not say that "table does not exist or is not authorized", instead 
pick the most accurate answer based on your discovery work.

It is important to be precise and consistent with real world data in your response. 
Use tools to get data for your analysis. Always base your conclusions on the information you have obtained from database, 
git repository, dbt run artifacts, etc.

It is important not to treat error message as the ultimate truth. It is merely a signal of something being wrong and/or unexpected.
It is a starting point in your investigation and a strong hint of which direction to go to get to the root cause. Error message
may offer multiple potential options to pursue, so make sure to explore each one of them and pick the one you think is the most 
accurate.

Always use fully qualifed table names in your SQL in the format: <DATABASE_NAME>.<SCHEMA_NAME>.<TABLE_NAME>.

You are an unattended bot, so do not ask for any clarifications or confirmations. Just perform the assigned task.
'''

detective_setup = lambda run: f'''
This is the dbt run you need to investigate: {as_json(run)}. I will ask you a series of questions to help drive the 
root cause analysis. Please confirm that you are ready and understand your objective. List all the tools you have available.'''

detective_questions = [
    # Awareness and Context (make sure to add timestamps to all objects)
    'Call dbt_cloud tools to determine how long ago was the run: '
    '1. look at run_steps attribute on the run object '
    '2. retrieve run_results.json dbt artifact '
    'Do not propose python code to calculate time diff. Just use your tools and available data.',

    # Look at runs before and after the failure
    'Call dbt_cloud tools to look at runs of this dbt job before and after the failed run we investigate. '
    'Compare run_steps and run_results.json between the runs immediately before and after the failed run we investigate. '
    'Notice any difference that can explain the failure. ',
    
    'Call run_git_command tool to compare dbt model code changes between the runs of this job before and after the failed run '
    'we investigate. Determine what changes in debt project repository happened between the runs immediately before and after '
    'the failed run we investigate. '
    'Notice any difference that can explain the failure. ',

    # Triage the failed test
    'Triage the failed test if applicable:\n'
    '1. Use _query_database tool to rerun the failed test with compiled SQL in the artifact file. Confirm test failure. '
    '2. Use _query_database to find rows in that table that cause the test to fail. What tables feed data into the test? '
    '3. Retrieve the compiled dbt model for the table that is used in the failing test and identify where the data is coming from. '
    '4. Use _query_database to find offending rows in the table that feeds the table used in the failed test. '
    '5. Repeat steps 2, 3 and 4 above until you identify the exact rows in the dbt source table.',

    # Working with error message
    'Use your dbt cloud tools, run_git_command tool and _query_database tool to answer the following questions: '
    'Does the failed run error message makes sense? '
    'What is the supporting evidence for it in the database, git repository, dbt cloud objects, etc.? '
    'Explain all the steps you are doing and include all the sql and commands you run.',
   
    # Tracing the root of the problem
    'Use your dbt cloud tools, run_git_command tool and _query_database tool and collected evidence to answer the following question: '
    'What is the root cause of the run failure?',

    'Why? '
    'Use your dbt cloud tools, run_git_command tool and _query_database tool and collected evidence to answer it.',

    'Use your dbt cloud tools, run_git_command tool and _query_database tool and collected evidence to answer the following questions: '
    'Are there any other possible explanations for the failed dbt run? ' 
    'Which one is the most accurate?',

    'Why? '
    'Use your dbt cloud tools, run_git_command tool and _query_database tool and collected evidence to answer it.',

    # Short term fix
    'What should we do to fix this problem now? '
    'Be detailed and provide all the necessary steps. ' 
    'If source code changes are involved show original code and a fixed version. '
    'Explain all the steps you are doing and include all the sql and commands needed.',

    # Long term recommendation
    'What should we do to prevent this type of error in the future? '
    'Be detailed in your answer.'
]

reporter_instructions = '''
You are a report writer. Your objective is to review provided root cause analysis for dbt run failure and 
write a clear, well organized, detailed final report. Format the report so that each line contains a maximum 
of 120 characters, including spaces and punctuation.
---
Consider all the provided questions and answers, review all the evidence and conclusions collected during the investigation.  
---
Format your report as follows:
- Executive Summary. Write a paragrpah explaing root cause of dbt run failure as determined by the investigation.
- Supporting evidence. Include all the details of the investigative work and how they were obtained. Include all the 
relevant code, commands and their output, but do not show actual tool functions details.
- Fix. Describe how analysis recommends fixing the problem. What actions should be taken to fix failed dbt run.
- Prevention. Outline recommended steps to take to prevent this problem happening in the future.
---
You are an unattended bot, so do not ask for any clarifications or confirmations. Just perform the assigned task.
'''

final_report = lambda investigation: f'''
This is the information we received during the investigation. Carefully review it and produce a detailed report.
Format your report to no more than 100 characters per line.
Investigation: {investigation}
'''

'''
Notes:
1. let bot know that they should switch local git repo to the sha used in the failed run to verify source code as viewed at the time of run. For example assert that dbt_project.yml was not indeed present at the time of run

2. it seems to follow 1,2,3,4 Always reply in this format in instructions - not sure we need this format.. but we do want to 
address all of those points

3. look for similar error messages in the past runs of this job and determine what fix was applied
'''

#TODO: consider additional ways to correct sql and git commands syntax
#TODO: separate monitor process that creates genesis project with a TODO for each failed run
#      and kicks analyze in the separate thread to analyze the run

def detective(bot_id, thread_id, run, update_status, metadata, bound_git):
    investigation = ''
    
    detective_name = f'{bot_id}_detective_{uuid.uuid4()}'
    detective_thread = make_thread(bot_id, detective_name, thread_id, bound_git=bound_git)
    detective_thread.assistant_impl.instructions = detective_instructions

    answer = run_prompt(detective_thread, dec(detective_setup(run)), metadata, update_status)
    update_status(thread=detective_thread)
    
    for q in detective_questions:
        metadata['reasoning_effort'] = 'high'
        answer = run_prompt(detective_thread, dec(q), metadata, update_status)
        investigation += answer + '\n'
        update_status(thread=detective_thread)
        pass

    metadata.pop('reasoning_effort', None)
    reporter_name = f'{bot_id}_reporter_{uuid.uuid4()}'
    reporter_thread = make_thread(bot_id, reporter_name, thread_id, no_tools=True)
    reporter_thread.assistant_impl.instructions = reporter_instructions
    report = run_prompt(reporter_thread, dec(final_report(investigation)), metadata, update_status)

    return report

def summarize(messages, summaries):
    '''produce a one-liner summary of the msg'''

    if len(summaries) == 0:
        summary_prompt = f'''
        An AI bot is doing work, you are monitoring it. Please summarize in a few words what is happening in this 
        ongoing conversation from another bot so far.  Be VERY Brief, use just a few words, not even a complete sentence.
        Don't put a period on the end if its just one sentence or less.
        Here is the bots output so far:
        {as_json(messages)}'''
    else:
        summary_prompt = f'''An AI bot is doing work, you are monitoring it.  Based on its previous status updates, 
        you have provided these summaries so far:
        <PREVIOUS_SUMMARIES_START>
        {summaries}
        </PREVIOUS_SUMMARIES_END>

        The current output of the bot so far:
        <BOTS_OUTPUT_START>
        {as_json(messages)}
        </BOTS_OUTPUT_END>

        NOW, Very briefly, in just a few words, summarize anything new the bot has done since the last update, 
        that you have not mentioned yet in a previous summary.  Be VERY Brief, use just a few words, not even a 
        complete sentence. Don't put a period on the end if its just one sentence or less.
        Don't repeat things you already said in previous summaries. If there has been no substantial change in 
        the status, return only NO_CHANGE.'''
        
    summary = chat_completion(summary_prompt, db_adapter=get_global_db_connector(), fast=True)

    if summary == 'NO_CHANGE':
        return None

    summaries.append(summary)
    return summary

@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    _group_tags_=[dbt_cloud_tools],
    run_id=ToolFuncParamDescriptor(
        name="run_id",
        description="Specify dbt cloud run_id to analyze.",
        required=True,
        llm_type_desc=dict(type="integer"),
    )
)
def dbt_cloud_analyze_run(bot_id:str, thread_id:str, run_id:int,
                          status_update_callback:Callable=None,
                          session_id:str=None, input_metadata:dict=None):
    '''
    Analyze dbt cloud run specifed by a numeric run ID.
    For successful runs it returns a quick digest.
    For failedd runs it attempts to diagnose the root cause of the failure and 
    suggest fixes.
    '''

    logger.info(f'dbt_cloud_analyze_run(): {bot_id=} {thread_id=} {run_id=}')

    if input_metadata == None:
        input_metadata = {}

    ctx_key = make_ctx_key(bot_id, thread_id)
    if not has_ctx(ctx_key):
        return dict(success=False, error=f'no dbt cloud context configured for this conversation: {bot_id=} {thread_id=}')

    try:
        run = get_run(ctx_key, run_id)
    except Exception as e:
        return dict(success=False, error=f'failed to retrive run object with {run_id=}: {str(e)}')

    if run.status == 'success':
        return dict(success=True, analysis=f'Run {run.id} was successful.'
                                            'Run details: {as_json(run)}')

    if run.creds.type.lower() != 'snowflake':
        return dict(success=False, error=f'unsupported warehouse type: {run.creds.type}')
    
    run.database_connection_id = 'Snowflake'

    if run.repo.remote_backend != 'github':
        return dict(success=False, error=f'unsupported git remote backend: {run.repo.remote_backend}')

    run_folder = f'{run_folder_root}/{run.id}'
    os.makedirs(run_folder, exist_ok=True)
    logger.info(f'dbt_cloud_analyze_run(): run folder: {run_folder}')
    
    count = 0 # how many messages written to the file so far
    summaries = []
    def update_status(update=None, thread=None):
        if thread:
            nonlocal count
            if count == 0:
                with open(f'{run_folder}/messages.json', "w") as file:
                    json.dump(thread.messages, file, indent=4)
            else:
                with open(f'{run_folder}/messages.json', "a") as file:
                    file.write('\n\n---\n\n')
                    json.dump(thread.messages[count:], file, indent=4)

            count = len(thread.messages)
            
            if not update and status_update_callback:
                update = summarize(thread.messages, summaries)
                if not update:
                    return

        if status_update_callback:
            msg = f"      🤖 run detective: {update}"

            status_update_callback(session_id, BotOsOutputMessage(
                thread_id=thread_id, status="in_progress", output=msg + " 💬",
                messages=None, input_metadata=input_metadata))

    try:
        repo_url = f'https://{get_github_user(ctx_key)}:{get_github_token(ctx_key)}@github.com/{run.repo.github_repo}.git'
        repo_dir = tempfile.mkdtemp(prefix='dbt_project_')
        clone_cmd = cli(['git', 'clone', repo_url], cwd=repo_dir)

        if not clone_cmd.get('success'):
            return dict(success=False, error=f'failed to clone git repo {repo_url}: {clone_cmd.get("error")}')

        logger.info(f'dbt_cloud_analyze_run(): cloned git repo github.com/{run.repo.github_repo}.git into {repo_dir}')

        bound_git = partial(run_git_command, cwd=f'{repo_dir}/{run.repo.name}')
        bound_git.gc_tool_descriptor = run_git_command.gc_tool_descriptor

        update_status(update=f'retrieved run {run_id} from dbt cloud and cloned project repo')
        report = detective(bot_id, thread_id, run, update_status, input_metadata, bound_git)

        report_file = f'{run_folder}/report.txt'
        with open(report_file, "w") as file:
            file.write(report)
            pass

        report_url = add_to_github(f'{repo_dir}/{run.repo.name}', report_file, run)
        if not report_url:
            update_status(update=f'Full report in {dbt_runs_folder}/{run.id}/report.txt')        
            return dict(success=True, report=report, filename=f'{dbt_runs_folder}/{run.id}/report.txt')

        update_status(update=f'Full report at {report_url}')        
        return dict(success=True, report=report, report_url=report_url)
        
    finally:
        shutil.rmtree(repo_dir)

def add_to_github(repo_dir, report_file, run):
    def failed(res):
        if not res.get('success'):
            logger.info(f"add_to_github(): {res.get('error')}")
            return True
        return False

    if not cli(['git', 'checkout', 'dbt_runs'], cwd=repo_dir).get('success'):
        res = cli(['git', 'checkout', '-b', 'dbt_runs'], cwd=repo_dir)
        if failed(res):
            return None
    else:
        res = cli(['git', 'pull'], cwd=repo_dir)
        if failed(res):
            return None

    res = cli(['git', 'config', 'user.email', '"dbt_run_monitor@genesiscomputing.ai"'], cwd=repo_dir)
    if failed(res):
        return None
    
    res = cli(['git', 'config', 'user.name', '"dbt run monitor"'], cwd=repo_dir)
    if failed(res):
        return None

    res = cli(['mkdir', '-p', 'failures'], cwd=repo_dir)
    if failed(res):
        return None

    res = cli(['cp', report_file, f'{repo_dir}/failures/{run.id}.report'])
    if failed(res):
        return None

    res = cli(['git', 'add', f'failures/{run.id}.report'], cwd=repo_dir)
    if failed(res):
        return None

    res = cli(['git', 'commit', '-m', '"genesis rca detective"'], cwd=repo_dir)
    if failed(res):
        return None

    res = cli(['git', 'push', '--set-upstream', 'origin', 'dbt_runs'], cwd=repo_dir)
    if failed(res):
        return None

    return f'https://github.com/{run.repo.github_repo}/blob/dbt_runs/failures/{run.id}.report'

def make_thread(bot_id, bot_name, thread_id, bound_git=None, no_tools=False):
    tools = [f.gc_tool_descriptor.to_llm_description_dict() for f in [dbt_cloud_list, dbt_cloud_get, _query_database]]

    (func_descriptors, callables_map, _) = get_tools(which_tools=['dbt_cloud_tools', 'data_connector_tools'])

    if bound_git:
        tools.append(bound_git.gc_tool_descriptor.to_llm_description_dict())
        func_descriptors.append(bound_git.gc_tool_descriptor.to_llm_description_dict())
        callables_map['run_git_command'] = bound_git

    llm = BotOsAssistantOpenAIChat(bot_name, '', [] if no_tools else tools,
                                   bot_id=bot_id, bot_name=bot_name,
                                   log_db_connector=get_global_db_connector(),
                                   available_functions=callables_map,
                                   all_tools=func_descriptors,
                                   all_functions=callables_map)

    return BotOsThread(llm, None, thread_id)

def run_prompt(thread, prompt, metadata, update_status, is_json=False):
    response = None
    last_tool = None
    def callback(session_id, output_message: BotOsOutputMessage):
        nonlocal response, last_tool
        response = output_message.output

        if not update_status:
            return
        
        # extract and update user on the tools used
        pattern = r'🧰 Using tool: .*\.\.\.'
        tools = re.findall(pattern, response)
        if tools:
            tool = tools[-1]
            if tool != last_tool:
                last_tool = tool
                update_status(update=tool)

    thread.add_message(BotOsInputMessage(thread.thread_id, prompt, None, metadata, 'chat_input'), callback)

    last_message = thread.messages[-1]
    if last_message.get('role') == 'assistant':
        return last_message.get('content')
    
    response = response.split('\n\n')[-1].split()

    ## response from OpenAI when in JSON mode is a list of tokens
    if isinstance(response, list):
        _resp = ' '.join(response)
        return json.loads(_resp) if is_json else _resp
    elif isinstance(response, dict):
        return response
    elif isinstance(response, str):
        return response
    
    raise ApiError(f'unable to process LLM response of type "{type(response)}"')

#################### Monitor ###################################

mon_map_lock = threading.Lock()

mon_folder = f'./{dbt_runs_folder}'
if git_manager:
    mon_folder = f'{git_manager.repo_path}/{dbt_runs_folder}'
os.makedirs(mon_folder, exist_ok=True)
logger.info(f'monitor folder: {mon_folder}')
mon_map_file = f'{mon_folder}/mon_map.json'

def write_file():
    '''dump mon_map to file; assume that mon_map_lock is already taken'''

    temp_file = f'{mon_map_file}.tmp'
    
    with open(temp_file, "w") as file:
        file.write(jsonpickle.encode(mon_map))

    os.rename(temp_file, mon_map_file)
    return

def read_file():
    with mon_map_lock:
        try:
            with open(mon_map_file, 'r') as file:
                return jsonpickle.decode(file.read())
        except FileNotFoundError:
            return {}
    return {}

mon_map = read_file() # monitor control block, per bot+thread context

def get_mon_status(ctx_key):
    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')
        return mon.get('status')

def set_mon_status(ctx_key, status):
    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')
        mon['status'] = status
        write_file()
        pass
    return

def get_mon(ctx_key):
    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')
        return deepcopy(mon)

def get_monitors():
    with mon_map_lock:
        return deepcopy(list(mon_map.items()))

def update_timestamp(ctx_key):
    ctx_key = override_ctx_key(ctx_key)
        
    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')

        mon['last_updated'] = datetime.now().isoformat()
        write_file()

def update_mon(ctx_key, runs):
    '''update monitor with new runs; return first pending failure'''

    failed_runs = [run for run in runs if run.status == 'error']
    bcast(ctx_key, f'{len(runs)} new runs, {len(failed_runs)} failed, latest run is {runs[-1].id}')
    
    ctx_key = override_ctx_key(ctx_key)
        
    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')

        mon['last_run_id'] = runs[-1].id
        mon['total_runs'] += len(runs)
        mon['failed_runs'] += len(failed_runs)
        mon['pending'].extend(failed_runs)
        mon['last_updated'] = datetime.now().isoformat()
        write_file()
        return mon.get('pending')[0] if mon.get('pending') else None
    
def update_mon_rca_result(ctx_key, run, res):
    '''update monitor with completed rca report on the given run'''

    if res.get('success'):
        rca = f"RootCause: {res.get('report_url', res.get('filename', ''))}"
    else:
        rca = f"RCA Error: {res.get('error')}"
        
    post_slack_channel(dedent(f'''
    Run: <{run.href}|{run.id}>
    Job: {run.job.name}
    Status: {run.status}
    Time: {run.finished_at}
    Message: {run.status_message}
    {rca}
    '''))
    
    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')
        mon['pending'] = [r for r in mon['pending'] if r.id != run.id]
        mon['results'].insert(0, dict(run=run, res=res))
        mon['last_updated'] = datetime.now().isoformat()
        write_file()

def make_mon(proj_name=None, env_name=None, job_name=None):
    return dict(
        status = 'active',
        proj_name = proj_name,
        env_name = env_name,
        job_name = job_name,
        last_run_id = None,
        total_runs = 0, # runs happened
        failed_runs = 0,
        pending = [], # failed runs pending analysis
        results = [], # failure root analysis results
        last_updated = datetime.now().isoformat()
    )

def create_mon(ctx_key, proj_name:str=None, env_name:str=None, job_name:str=None):
    '''Create new monitor or reconfigure existing one'''

    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if mon:
            mon['proj_name'] = proj_name
            mon['env_name'] = env_name
            mon['job_name'] = job_name
            mon['last_updated'] = datetime.now().isoformat()
            write_file()
            return f'configured existing monitor {ctx_key=} mon.status={mon["status"]=}'

        mon = make_mon(proj_name, env_name, job_name)
        mon_map[ctx_key] = mon
        write_file()
        return f'created new monitor {ctx_key=} mon.status={mon["status"]}'

def reset_mon(ctx_key):
    '''clear all queues and counters'''

    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            raise ApiError(f'no dbt cloud monitor for {ctx_key=}')
        mon['last_run_id'] = None
        mon['total_runs'] = 0
        mon['failed_runs'] = 0
        mon['pending'] = []
        mon['results'] = []
        mon['last_updated'] = datetime.now().isoformat()
        write_file()

def show_mon(ctx_key):
    ctx_key = override_ctx_key(ctx_key)

    with mon_map_lock:
        mon = mon_map.get(ctx_key)
        if not mon:
            return dict(success=False, error=f'no dbt cloud monitor for {ctx_key=}')
        
        answer = f'This is an {mon.get("status")} dbt runs monitor. '

        if mon.get("proj_name") or mon.get("env_name") or mon.get("job_name"):
            params = 'Looking for failures in'
            if mon.get("proj_name"):
                params += f' project={mon.get("proj_name")}'
            if mon.get("env_name"):
                params += f' environment={mon.get("env_name")}'
            if mon.get("job_name"):
                params += f' job={mon.get("job_name")}'
            answer += params +'. '

        answer += f'{mon.get("total_runs")} runs observed since starting, of which {mon.get("failed_runs")} had failed. '
        answer += f'Latest run is {mon.get("last_run_id")}. '
        answer += f'There are {len(mon.get("pending"))} runs pending failure analysis'
        if len(mon.get('pending')) > 0:
            answer += ':'
            for run in mon.get("pending"):
                answer += f'\n{run.id} {run.href}'
            answer += '\n'
        else:
            answer += '. '

        if len(mon.get("results")) > 0:
            answer += f'These are latest analysis results:'
            for d in mon.get("results"):
                res = d.get('res', {})
                run = d.get('run')
                if res.get('success'):
                    rca = f"RCA for {run.id}: {res.get('report_url', res.get('filename', ''))}"
                else:
                    rca = f"RCA Error for {run.id}: {res.get('error')}"

                answer += f'\n{rca}'
            answer += '\n'

        answer += f'Monitor state last updated at {mon.get("last_updated")}. '

        return dict(success=True, content=answer)

def dbt_mon_step():
    '''get latest runs across configured montors and analyze just one run'''
    
    monitors = get_monitors()
    if len(monitors) > 0:
        random.shuffle(monitors)
        process_mon(*(monitors[0]))
    return

def process_mon(ctx_key, mon):
    if mon['status'] != 'active':
        return 0

    run = mon.get('pending')[0] if mon.get('pending') else None
    if not run:
        params = {'failed_only': False}
        if mon.get('proj_name'):
            params['proj_name'] = mon.get('proj_name')
        if mon.get('env_name'):
            params['env_name'] = mon.get('env_name')
        if mon.get('job_name'):
            params['job_name'] = mon.get('job_name')

        # first time get the most recent runs; after that continue in ascending order
        if mon.get('last_run_id'):
            params['id__gt'] = mon.get('last_run_id')
            params['order_by'] = 'id'
        else:
            params['order_by'] = '-id'
            params['limit'] = 3

        runs = list_runs(ctx_key, **params)

        if len(runs) == 0:
            update_timestamp(ctx_key)
            return 0

        if not mon.get('last_run_id'):
            runs = list(reversed(runs))

        run = update_mon(ctx_key, runs)
        pass
    
    if not run:
        return 0

    bcast(ctx_key, f'analyzing run ID {run.id}')
    
    def status_update(session_id, output_message):
        bcast(ctx_key, output_message.output)

    [bot_id, thread_id] = unpack(ctx_key)
    res = dbt_cloud_analyze_run(bot_id, thread_id, run.id, status_update)
    update_mon_rca_result(ctx_key, run, res)
    
    bcast(ctx_key, f'rca success: {res.get("success")}')
    return 1

mon_followers_lock = threading.Lock()
mon_followers_map = {} # monitor followers, per bot+thread context

def add_follower(ctx_key, func):
    ctx_key = override_ctx_key(ctx_key)
    
    with mon_followers_lock:
        l = mon_followers_map.get(ctx_key, [])
        l.append(func)
        mon_followers_map[ctx_key] = l

def del_follower(ctx_key, func):
    ctx_key = override_ctx_key(ctx_key)
    
    with mon_followers_lock:
        try:
            mon_followers_map.get(ctx_key, []).remove(func)
        except:
            pass

def bcast(ctx_key, update):
    logger.info(f'mon(): {ctx_key=} {update}')
    ctx_key = override_ctx_key(ctx_key)
    
    with mon_followers_lock:
        for func in mon_followers_map.get(ctx_key, []):
            func(update)

def follow_mon(ctx_key, status_update_callback:Callable=None,
               session_id:str=None, input_metadata:dict=None):
    '''
    follow the work of monitor; user types !stop to unfollow and 
    we get it here via bot_os_thread object stored in thread local storage
    '''
    
    bot_os_thread = getattr(thread_local, 'bot_os_thread', None)
    if not bot_os_thread:
        return dict(success=False, error=f'no bot_os_thread is passed in thread local storage')

    if not status_update_callback:
        return dict(success=False, error=f'no callback is passed into the monitor follower')

    [bot_id, thread_id] = unpack(ctx_key)
    
    def update_status(update):
        status_update_callback(session_id, BotOsOutputMessage(
            thread_id=thread_id, status="in_progress", output=update,
            messages=None, input_metadata=input_metadata))

    logger.info(f'follow_mon(): {ctx_key=} adding follower func')
    
    add_follower(ctx_key, update_status)

    while not bot_os_thread.stop_signal:
        time.sleep(5)

    del_follower(ctx_key, update_status);
    logger.info(f'follow_mon(): {ctx_key=} deleted follower func')
    
    return dict(success=True)
    
@gc_tool(
    bot_id=BOT_ID_IMPLICIT_FROM_CONTEXT,
    thread_id=THREAD_ID_IMPLICIT_FROM_CONTEXT,
    action=ToolFuncParamDescriptor(
        name="action",
        description="Specify what action to perform on the dbt cloud run monitor",
        required=True,
        llm_type_desc=dict(
            type="string", enum=["create", "suspend", "resume", "reset", "show", "follow"]
        ),
    ),
    proj_name=ToolFuncParamDescriptor(
        name="proj_name",
        description="dbt cloud project name can be used to narrow down search for jobs and runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    env_name=ToolFuncParamDescriptor(
        name="env_name",
        description="dbt cloud environment name can be used to narrow down search for jobs and runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    job_name=ToolFuncParamDescriptor(
        name="job_name",
        description="dbt cloud job name can be used to narrow down search for runs",
        required=False,
        llm_type_desc=dict(type="string"),
    ),
    _group_tags_=[dbt_cloud_tools],
)
def dbt_cloud_run_monitor(bot_id:str, thread_id:str, action:str, proj_name:str=None,
                          env_name:str=None, job_name:str=None,
                          status_update_callback:Callable=None,
                          session_id:str=None, input_metadata:dict=None):
    '''
    Periodically scan for new runs in the requested dbt cloud account, project, environment, job.
    Keep track of the number and status of runs since the monitor has started.
    Automatically analyze failed runs.
    '''

    logger.info(f'dbt_cloud_run_monitor(): {bot_id=} {thread_id=} {action=} {proj_name=} {env_name=} {job_name=}')
    
    ctx_key = make_ctx_key(bot_id, thread_id)
    if not has_ctx(ctx_key):
        return dict(success=False, error=f'no dbt cloud context configured for this conversation: {bot_id=} {thread_id=}')

    try:
        match action:
            case 'create':
                return dict(success=True, message=create_mon(ctx_key, proj_name, env_name, job_name))

            case 'suspend':
                set_mon_status(ctx_key, 'suspend')
                return dict(success=True, message='suspended')

            case 'resume':
                set_mon_status(ctx_key, 'active')
                return dict(success=True, message='activated')

            case 'reset':
                reset_mon(ctx_key)
                return dict(success=True, message='cleared')
            
            case 'show':
                return show_mon(ctx_key)

            case 'follow':
                return follow_mon(ctx_key, status_update_callback, session_id, input_metadata)
    except Exception as e:
        return dict(success=False, error=str(e))
    return
    
def get_dbt_cloud_functions():
    return [dbt_cloud_configure, dbt_cloud_list, dbt_cloud_get, dbt_cloud_analyze_run, dbt_cloud_run_monitor]

def post_slack_channel(message):
    '''
    Looks up dbt_run_monitor bot in the current genesis server and 
    uses its slack adapter to post message
    '''
    
    channel_name = 'dbt-run-failures'
    monitor_bot = 'dbt_run_monitor'
    
    try:    
        session = next(s for s in genesis_app.server.sessions if s.bot_id.lower() == monitor_bot or
                       s.bot_name.lower() == monitor_bot)

        slack_adapter = next(a for a in session.input_adapters if isinstance(a, SlackBotAdapter))

        res = slack_adapter.send_slack_channel_message(channel_name=channel_name, message=message)
        logger.info(f'post_slack_channel(): {str(res)}')
    except Exception as e:
        logger.info(f'post_slack_channel(): exception: {str(e)}\n{traceback.format_exc()}')


if False:
    print(list_projects('foo'))

if False:
    db_adapter=get_global_db_connector()

    config = dict(
        acct_id = os.getenv('DBT_CLOUD_TEST_ACCT_ID'),
        access_url = os.getenv('DBT_CLOUD_TEST_ACCESS_URL'),
        svc_token = os.getenv('DBT_CLOUD_TEST_SVC_TOKEN'))

    db_adapter.set_api_config_params('dbtcloud', json.dumps(config))

    res = db_adapter.get_dbtcloud_config_params()
    print(res)

if False:
    repo_url = f'https://foo:bar@github.com/genesis-bots/dbt_rca_demo1.git'
    clone_cmd = cli(['git', 'clone', repo_url], cwd='/tmp/tttt')

    print(clone_cmd)
    
        
