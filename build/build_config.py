# Files that should remain as Python source
PUBLIC_API_FILES = [
    'genesis_bots/api/genesis_api.py',
    'genesis_bots/api/snowflake_remote_server.py',
    'genesis_bots/api/genesis_base.py',
    'genesis_bots/api/control.py',
]

# Directories to ignore
IGNORE_DIRS = {
    '.venv',
    '.git',
    'build',
    'bot_git',
    'backup',
    'default_files',
    'genesis_api_whl',
    'app_engine',
    'experimental',
    'tests',
    'test_services',
    '__pycache__',
    'dist',
    'egg-info',
}

# Individual files to ignore
IGNORE_FILES = {
    'genesis_bots/teams/app.py',
}

VERSION = "1.0.9" 