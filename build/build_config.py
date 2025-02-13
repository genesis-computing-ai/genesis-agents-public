from glob import glob
import os
from functools import lru_cache


# File patterns that should remain as Python source (.py) and not cythonized
PUBLIC_API_FILES = (
    'genesis_bots/api/*.py',
    'genesis_bots/api/README.md',
    'genesis_bots/api/LICENSE',
    'genesis_bots/apps/**/*.py',  # Keep all Python files under apps as .py files
    'genesis_bots/genesis_sample_golden/**/*', # these files should remain as is as they are copied from the genesis_bots package when user runs `genesis setup`
)


@lru_cache(maxsize=None)
def _expand_glob_patterns(root_dir, patterns: tuple[str]) -> set[str]:
    expanded_patterns = set()
    prev_cwd = os.getcwd()
    os.chdir(root_dir)
    for pattern in patterns:
        expanded_patterns.update(glob(pattern, recursive=True))
    os.chdir(prev_cwd)
    return expanded_patterns


def is_public_api_file(root_dir, file_path):
    """
    Check if a given file path is part of the public API files.

    Args:
        root_dir (str): The root directory to resolve the file paths (typically the root of the repository, or CWD)
        file_path (str): The file path relative to the root directory (e.g. 'genesis_bots/api/genesis_api.py')

    Returns:
        bool: True if the file path is part of the public API files, False otherwise.
    """
    return file_path in _expand_glob_patterns(root_dir, PUBLIC_API_FILES)


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
    'genesis_bots/teams',
}

# Individual files to ignore
IGNORE_FILES = {
    'genesis_bots/teams/app.py',
}