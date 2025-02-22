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
        if pattern.endswith('.py') or '**' in pattern:  # Only expand .py files and recursive patterns
            expanded_patterns.update(glob(pattern, recursive=True))
    os.chdir(prev_cwd)
    return expanded_patterns


def is_public_api_file(root_dir, file_path):
    """
    Check if a given file path is part of the public API files.

    Args:
        root_dir (str): The root directory to resolve the file paths
        file_path (str): The file path to check (can be absolute or relative)

    Returns:
        bool: True if the file path is part of the public API files, False otherwise.
    """
    # Normalize the file path to be relative and use forward slashes
    file_path = os.path.relpath(file_path, root_dir).replace('\\', '/')
    
    # First check exact matches in PUBLIC_API_FILES
    if file_path in PUBLIC_API_FILES:
        return True
        
    # Then check expanded glob patterns
    expanded_files = _expand_glob_patterns(root_dir, PUBLIC_API_FILES)
    if file_path in expanded_files:
        return True
        
    # Finally check glob pattern matches
    for pattern in PUBLIC_API_FILES:
        if '**' in pattern and glob.fnmatch.fnmatch(file_path, pattern):
            return True
            
    return False


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
    'genesis_bots/genesis_sample_golden/demos/genesis_spcs_ingress'
}

# Individual files to ignore
IGNORE_FILES = {
    'genesis_bots/teams/app.py',
}