import os
import shutil
from pathlib import Path
import genesis_bots


# these are the top-level directories that we will create in the working directory (if not existing)
TOP_LEVEL_RESOURCE_DIRS = ["genesis_sample"]

def _copy_files_by_patterns(src_dir, dest_dir, patterns, verbose=False):
    copied_files = set()  # Keep track of what we've copied to avoid duplicates
    
    for pattern in patterns:
        for src_file in src_dir.glob(pattern):
            # Skip __pycache__ directories
            if "__pycache__" in src_file.parts:
                continue
                
            if src_file in copied_files:
                continue
                
            tgt_file = dest_dir / src_file.relative_to(src_dir)
            if src_file.is_dir():
                tgt_file.mkdir(parents=True, exist_ok=True)
                if verbose:
                    _trace_action(f"Created directory: {tgt_file}", verbose)
                continue
                
            tgt_file.parent.mkdir(parents=True, exist_ok=True)
            if src_file.resolve() == tgt_file.resolve():
                continue
                
            shutil.copy2(src_file, tgt_file)
            copied_files.add(src_file)
            if verbose:
                _trace_action(f"Copied file: {tgt_file}", verbose)


def _trace_action(message: str, verbose: bool):
    if verbose:
        print(" -->", message)


def _mkdir(path : Path, base_dir, verbose: bool):
    # check that the directory we are creating is (or a subdir of) one of the allowed top level directories.
    # we keep that list so that we know what to delete upon cleanup
    assert any(path.is_relative_to(base_dir / resource_dir) for resource_dir in TOP_LEVEL_RESOURCE_DIRS), (
        f"Refusing to create dir '{path}' since it is not a subdir of {base_dir}/<resource_dir> "
        f"where <resource_dir> is one of the allowed top level dirs: {TOP_LEVEL_RESOURCE_DIRS}"
    )
    _trace_action(f"Creating/updating directory: {path}", verbose)
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)



def copy_resources(base_dir=None, verbose=False):
    """
    Copies resources from the genesis_bots package to the specified base directory.
    This is intended to be called right after installing the genesis_bots package (with pip install)
    It copies the demo apps and golden defaults into the base directory, making them visible and editable for the users.

    Args:
        base_dir (str or Path, optional): The base directory where resources will be copied. 
                                          If None, defaults to CWD
        verbose (bool, optional): If True, prints detailed trace of actions being performed.

    """

    # Get the current working directory
    if base_dir is None:
        base_dir = Path.cwd()
    else:
        base_dir = Path(base_dir).resolve()

    # Get the source directory (to the root of the genesis_bots package)
    root_pkg_source_dir = Path(genesis_bots.__file__).parent

    # Copy demo apps data and source code
    demo_apps_src = root_pkg_source_dir / "genesis_sample_golden"
    demo_apps_dest = base_dir / "genesis_sample"
    demo_incl_globs = [
        "**/*",  # This should catch everything
    ]

    _mkdir(demo_apps_dest, base_dir, verbose)
    _copy_files_by_patterns(demo_apps_src, demo_apps_dest, demo_incl_globs, verbose=False)

    # Copy API documentation files (currently disabled - can't we just leave them in the package?)
    # api_src = root_pkg_source_dir / "api"
    # api_dest = base_dir / "api"
    # api_dest.mkdir(parents=True, exist_ok=True)
    # trace_action(f"Creating/updating directory: {api_dest}")

    # # Copy README.md and LICENSE
    # api_incl_globs = ["README.md", "LICENSE"]
    # _copy_files_by_patterns(api_src, api_dest, api_incl_globs, verbose)

    rcs_dirs = [base_dir / d for d in TOP_LEVEL_RESOURCE_DIRS]
    if len(rcs_dirs) == 1:
       rcs_dirs_str = f"directory {rcs_dirs[0]}"
    else:
        rcs_dirs_str = f"directories {', '.join([str(d) for d in rcs_dirs])}"

    _trace_action(f"DONE creating/updating resources in {rcs_dirs_str}", verbose)


def cleanup_resources(base_dir=None, skip_git_dirs=True, verbose=False):
    """
    Cleanup the resources created by the copy_resources function.
    """
    if base_dir is None:
        base_dir = Path.cwd()
    else:
        base_dir = Path(base_dir).resolve()
    from git import Repo, InvalidGitRepositoryError

    def is_under_git(path):
        try:
            repo = Repo(path, search_parent_directories=True)
            # Check if the specific path is tracked in git
            if path.is_file():
                return path in repo.untracked_files or repo.git.ls_files(path, error_unmatch=True)
            else:
                return any((path / f).exists() for f in repo.untracked_files) or any(repo.git.ls_files(path / '**/*', error_unmatch=True))
        except InvalidGitRepositoryError:
            return False
        except Exception as e:
            pass # defaults to False if we can't check
        return False

    # Remove the top-level dirs (unless they are under git control)
    for dirs in TOP_LEVEL_RESOURCE_DIRS:
        dest = base_dir / dirs
        if dest.exists():
            if skip_git_dirs and is_under_git(dest):
                _trace_action(f"Skipping cleanup of directory {dest} since it's under git control", verbose)
            else:
                shutil.rmtree(dest)
                _trace_action(f"Removed directory: {dest}", verbose)

    _trace_action(f"DONE cleaning up resources in directory {base_dir}", verbose)

