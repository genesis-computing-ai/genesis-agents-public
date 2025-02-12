import os
import shutil
from pathlib import Path
import genesis_bots


# these are the top level directories that we will create on the working directory (of not existing)
# Note that in development mode, golden_defaults might be under source control, so we don't want to delete it
TOP_LEVEL_RESOURCE_DIRS = ["apps", "genesis_bots/golden_defaults"]

def _copy_files_by_patterns(src_dir, dest_dir, patterns, verbose=False):
    for pattern in patterns:
        for src_file in src_dir.glob(pattern):
            tgt_file = dest_dir / src_file.relative_to(src_dir)
            tgt_file.parent.mkdir(parents=True, exist_ok=True) # Ensure the target directory exists
            if src_file.resolve() == tgt_file.resolve(): # don't copy if the source and target are the same file (can happen in development mode)
                continue
            shutil.copy2(src_file, tgt_file)


def _trace_action(message : str, verbose: bool):
    if verbose:
        print(" -->", message)


def _mkdir(path : Path, base_dir, verbose: bool):
    # check that the directory we are creating is (or a subdir of) one of the allowed top level directories.
    # we keep that list so that we know what to delete upon cleanup
    assert any(path.is_relative_to(base_dir / resource_dir) for resource_dir in TOP_LEVEL_RESOURCE_DIRS), (
        f"Refusing to create dir '{path}' since it is not a subdir of {base_dir}/<resource_dir> "
        f"where <resource_dir> is one of the allowed top level dirs: {TOP_LEVEL_RESOURCE_DIRS}"
    )
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        _trace_action(f"Creating/updating directory: {path}", verbose)


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
    demo_apps_src = root_pkg_source_dir / "apps" / "demos"
    demo_apps_dest = base_dir / "apps" / "demos"
    demo_incl_globs = ["demo_data/**/*",
                       "database_demos/**/*.sql",
                       "database_demos/**/*.md",
                       "database_demos/**/*.py",
                       "*.py",
                       ]
    _mkdir(demo_apps_dest, base_dir, verbose)
    _copy_files_by_patterns(demo_apps_src, demo_apps_dest, demo_incl_globs, verbose)

    # Copy golden defaults structure
    golden_src = root_pkg_source_dir / "golden_defaults"
    golden_dest = base_dir / "genesis_bots" / "golden_defaults"
    if golden_dest == golden_src:
        _trace_action("Skipping creating/updating {golden_dest} since it is the same as the source directory", verbose)
    elif golden_src.exists():
        _mkdir(golden_dest, base_dir, verbose)
        golden_incl_globs = ["golden_notes/**/*",
                             "golden_processes/**/*",
                            ]
        _copy_files_by_patterns(golden_src, golden_dest, golden_incl_globs, verbose)

    # Copy API documentation files (currently disabes - can't we just leave them in the package?)
    # api_src = root_pkg_source_dir / "api"
    # api_dest = base_dir / "api"
    # api_dest.mkdir(parents=True, exist_ok=True)
    # trace_action(f"Creating/updating directory: {api_dest}")

    # # Copy README.md and LICENSE
    # api_incl_globs = ["README.md", "LICENSE"]
    # _copy_files_by_patterns(api_src, api_dest, api_incl_globs, verbose)

    _trace_action(f"DONE creating/updating resources in directory {base_dir}", verbose)


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
                _trace_action(f"Skipping cleanup of directory {dest} since its under git control", verbose)
            else:
                shutil.rmtree(dest)
                _trace_action(f"Removed directory: {dest}", verbose)

    _trace_action(f"DONE cleaning up resources in directory {base_dir}", verbose)

