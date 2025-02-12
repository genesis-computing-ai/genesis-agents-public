import os
import shutil
from pathlib import Path
import genesis_bots


def _copy_files_by_patterns(src_dir, dest_dir, patterns, verbose=False):
    for pattern in patterns:
        for src_file in src_dir.glob(pattern):
            tgt_file = dest_dir / src_file.relative_to(src_dir)
            tgt_file.parent.mkdir(parents=True, exist_ok=True) # Ensure the target directory exists
            if src_file.resolve() == tgt_file.resolve(): # don't copy if the source and target are the same file (can happen in development mode)
                continue
            shutil.copy2(src_file, tgt_file)


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

    def trace_action(message):
        if verbose:
            print(" -->", message)

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
    demo_apps_dest.mkdir(parents=True, exist_ok=True)
    trace_action(f"Creating/updating directory: {demo_apps_dest}")
    _copy_files_by_patterns(demo_apps_src, demo_apps_dest, demo_incl_globs, verbose)

    # Copy golden defaults structure
    golden_src = root_pkg_source_dir / "golden_defaults"
    golden_dest = base_dir / "genesis_bots" / "golden_defaults"
    if golden_src.exists():
        golden_dest.mkdir(parents=True, exist_ok=True)
        trace_action(f"Creating/updating directory: {golden_dest}")
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

if __name__ == '__main__':
    copy_resources(verbose=True)
    print("Local Resources created/updated successfully.")