import click
import os
from   pathlib                  import Path
import sys

@click.group()
def genesis_cli():
    """
    CLI for managing genesis_bots services.
    """
    pass


@genesis_cli.command()
@click.option('--working_dir', '-d', default='.', type=click.Path(),
              help='The directory where to setup example files and sample data.')
def setup(working_dir):
    """
    Setup example files and sample data in a working directory.
    """
    workdir = Path(working_dir).resolve()
    from genesis_bots.apps.install_resources import copy_resources
    copy_resources(workdir, verbose=True)
    print(f"Resources and demo files set up successfully")


@genesis_cli.command()
@click.option('--working_dir', '-d', default='.', type=click.Path(),
              help='The directory where the setup was created.')
def cleanup(working_dir):
    """
    Cleanup the setup example files and sample data in the working directory.
    """
    workdir = Path(working_dir).resolve()
    from genesis_bots.apps.install_resources import cleanup_resources
    cleanup_resources(workdir, verbose=True)


@genesis_cli.command()
@click.option('--launch-ui/--no-launch-ui', default=True,
              help='Specify whether to launch the UI frontend (default: --launch-ui).')
@click.option('--log_level', '--log', default=None, hidden=True, # NOTE: hidden. For internal/advanced use.
              help='Set the LOG_LEVEL')
def start(launch_ui, log_level):
    """
    Start the genesis_bots services locally (as a blocking process).
    """
    resources_dir = Path("genesis_sample")
    print(f"Checking for Genesis sample directory: {resources_dir}", flush=True)
    if not resources_dir.exists() or not resources_dir.is_dir():
        print("Error: 'genesis_sample' directory not found in current working directory.", flush=True)
        print("Please run 'genesis setup' first to create the required resources.", flush=True)
        sys.stdout.flush()  # Extra flush for good measure
        os._exit(1)  # Force immediate exit
    from genesis_bots.apps.genesis_server import bot_os_multibot_1
    if launch_ui:
        os.environ["LAUNCH_GUI"] = "TRUE"
    else:
        os.environ["LAUNCH_GUI"] = "FALSE"

    # Set the global log level env var before importing any genesis_bots modules.
    if log_level:
        from genesis_bots.core.logging_config import logger
        try:
            logger.setLevel(log_level.upper())
            os.environ["LOG_LEVEL"] = log_level.upper() # keep env var consistent with the new logger level.
        except Exception as e:
            print(f"Error setting log level. Invalid log level?")
            sys.exit(1)

    # launch the server
    bot_os_multibot_1.main()


def main():
    return genesis_cli()


if __name__ == '__main__':
    sys.exit(main())

