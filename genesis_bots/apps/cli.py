import click
import os
from   pathlib                  import Path
import sys
from   genesis_bots.apps.genesis_server \
                                import bot_os_multibot_1


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
    print(f"Resources and demo files set up successfully in {workdir}")


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
def start(launch_ui):
    """
    Start the genesis_bots services locally (as a blocking process).
    """
    # Implement start logic here
    if launch_ui:
        os.environ["LAUNCH_GUI"] = "TRUE"
    else:
        os.environ["LAUNCH_GUI"] = "FALSE"

    bot_os_multibot_1.main()


def main():
    return genesis_cli()


if __name__ == '__main__':
    sys.exit(main())

