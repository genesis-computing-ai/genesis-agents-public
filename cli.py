# genesis_bots/cli.py
from pathlib import Path
import logging
import click
from datetime import datetime
import sys
from typing import Optional

from .control import GenesisService

def setup_logging():
    logger = logging.getLogger('genesis-cli')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger

logger = setup_logging()
genesis = GenesisService()

@click.group()
@click.option('--debug', is_flag=True, help='Enable debug output')
def cli(debug):
    """Genesis Bots Service Management CLI"""
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled")

@cli.command()
@click.argument('service', type=click.Choice(list(GenesisService.AVAILABLE_SERVICES.keys()) + ['all']))
@click.option('--wait/--no-wait', default=True, help='Wait for service to start')
def start(service: str, wait: bool):
    """Start a service or all services"""
    try:
        if service == 'all':
            for svc in GenesisService.AVAILABLE_SERVICES:
                try:
                    result = genesis.start_service(svc, wait=wait)
                    click.echo(f"Started {svc}: {result}")
                except ValueError as e:
                    click.echo(f"Failed to start {svc}: {e}", err=True)
        else:
            result = genesis.start_service(service, wait=wait)
            click.echo(f"Started {service}: {result}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('service', type=click.Choice(list(GenesisService.AVAILABLE_SERVICES.keys()) + ['all']))
@click.option('--timeout', default=5, help='Timeout in seconds for graceful shutdown')
def stop(service: str, timeout: int):
    """Stop a service or all services"""
    try:
        if service == 'all':
            for svc in GenesisService.AVAILABLE_SERVICES:
                try:
                    result = genesis.stop_service(svc, timeout=timeout)
                    click.echo(f"Stopped {svc}: {result}")
                except ValueError as e:
                    click.echo(f"Failed to stop {svc}: {e}", err=True)
        else:
            result = genesis.stop_service(service, timeout=timeout)
            click.echo(f"Stopped {service}: {result}")
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--service', type=click.Choice(list(GenesisService.AVAILABLE_SERVICES.keys())),
              help='Show status for specific service')
def status(service: Optional[str] = None):
    """Show status of services"""
    try:
        services = genesis.list_services()
        if service:
            if service in services:
                _display_service_status(service, services[service])
            else:
                click.echo(f"Unknown service: {service}", err=True)
        else:
            for svc_name, svc_info in services.items():
                _display_service_status(svc_name, svc_info)
    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)

def _display_service_status(name: str, info: dict):
    """Helper to format service status output"""
    status_colors = {
        'running': 'green',
        'stopped': 'red',
        'unknown': 'yellow'
    }
    status = info['status']
    click.echo(f"{name}:")
    click.echo(f"  Status: {click.style(status, fg=status_colors.get(status, 'white'))}")
    if info['pid']:
        click.echo(f"  PID: {info['pid']}")
    if info['log_file']:
        click.echo(f"  Log: {info['log_file']}")

@cli.command()
@click.argument('service', type=click.Choice(list(GenesisService.AVAILABLE_SERVICES.keys())))
def logs(service: str):
    """View logs for a service"""
    try:
        log_file = genesis.get_latest_log_file(service)
        if log_file:
            # Use system's default pager for log viewing
            click.echo_via_pager(Path(log_file).read_text())
        else:
            click.echo(f"No logs found for {service}", err=True)
    except Exception as e:
        click.echo(f"Error reading logs: {str(e)}", err=True)
        sys.exit(1)

def main():
    cli()

if __name__ == '__main__':
    main()

# usage:
# genesis start bot_os
# genesis start all
# genesis stop task_service
# genesis status
# genesis logs harvester_service