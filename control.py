from pathlib import Path
import logging
from datetime import datetime
import subprocess
import sys
import os


class GenesisService:
    AVAILABLE_SERVICES = {
        'bot_os_service': 'apps/genesis_server/bot_os_multibot_1.cpython-312-darwin.so',
        'harvester_service': 'genesis_bots/services/standalone_harvester.cpython-312-darwin.so',
        'task_service': 'genesis_bots/services/bot_os_task_server.cpython-312-darwin.so',
        'knowledge_service': 'genesis_bots/services/knowledge_server.cpython-312-darwin.so'
    }

    DEFAULT_LOG_DIR = Path.home() / ".genesis" / "logs"
    DEFAULT_PID_DIR = Path.home() / ".genesis" / "pid"

    def __init__(self):
        self.DEFAULT_LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.DEFAULT_PID_DIR.mkdir(parents=True, exist_ok=True)

    def setup_logging(self, service_name):
        log_file = self.DEFAULT_LOG_DIR / f"{service_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logger = logging.getLogger(service_name)
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.handlers.clear()
        logger.addHandler(file_handler)

        return log_file, logger

    def get_latest_log_file(self, service_name):
        try:
            log_files = list(self.DEFAULT_LOG_DIR.glob(f"{service_name}_*.log"))
            return str(max(log_files, key=lambda x: x.stat().st_mtime)) if log_files else None
        except Exception:
            return None

    def list_services(self):
        services = {}
        for service_name in self.AVAILABLE_SERVICES:
            pid_file = self.DEFAULT_PID_DIR / f"{service_name}.pid"
            log_file = self.get_latest_log_file(service_name)

            if pid_file.exists():
                try:
                    with open(pid_file) as f:
                        pid = int(f.read().strip())
                    services[service_name] = {'status': 'running', 'pid': pid, 'log_file': log_file}
                except (ValueError, IOError):
                    services[service_name] = {'status': 'unknown', 'pid': None, 'log_file': log_file}
            else:
                services[service_name] = {'status': 'stopped', 'pid': None, 'log_file': log_file}
        return services

    def start_service(self, service_name: str, wait: bool = True):
        if service_name not in self.AVAILABLE_SERVICES:
            raise ValueError(f"Unknown service: {service_name}")

        service_config = self.AVAILABLE_SERVICES[service_name]

        # Get path to the installed package
        import genesis_bots
        base_path = Path(genesis_bots.__file__).parent

        try:
            log_file = self.DEFAULT_LOG_DIR / f"{service_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            log_fh = open(log_file, 'w')

            script_path = base_path / service_config
            if not script_path.exists():
                raise ValueError(f"Service script not found: {script_path}")

            # Convert path to module path
            module_path = f"genesis_bots.{service_config.replace('/', '.').replace('.cpython-312-darwin.so', '')}"
            process = subprocess.Popen(
                [sys.executable, "-c", f"import {module_path}; {module_path}.main()"],
                stdout=log_fh,
                stderr=log_fh,
                universal_newlines=True,
                env=dict(os.environ, PYTHONUNBUFFERED="1")
            )

            if process.poll() is not None:
                log_fh.close()
                raise ValueError("Process failed to start")

            pid_file = self.DEFAULT_PID_DIR / f"{service_name}.pid"
            with open(pid_file, "w") as f:
                f.write(str(process.pid))

            return {
                'service': service_name,
                'pid': process.pid,
                'status': 'starting',
                'log_file': str(log_file)
            }

        except Exception as e:
            pid_file = self.DEFAULT_PID_DIR / f"{service_name}.pid"
            if pid_file.exists():
                pid_file.unlink()
            raise ValueError(f"Failed to start {service_name}: {str(e)}")

    def stop_service(self, service_name: str, timeout: int = 5):
        if service_name not in self.AVAILABLE_SERVICES:
            raise ValueError(f"Unknown service: {service_name}")

        pid_file = Path(f"{service_name}.pid")
        if not pid_file.exists():
            return {'service': service_name, 'status': 'not_running'}

        try:
            with open(pid_file) as f:
                pid = int(f.read().strip())

            os.kill(pid, 15)

            for _ in range(timeout):
                try:
                    os.kill(pid, 0)
                    import time
                    time.sleep(1)
                except ProcessLookupError:
                    break
            else:
                os.kill(pid, 9)

            pid_file.unlink()
            return {'service': service_name, 'status': 'stopped'}

        except (ValueError, IOError) as e:
            raise ValueError(f"Error reading PID file: {str(e)}")
        except ProcessLookupError:
            pid_file.unlink()
            return {'service': service_name, 'status': 'not_running'}
        except Exception as e:
            raise ValueError(f"Failed to stop {service_name}: {str(e)}")


# Usage example:
if __name__ == "__main__":
    genesis = GenesisService()
    result = genesis.start_service("bot_os")
    print(f"Service status: {result}")