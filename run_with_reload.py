import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from core.logging_config import logger

class ChangeHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith('.py'):
            logger.info(f"File {event.src_path} has been modified. Restarting...")
            self.restart_app()

    def restart_app(self):
        global process
        process.terminate()
        process.wait()
        process = subprocess.Popen(["python", "gradio_gui/main.py"])

if __name__ == "__main__":
    process = subprocess.Popen(["python", "gradio_gui/main.py"])

    event_handler = ChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path='.', recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()