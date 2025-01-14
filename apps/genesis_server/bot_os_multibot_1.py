import os, subprocess, sys
from flask import Flask
from pathlib import Path

from genesis_bots.demo.app import genesis_app
from genesis_bots.demo.routes import realtime_routes, slack_routes
from genesis_bots.demo.routes import udf_routes, main_routes, auth_routes
from genesis_bots.core import global_flags

main_server = None

def main():

    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
    global_flags.runner_id = runner_id
    global_flags.multibot_mode = True

    app = Flask(__name__)
    app_https = Flask(__name__)

    app.register_blueprint(main_routes)
    app.register_blueprint(realtime_routes)
    app.register_blueprint(slack_routes)
    app.register_blueprint(udf_routes)
    app_https.register_blueprint(auth_routes)

    SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

    genesis_app.start_all()

    if os.getenv("LAUNCH_GUI", "true").lower() != "false":

        streamlit_path = Path("apps/streamlit_gui/Genesis.py")
        if streamlit_path.exists():
            subprocess.Popen([
                sys.executable, "-m", "streamlit", "run",
                str(streamlit_path),
                "--server.port", "8501"
            ])

    app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)
    app_https.run(host=SERVICE_HOST, port=8082, ssl_context='adhoc', debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
