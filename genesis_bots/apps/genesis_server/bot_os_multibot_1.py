from   flask                    import Flask
import os
from   pathlib                  import Path
import subprocess
import sys

from   genesis_bots.core        import global_flags
from   genesis_bots.demo.app.genesis_app \
                                import (DEFAULT_HTTPS_ENDPOINT_PORT,
                                        DEFAULT_HTTP_ENDPOINT_PORT,
                                        DEFAULT_STREAMLIT_APP_PORT,
                                        genesis_app)
from   genesis_bots.demo.routes import (oauth_routes, main_routes,
                                        realtime_routes, slack_routes,
                                        udf_routes)

main_server = None

def main():
    runner_id = os.getenv("RUNNER_ID", "jl-local-runner")
    genbot_internal_project_and_schema = os.getenv("GENESIS_INTERNAL_DB_SCHEMA")
    if not genbot_internal_project_and_schema and os.getenv("SNOWFLAKE_METADATA", "FALSE").upper() != "TRUE":
        os.environ["GENESIS_INTERNAL_DB_SCHEMA"] = "NONE.NONE"
    global_flags.runner_id = runner_id
    global_flags.multibot_mode = True

    app = Flask(__name__)
    app_https = Flask(__name__)

    app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your-development-secret-key')

    app.register_blueprint(main_routes)
    app.register_blueprint(realtime_routes)
    app.register_blueprint(slack_routes)
    app.register_blueprint(udf_routes)
    app_https.register_blueprint(oauth_routes)

    app.register_blueprint(oauth_routes, url_prefix='/oauth')

    SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

    genesis_app.start_all()

    if os.getenv("LAUNCH_GUI", "true").lower() != "false":
        streamlit_path = Path(__file__).parent.parent / "streamlit_gui" / "Genesis.py"
        if streamlit_path.exists():
            try:
                additional_params = [
                    "--browser.gatherUsageStats=false",
                ]
                if os.name == 'nt':  # Windows
                    cmd = f'{sys.executable} -m streamlit run "{str(streamlit_path)}" --server.port {DEFAULT_STREAMLIT_APP_PORT} ' + ' '.join(additional_params)
                    subprocess.Popen(cmd, shell=True)
                else:  # Unix-like systems
                    subprocess.Popen([
                        sys.executable, "-m", "streamlit", "run",
                        str(streamlit_path),
                        "--server.port", str(DEFAULT_STREAMLIT_APP_PORT)
                    ] + additional_params)
            except Exception as e:
                print(f"Failed to start Streamlit: {e}")

    app.run(host=SERVICE_HOST, port=DEFAULT_HTTP_ENDPOINT_PORT, debug=False, use_reloader=False)
    app_https.run(host=SERVICE_HOST, port=DEFAULT_HTTPS_ENDPOINT_PORT, ssl_context='adhoc', debug=False, use_reloader=False)

if __name__ == "__main__":
    main()
