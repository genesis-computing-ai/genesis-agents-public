import os
from flask import Flask
from core.logging_config import logger
from demo.app import genesis_app
from demo.routes import realtime_routes, slack_routes
from demo.routes import udf_routes, main_routes, auth_routes
import core.global_flags as global_flags

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
    app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)
    app_https.run(host=SERVICE_HOST, port=8082, ssl_context='adhoc', debug=False, use_reloader=False)

if __name__ == "__main__":    
    main()
