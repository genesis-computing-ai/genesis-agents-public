import os
from flask import Flask
from core.logging_config import logger
from demo.app import genesis_app
from demo.routes import realtime_routes, slack_routes
from demo.routes import udf_routes, main_routes, auth_routes


app = Flask(__name__)
app_https = Flask(__name__)

app.register_blueprint(main_routes)
app.register_blueprint(realtime_routes)
app.register_blueprint(slack_routes)
app.register_blueprint(udf_routes)
app_https.register_blueprint(auth_routes)

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

genesis_app.start()
app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)
app_https.run(host=SERVICE_HOST, port=8082, ssl_context='adhoc', debug=False, use_reloader=False)
