import os
from flask import Flask
from core.logging_config import logger
from demo.config import scheduler
from demo.routes import realtime_routes, slack_routes, udf_routes, main_routes


app = Flask(__name__)

app.register_blueprint(main_routes)
app.register_blueprint(realtime_routes)
app.register_blueprint(slack_routes)
app.register_blueprint(udf_routes)

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")

scheduler.start()

if __name__ == "__main__":
    app.run(host=SERVICE_HOST, port=8080, debug=False, use_reloader=False)
