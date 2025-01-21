import os
from flask import Blueprint
from genesis_bots.core.logging_config import logger
from flask import request, jsonify
from genesis_bots.demo.app import genesis_app
import json

realtime_routes = Blueprint('realtime_routes', __name__)


@realtime_routes.route("/realtime/get_tools", methods=["GET"])
def get_session_tools():
    try:
        bot_id = request.args.get("bot_id")
        if not bot_id:
            return jsonify({"success": False, "message": "Missing 'bot_id' parameter."}), 400

        # Find the session for the given bot_id
        session = next((s for s in genesis_app.sessions if s.bot_id == bot_id), None)

        if session is None:
            return jsonify({"success": False, "message": f"Session for bot ID '{bot_id}' not found."}), 404

        # Assuming session.tools is a dictionary or serializable object
        tools = session.tools

        return jsonify({"success": True, "tools": tools}), 200

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@realtime_routes.route("/realtime/genesis_tool", methods=["POST"])
def genesis_tool():
    try:
        data = request.json
        bot_id = data.get('bot_id')
        tool_name = data.get('tool_name')
        #query = data.get('query')

        params = data.get('params', {})
        # Convert params from string to dict if it's a string
        if isinstance(params, str):
            try:
                params = json.loads(params)
            except json.JSONDecodeError:
                return jsonify({"success": False, "message": "Invalid params format. Expected JSON string or object."}), 400
        if not isinstance(params, dict):
            return jsonify({"success": False, "message": "Invalid params format. Expected JSON object."}), 400

        # Check if thread_id is missing from params
        if 'thread_id' not in params and tool_name.startswith('_'):
            # If missing, add it with id "voice_1"
            params['thread_id'] = "voice_1"

        # Add return_base64=True parameter for _run_snowpark_python tool
        if tool_name == 'run_snowpark_python':
            params['return_base64'] = True
            params['save_artifacts'] = False

        # Find the session for the bot_id
        session = next((s for s in genesis_app.sessions if s.bot_id == bot_id), None)

        if session is None:
            return jsonify({"success": False, "message": f"Session for bot ID {bot_id} not found"}), 404

        # Search for the tool in the assistant's available functions
        tool = session.assistant_impl.available_functions.get(tool_name)
        if tool is None:
            # Try appending an underscore to the front of the tool name
            tool = session.assistant_impl.available_functions.get(f"_{tool_name}")
        if tool is None:
            return jsonify({"success": False, "message": f"Tool {tool_name} not found for bot {bot_id}"}), 404


        # Find the tool in the session.tools array
        # Run the tool with the query
        try:
            tool_result = tool(**params)
            return jsonify({"success": True, "results": tool_result})
        except Exception as tool_error:
            logger.error(f"Error running tool {tool_name} for bot {bot_id}: {str(tool_error)}")
            return jsonify({"success": False, "message": f"Error running tool: {str(tool_error)}"}), 500

    except Exception as e:
        logger.error(f"Error in tool call: {str(e)}")
        return jsonify({"success": False, "message": str(e)}), 500




def get_udf_endpoint_url(endpoint_name="udfendpoint"):

    alt_service_name = os.getenv("ALT_SERVICE_NAME", None)
    if alt_service_name:
        query1 = f"SHOW ENDPOINTS IN SERVICE {alt_service_name};"
    else:
        query1 = f"SHOW ENDPOINTS IN SERVICE {genesis_app.project_id}.{genesis_app.dataset_name}.GENESISAPP_SERVICE_SERVICE;"
    try:
        logger.warning(f"Running query to check endpoints: {query1}")
        results = genesis_app.db_adapter.run_query(query1)
        udf_endpoint_url = next(
            (
                endpoint["INGRESS_URL"]
                for endpoint in results
                if endpoint["NAME"] == endpoint_name
            ),
            None,
        )
        return udf_endpoint_url
    except Exception as e:
        logger.warning(f"Failed to get {endpoint_name} endpoint URL with error: {e}")
        return None
# Example curl command:
# curl -X GET "http://localhost:8080/realtime/get_tools?bot_id=Janice"
# Example curl command:
# curl -X GET "http://localhost:8080/realtime/get_udf_endpoint?endpoint_name=udfendpoint"
@realtime_routes.route("/realtime/get_endpoint", methods=["GET"])
def get_endpoint():
    try:
        endpoint_name = request.args.get("endpoint_name", "udfendpoint")
        endpoint_url = get_udf_endpoint_url(endpoint_name)

        if endpoint_url:
            return jsonify({
                "success": True,
                "endpoint_url": endpoint_url
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": f"Could not find endpoint URL for {endpoint_name}"
            }), 404

    except Exception as e:
        return jsonify({
            "success": False,
            "message": f"Error getting endpoint URL: {str(e)}"
        }), 500