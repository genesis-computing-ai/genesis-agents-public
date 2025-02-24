import os
# from ngrok import ngrok
from genesis_bots.bot_genesis.make_baby_bot import update_bot_endpoints, get_ngrok_auth_token, set_ngrok_auth_token
from genesis_bots.core.logging_config import logger

ngrok_from_env = False

spcs_test =  os.getenv('SNOWFLAKE_HOST',None)

if spcs_test is not None:
    # in SPCS, so No Ngrok
    needs_ngrok = False
else:
    needs_ngrok = True

if needs_ngrok:
    from ngrok import ngrok

def stop_existing_ngrok():
    try:
        # Stop tunnels through the ngrok API
        ngrok.disconnect()  # Disconnects all tunnels
        logger.info('Stopped existing ngrok tunnels')
        return True
    except Exception as e:
        logger.info(f"Error stopping ngrok tunnels: {e}")
        return False

def start_ngrok():
    # Get the ngrok auth token from an environment variable
    global ngrok_from_env

    NGROK_AUTH_TOKEN = os.environ.get('NGROK_AUTH_TOKEN',None)

    if not NGROK_AUTH_TOKEN:
        ngrok_token, ngrok_use_domain, ngrok_domain = get_ngrok_auth_token()
        if ngrok_token is not None:
            NGROK_AUTH_TOKEN = ngrok_token

    if NGROK_AUTH_TOKEN:
        try:
            # Set auth token
            ngrok.set_auth_token(NGROK_AUTH_TOKEN)

            # If we don't have both tunnels, create new ones
            initial_tunnel = ngrok.connect(
                addr="http://localhost:8080",
                proto="http",
                name="web-tunnel"
            )

            listener_3978 = ngrok.connect(
                addr="http://localhost:3978",
                proto="http",
                name="bot-tunnel"
            )

            return initial_tunnel.url(), listener_3978.url()
        except Exception as e:
            logger.info(f"Error establishing NGROK connection: {e}")
            logger.info('NGROK not established')
            return False

    else:
        logger.info('Error: NGROK_AUTH_TOKEN environment variable not set.')
        return False


def launch_ngrok_and_update_bots(update_endpoints=False):
    if needs_ngrok:
        # Check if ngrok is already running
        existing_listeners = ngrok.get_listeners()
        if existing_listeners:
            logger.info('Ngrok already running, skipping start')
            return True

        # If no listeners exist, start new ones
        ngrok_urls = start_ngrok()

        if update_endpoints and ngrok_urls is not False:
            update_bot_endpoints(new_base_url=ngrok_urls[0], runner_id=os.getenv('RUNNER_ID', 'jl-local-runner'))

        if ngrok_urls is not False:
            logger.info(f'NGROK Web endpoint (port 8080): {ngrok_urls[0]}')
            logger.info(f'NGROKBot endpoint (port 3978): {ngrok_urls[1]}')
            os.environ['NGROK_BASE_URL_8080'] = ngrok_urls[0]
            os.environ['NGROK_BASE_URL'] = ngrok_urls[0]
            os.environ['NGROK_BASE_URL_3978'] = ngrok_urls[1]

        if ngrok_urls == False:
            return False
        else:
            return True
    else:
        return False
