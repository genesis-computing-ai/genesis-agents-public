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

def start_ngrok():
    # Get the ngrok auth token from an environment variable
    global ngrok_from_env

    NGROK_AUTH_TOKEN = os.environ.get('NGROK_AUTH_TOKEN',None)

    if NGROK_AUTH_TOKEN:
        stored_token, _, _ = get_ngrok_auth_token()
        if stored_token != NGROK_AUTH_TOKEN or stored_token is None:
            set_ngrok_auth_token(NGROK_AUTH_TOKEN)

    if not NGROK_AUTH_TOKEN:
        ngrok_token, ngrok_use_domain, ngrok_domain = get_ngrok_auth_token()
        if ngrok_token is not None:
            NGROK_AUTH_TOKEN = ngrok_token

    if NGROK_AUTH_TOKEN:

        # Establish connectivity

        try:
            listener_8080 = ngrok.forward(8080, authtoken=NGROK_AUTH_TOKEN)
            listener_3978 = ngrok.forward(3978, authtoken=NGROK_AUTH_TOKEN)
        except:
            logger.info('NGROK not established')
            return False

        # Output ngrok url to console
        print(f"Ingress established at {listener_8080.url()} for port 8080")
        print(f"Ingress established at {listener_3978.url()} for port 3978")
        return listener_8080.url(), listener_3978.url()
    else:
        logger.info('Error: NGROK_AUTH_TOKEN environment variable not set.')
        return False


def launch_ngrok_and_update_bots(update_endpoints=False):

    if needs_ngrok:
        ngrok_urls = start_ngrok()

        if update_endpoints and ngrok_urls is not False:
            update_bot_endpoints(new_base_url=ngrok_urls[0])

        if ngrok_urls is not False:
            os.environ['NGROK_BASE_URL_8080'] = ngrok_urls[0]
            os.environ['NGROK_BASE_URL'] = ngrok_urls[0]
            os.environ['NGROK_BASE_URL_3978'] = ngrok_urls[1]

        if ngrok_urls == False:
            return False
        else:
            return True
    else:
        return False
