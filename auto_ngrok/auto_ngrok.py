import os
#from ngrok import ngrok
from bot_genesis.make_baby_bot import update_bot_endpoints, get_ngrok_auth_token
from core.logging_config import setup_logger
logger = setup_logger(__name__)

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

    if not NGROK_AUTH_TOKEN:
        ngrok_token, ngrok_use_domain, ngrok_domain = get_ngrok_auth_token()
        if ngrok_token is not None:
            NGROK_AUTH_TOKEN = ngrok_token

    if NGROK_AUTH_TOKEN:

        # Establish connectivity

        try:
            listener = ngrok.forward(8080, authtoken=NGROK_AUTH_TOKEN)
        except:
            logger.info('NGROK not established')
            return False

        # Output ngrok url to console
        logger.info(f"Ingress established at {listener.url()}")
        return(listener.url())
    else:
        logger.info('Error: NGROK_AUTH_TOKEN environment variable not set.')
        return False 

   
def launch_ngrok_and_update_bots(update_endpoints=False):

    if needs_ngrok:
        ngrok_url = start_ngrok()

        if update_endpoints and ngrok_url is not False:
            update_bot_endpoints(new_base_url=ngrok_url)
        
        if ngrok_url is not False:
            os.environ['NGROK_BASE_URL'] = ngrok_url

        if ngrok_url == False:
            return False 
        else:
            return True
    else:
        return False

