import streamlit as st
import time
from utils.snowflake_connector import submit_to_udf_proxy, get_response_from_udf_proxy
from core.logging_config import setup_logger
logger = setup_logger(__name__)

def get_bot_response(session, run_mode, user_input, bot_id, in_resp):

    thread_id = st.session_state.get(f"thread_id_{bot_id}", "")
    request_id = submit_to_udf_proxy(session, run_mode, user_input, thread_id, bot_id)
    
    if not request_id:
        st.error("Failed to get a request ID")
        yield "I'm sorry, I couldn't process your request. Please try again."
        return

    def get_bot_response_sync():

        while True:
            response = get_response_from_udf_proxy(
                uu=request_id, bot_id=bot_id, session=session, run_mode=run_mode
            )
            logger.info(response)
            if response != "not found":
                return response
            time.sleep(0.5)  # Short delay before checking again

    def response_generator(in_resp=None):
        previous_response = ""
        #  start_time = time.time()
        while True:
            if in_resp is None:
                response = get_response_from_udf_proxy(
                    uu=request_id, bot_id=bot_id, session=session, run_mode=run_mode
                )
            else:
                response = in_resp
                in_resp = None

            # st.write(response)
            if response != previous_response:
                if response != "not found":
                    if ( 
                        len(previous_response) > 10
                        and '...' in previous_response[-6:]
                        and chr(129520) in previous_response[-50:]
                        # ":toolbox:" in previous_response
                        and (
                            len(response) > len(previous_response)
                  #          or response[len(previous_response)] != previous_response
                        )
                    ):
                        offset = 0
                        new_increment = "\n\n"  + response[
                            max(len(previous_response) - 2, 0) : len(response) - offset
                        ]
               #         if new_increment.startswith('\n\n.') and not new_increment.startswith('\n\n..'):
               #             new_increment = new_increment[:2] + new_increment[3:]
                    else:
                        if len(response) >= 2 and ord(response[-1]) == 128172:
                            offset = -2
                        else:
                            offset = 0
                        new_increment = response[
                            max(len(previous_response) - 2, 0) : len(response) - offset
                        ]
                                                    # Remove single leading dot if present
                    # Check if response ends with '..' but not '...'
                 #   if chr(129520) in response[-50:] or chr(129520) in previous_response[-50:]:
                 #       st.write('here')
                 #       if '..' in new_increment and '...' not in new_increment:
                 #           new_increment = new_increment.replace('..', '...')

                    previous_response = response
                    try:
                        if ord(new_increment[-1]) == 128172:
                            new_increment = new_increment[:-2]
                    except:
                        new_increment = ''
                    yield new_increment
            if response != 'not found' and ( len(response) < 3 or ord(response[-1]) != 128172):
                break
            if len(response)>=1 and ord(response[-1]) == 128172:
                time.sleep(0.5)
            # if len(response) > 1 and response[:10] != ':toolbox: ' and time.time() - start_time > 3:
            #    break

    with st.spinner("Thinking..."):
        in_resp = get_bot_response_sync()

    for chunk in response_generator(in_resp):
        yield chunk