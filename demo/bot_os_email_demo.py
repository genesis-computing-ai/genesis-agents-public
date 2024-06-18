import imaplib
import email
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
from core.bot_os import BotOsSession
from core.bot_os_defaults import ELSA_DATA_ANALYST_INSTRUCTIONS
from core.bot_os_server import BotOsServer

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def send_email_response(
    session_id: str,
    thread_id: str,
    status: str,
    output: str,
    messages: str,
    attachments: [],
):
    logger.debug(
        f"send_email_response - {session_id} {thread_id} {status} {output} {messages} has {len(attachments)} attachments"
    )
    print(output)


session = BotOsSession(
    "Elsa_03",
    event_callback=send_email_response,
    instructions=ELSA_DATA_ANALYST_INSTRUCTIONS,
)


def check_email():
    mail = imaplib.IMAP4_SSL("imap.example.com")
    mail.login("your_email@example.com", "yourpassword")
    mail.select("inbox")

    result, data = mail.search(None, "ALL")
    ids = data[0]
    id_list = ids.split()
    latest_email_id = id_list[-1]

    result, data = mail.fetch(latest_email_id, "(RFC822)")
    raw_email = data[0][1]
    email_message = email.message_from_bytes(raw_email)

    # Parse the email contents here and call your bot's API


# scheduler.add_job(check_email, 'interval', minutes=5)

app = Flask(__name__)
scheduler = BackgroundScheduler()  # wish we could move this inside BotOsServer
server = BotOsServer(
    app, session=session, scheduler=scheduler, scheduler_seconds_interval=10
)
server.add_job(check_email, "interval", minutes=5)
scheduler.start()

if __name__ == "__main__":
    app.run(port=8080, debug=True)
