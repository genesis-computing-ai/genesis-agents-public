import logging
import os

def setup_logger(name=None):
    logger = logging.getLogger(name)
    level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(level)

    # Check if handlers are already added to avoid duplicate logs
    if not logger.handlers:
        # Create handlers
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Create formatters and add them to handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)

    return logger