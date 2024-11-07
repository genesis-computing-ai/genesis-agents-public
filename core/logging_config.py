import logging
import os
import sys

def setup_logger(name=None):
    logger = logging.getLogger(name)
    logger.propagate = False
    level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(level)

    # Check if handlers are already added to avoid duplicate logs
    if not logger.handlers:
        # Create handlers
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # Create formatters and add them to handlers
        formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(module)s][%(lineno)d]:: %(message)s')
        console_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(console_handler)

    return logger

# Create a custom logger class to handle print-like behavior
class PrintLikeLogger:
    def __init__(self, logger):
        self._logger = logger
    
    def _format_message(self, *args):
        return ' '.join(str(arg) for arg in args)
    
    def info(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.info(message)
    
    def error(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.error(message)
    
    def warning(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.warning(message)
        
    def debug(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.debug(message)
        
    def critical(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.critical(message)
        
    def exception(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.exception(message)

logger = PrintLikeLogger(setup_logger('GENSIS'))