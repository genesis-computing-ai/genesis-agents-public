import logging
import os
import sys

def setup_logger(name=None):
    logger = logging.getLogger(name)
    logger.propagate = False
    level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(level)

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # Update formatter to use custom attributes
        formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(caller_filename)s][%(caller_funcName)s:%(caller_lineno)s]:: %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Create a custom logger class to handle print-like behavior
class PrintLikeLogger:
    def __init__(self, logger):
        self._logger = logger
    
    def _format_message(self, *args):
        return ' '.join(str(arg) for arg in args)
    
    def _get_caller_info(self):
        return {
            'caller_funcName': sys._getframe(2).f_code.co_name,
            'caller_filename': os.path.basename(sys._getframe(2).f_code.co_filename),
            'caller_lineno': sys._getframe(2).f_lineno
        }
    
    def info(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.info(message, extra=self._get_caller_info())
    
    def error(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.error(message, extra=self._get_caller_info())
    
    def warning(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.warning(message, extra=self._get_caller_info())
    
    # Add alias for warn
    warn = warning
    
    def debug(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.debug(message, extra=self._get_caller_info())
    
    def critical(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.critical(message, extra=self._get_caller_info())
    
    def exception(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.exception(message, extra=self._get_caller_info())

logger = PrintLikeLogger(setup_logger('GENSIS'))