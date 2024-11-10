import logging
import os
import sys


# Logging format use for root logger and GENESIS logger.
DEFAULT_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(filename)s][%(funcName)s:%(lineno)s]:: %(message)s'
GENESIS_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(caller_filename)s][%(caller_funcName)s:%(caller_lineno)s]:: %(message)s'

GENESIS_LOGGER_NAME = "GENESIS"

def _setup_root_logger():
    root_logger = logging.getLogger()
    if not root_logger.hasHandlers():  # Check if the root logger has no handlers
        logging.basicConfig(level=logging.WARNING, format=DEFAULT_LOGGER_FOMRAT)
        # Ensure the root handler respects only WARNING and above
        for handler in root_logger.handlers:
            handler.setLevel(logging.WARNING)
        # Add a NullHandler to prevent "No handlers could be found" warnings
        null_handler = logging.NullHandler()
        root_logger.addHandler(null_handler)


def _setup_genesis_logger(name=GENESIS_LOGGER_NAME):
    logger = logging.getLogger(name)
    logger.propagate = False # do not propagate to the root logger. We thus override its default
    level = os.environ.get('LOG_LEVEL', 'INFO')
    logger.setLevel(level)

    # Define custom log level name and value
    TELEMETRY_LEVEL = 25
    logging.addLevelName(TELEMETRY_LEVEL, "TELEMETRY")

    # Add a method to the Logger class to handle the custom level
    def telemetry(self, message, *args, **kwargs):
        if self.isEnabledFor(TELEMETRY_LEVEL):
            self._log(TELEMETRY_LEVEL, message, args, **kwargs)

    # Attach the custom method to the Logger class
    logging.Logger.telemetry = telemetry

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # Update formatter to use custom attributes
        formatter = logging.Formatter(GENESIS_LOGGER_FOMRAT)
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

    def telemetry(self, *args, **kwargs):
        message = self._format_message(*args)
        self._logger.telemetry(message, extra=self._get_caller_info())

_setup_root_logger()
logger = PrintLikeLogger(_setup_genesis_logger())