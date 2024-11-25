import logging
import os
import sys
import re


# Logging format use for root logger and GENESIS logger.
DEFAULT_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(funcName)s]:: %(message)s'
GENESIS_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(caller_filename)s:%(caller_lineno)s]:: %(message)s'

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


class LogSupressor:
    """
    A class to suppress repetitive log messages based on specified criteria.

    This class allows you to define suppression rules for log messages
    from a specific logger. You can specify the log level, a regular
    expression to match the log message, and the frequency of messages
    to display. Messages that match the criteria will be suppressed
    *after* the first occurrence, and only every n-th message will be shown
    thereafter.

    IMPORTANT: This class is should not be instantiated. Call LogSupressor.add_supressor directly.

    Attributes:
        _modulename_to_filtersspec_map (dict): A mapping of module names
            to their respective filter specifications.
    """
    _modulename_to_filtersspec_map = dict()

    class _FilterSpec:
        def __init__(self, logger_name, log_level, regexp, n, counter=0):
            if isinstance(log_level, str):
                log_level = logging.getLevelName(log_level.upper())
            self.logger_name = logger_name
            self.log_level = log_level
            self.regexp = regexp
            self.n = n
            self.counter = counter

    @staticmethod
    def _filter_record(record):
        logger_name = record.name
        specs = LogSupressor._modulename_to_filtersspec_map.get(logger_name, [])
        show_record = True
        for spec in specs:
            # if the record matches the spec then increase the counter and determine if it should be filtered.
            if record.levelno == spec.log_level and re.search(spec.regexp, record.getMessage()):
                spec.counter += 1
                show_record = spec.counter == 1 or spec.counter % spec.n == 1
                if show_record and spec.counter > 1:
                    record.msg += f" [NOTE: PREVIOUS {spec.n-1} SIMILAR RECORDS HAS BEEN SUPRESSED ({spec.regexp=})]"
        return show_record

    @classmethod
    def add_supressor(cls, logger_name, log_level, regexp, n):
        """
        Adds a log suppression rule for a specific logger.

        Args:
            logger_name (str): The name of the logger to which the suppression rule applies.
            log_level (str or int): The log level at which the suppression rule applies.
            regexp (str): A regular expression to match the log message.
            n (int): The frequency of messages to display after the first occurrence.
        """
        specs = cls._modulename_to_filtersspec_map.get(logger_name)
        if not specs:
            specs = []
            cls._modulename_to_filtersspec_map[logger_name] = specs
        specs.append(LogSupressor._FilterSpec(logger_name, log_level, regexp, n))

        # Apply our filter function to this logger, if it has not been applied already
        logger = logging.getLogger(logger_name)
        for filter in logger.filters:
            if filter is LogSupressor._filter_record:
                return # We aready added this filter
        logger.addFilter(LogSupressor._filter_record)


# Log configruation starts here
#-----------------------------------------
_setup_root_logger()

# this was done to allow logging syntax that matches print statments syntax (since we auto-converted them to log messages) e.g. (print("hello", end="")
logger = PrintLikeLogger(_setup_genesis_logger())


