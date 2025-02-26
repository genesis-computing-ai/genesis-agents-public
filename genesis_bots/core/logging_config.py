import logging
import os
import re
import sys
from   contextlib               import contextmanager

# Logging format use for root logger and GENESIS logger.
DEFAULT_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)s][%(funcName)s]:: %(message)s'
GENESIS_LOGGER_FOMRAT = '[%(asctime)s][%(levelname)s][%(caller_filename)s:%(caller_lineno)s]:: %(message)s'
GENESIS_LOGGER_NAME = "GENESIS"
GENESIS_LOGGER_DEFAULT_LEVEL = logging.WARNING
TELEMETRY_LEVEL = 25


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


class GenesisLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        self.telemetry_logs = {'messages': 0, 'prompt_tokens': 0, 'completion_tokens': 0}

    def _log(self, level, msg, args, **kwargs):
        # this override allows logging syntax that matches print statments syntax e.g. (print("hello", end="").
        # (since we bulk-converted many prints to logs, we retained this behavior messages).
        if args:
            msg = msg + ' ' + ' '.join(str(arg) for arg in args)
            args = ()
        if level == TELEMETRY_LEVEL:
            items = msg.split(' ')
            if items[0] == 'add_answer:':
                self.telemetry_logs['messages'] += 1
                self.telemetry_logs['prompt_tokens'] += int(items[5])
                self.telemetry_logs['completion_tokens'] += int(items[6])

        # Call the parent class's log method to actually log the message
        super()._log(level, msg, args, **kwargs, extra=self._get_caller_info())


    def reset_telemetry(self):
        self.telemetry_logs = {'messages': 0, 'prompt_tokens': 0, 'completion_tokens': 0}


    def _get_caller_info(self):
        return {
            'caller_funcName': sys._getframe(3).f_code.co_name,
            'caller_filename': os.path.basename(sys._getframe(3).f_code.co_filename),
            'caller_lineno': sys._getframe(3).f_lineno
        }


# logging.warn is deprecated but we have a lot of legacy code using logger.warn.
# With the below line we 'un-deprecate' it. Without this line, logger.warn will call logger.warning
# with an additional frame, messing up the caller info above.
GenesisLogger.warn = GenesisLogger.warning

class _ColoredLogRecordFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno > logging.INFO:
            level_color = None
            # Special formatting for level > INFO
            if record.levelname == 'ERROR':
                level_color = '\033[91m'  # Red
                record.msg = f"{level_color}{record.msg}\033[0m"  # Make the whole message red as well
            elif record.levelname == 'WARNING':
                level_color = '\033[93m'  # Yellow
            elif record.levelname == 'TELEMETRY':
                level_color = '\033[94m'  # Blue
            if level_color:
                record.levelname = f"{level_color}{record.levelname}\033[0m"

        return super().format(record)


def _setup_genesis_logger(name=GENESIS_LOGGER_NAME):
    logging.setLoggerClass(GenesisLogger)
    logger = logging.getLogger(name)
    logger.propagate = False # do not propagate to the root logger. We thus override its default
    level = os.environ.get('LOG_LEVEL') or GENESIS_LOGGER_DEFAULT_LEVEL
    logger.setLevel(level)

    # Define custom log level name and value
    logging.addLevelName(TELEMETRY_LEVEL, "TELEMETRY")

    # Add a method to the Logger class to handle the custom level
    def telemetry(self, message, *args, **kwargs):
        if self.isEnabledFor(TELEMETRY_LEVEL):
            self._log(TELEMETRY_LEVEL, message, args, **kwargs)

    # Attach the custom method to the Logger class
    logging.Logger.telemetry = telemetry

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)  # handles all logging levels by default

        # Use the ColoredFormatter instead of the default formatter
        formatter = _ColoredLogRecordFormatter(GENESIS_LOGGER_FOMRAT)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

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

    Example usage: 
        To report only the first and then each 10th ERROR log records with the phrase 
        'Failed to connect to Snowflake, retyring in 1.3 sec..." that are emitted by logger named 'XYZ', do this:
        ```
        LogSupressor.add_supressor('XYZ', log_level=logging.ERROR,  regexp=r'Failed to connect to Snowflake, retyring in .* sec', n=10)
        ```
    """

    # A mapping of logger names to their respective filter specifications.
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
        suppression_note_added = hasattr(record, '_suppression_note_added')
        if suppression_note_added:
            # This is a 'summary' record we've already seen and cleared through.
            return True
        logger_name = record.name
        specs = LogSupressor._modulename_to_filtersspec_map.get(logger_name, [])
        show_record = True
        for spec in specs:
            # If the record matches the spec, then increase the counter and determine if it should be filtered.
            if record.levelno == spec.log_level and re.search(spec.regexp, record.getMessage()):
                spec.counter += 1
                show_record = spec.counter == 1 or spec.counter % spec.n == 1
                if show_record and spec.counter > 1:
                    record.msg += " [NOTE: PREVIOUS {} SIMILAR RECORDS HAVE BEEN SUPPRESSED (regexp={!r})]".format(spec.n-1, spec.regexp)
                    record._suppression_note_added = True  # Mark it as 'visited' and shortcut filtering for next time we see this record.
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


@contextmanager
def log_level_ctx(new_level: int|str):
    """
    A context manager to temporarily set the Genesis logger level to a specified level.

        >>> assert logger.level == logging.INFO
        >>> logger.debug("you should NOT see this")
        >>> with log_level_ctx(logging.DEBUG): # or "DEBUG"
        ...     logger.debug("you should be able to see this")
        >>> assert logger.level == logging.INFO

    Args:
        new_level (int): The temporary log level to set for the Genesis logger.
    """
    global logger
    if isinstance(new_level, str): # convert to the numerinc value
        new_level = logging.getLevelName(new_level.upper())
    original_level = logger.level
    logger.setLevel(new_level)
    try:
        yield
    finally:
        logger.setLevel(original_level)


# Log configruation starts here
#-----------------------------------------
_setup_root_logger()

# create the genesis logger singleton at import time
logger = _setup_genesis_logger()

