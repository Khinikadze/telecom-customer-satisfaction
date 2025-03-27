import logging
from termcolor import colored

LOG_COLORS = {
    "DEBUG": "blue",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "magenta"
}

class ColoredLevelFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt=fmt, datefmt=datefmt)

    def format(self, record):
        message = super().format(record)
        
        levelname = record.levelname
        if levelname in LOG_COLORS:
            colored_level = colored(levelname, LOG_COLORS[levelname])
            message = message.replace(levelname, colored_level)
        
        return message

def setup_logging(name="beautiful_logger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = ColoredLevelFormatter(
        '[%(asctime)s %(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger