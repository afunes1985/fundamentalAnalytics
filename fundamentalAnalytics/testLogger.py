import logging

from colorlog import ColoredFormatter


logging.basicConfig(level=logging.DEBUG)
fmt = ("%(levelname)s "
           "%(message)s")
colorfmt = "%(log_color)s{}%(reset)s".format(fmt)
datefmt = '%Y-%m-%d %H:%M:%S'
logging.getLogger().handlers[0].setFormatter(ColoredFormatter(
    colorfmt,
    datefmt=datefmt,
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red',
    }
))
logging.info("info")
logging.debug("debug")