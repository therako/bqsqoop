import logging


def setup(debug_mode):
    """Streams logs to stdout

    Args:
        debug_mode (bool): a boolean to enable verbose logs
    """
    logFormatter = logging.Formatter(
        "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    if(debug_mode):
        rootLogger = logging.getLogger()
        rootLogger.setLevel(logging.DEBUG)
        logging.debug("Verbose logging enabled")
