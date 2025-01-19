import logging

SIMPLE_FORMATTER = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
TIMED_SIMPLE_FORMATTER = logging.Formatter('%(asctime)s \t- %(name)s - %(levelname)s - %(message)s')

DEFAULT_LOGGER = logging.getLogger('DLog')
handler = logging.StreamHandler()
handler.setFormatter(SIMPLE_FORMATTER)
DEFAULT_LOGGER.addHandler(handler)
DEFAULT_LOGGER.setLevel(logging.INFO)

TIMED_DEFAULT_LOGGER = logging.getLogger('TDLog')
handler = logging.StreamHandler()
handler.setFormatter(TIMED_SIMPLE_FORMATTER)
TIMED_DEFAULT_LOGGER.addHandler(handler)
TIMED_DEFAULT_LOGGER.setLevel(logging.INFO)

del handler
