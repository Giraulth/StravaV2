# logger.py
import logging

logger = logging.getLogger("STRAVA")
logger.setLevel(logging.INFO)

# Handler console
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
