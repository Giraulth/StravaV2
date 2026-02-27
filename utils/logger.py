import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("STRAVA")

env = os.getenv("ENV", "PROD").upper()


if env == "DEV":
    log_level_str = os.getenv("LOG_LEVEL", "DEBUG").upper()
else:
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logger.setLevel(log_level)

# Handler console
ch = logging.StreamHandler()
ch.setLevel(log_level)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.debug(f"Logger initialized in {env} mode with level {log_level_str}")