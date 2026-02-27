import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("STRAVA")

env = os.getenv("ENV", "PROD")
logger.setLevel(logging.DEBUG if env.upper() == "DEV" else logging.INFO)

# Handler console
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG if env.upper() == "DEV" else logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(funcName)s] %(message)s"
)
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.debug(f"Logger initialized in {env} mode")
