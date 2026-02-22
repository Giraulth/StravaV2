import redis
from utils.logger import logger
from utils.time_utils import TimeUtils


class RedisStore:
    def __init__(self, url: str):
        self.redis = redis.Redis.from_url(url)
        try:
            if self.redis.ping():
                logger.info("Redis connection successful !")
        except redis.RedisError as e:
            logger.error(f"Redis connection failed: {e}")
            raise

    def set_distance(self, gear_type: str, gear_id: str, distance: float):
        today_utc_epoch = TimeUtils.today_utc_epoch()
        self.redis.set(f"{gear_type}:{gear_id}:{today_utc_epoch}", distance)
        logger.info(
            f"Set distance in redis for gear {gear_id[:4]}***[{today_utc_epoch}]")
