import redis
from utils.logger import logger
from utils.time_utils import TimeUtils
from utils.sanitize import hash_sha256


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

        base_key = f"{gear_type}:{gear_id}"
        history_key = f"{base_key}:{today_utc_epoch}"
        last_distance_key = f"{base_key}:last_distance"
        last_date_key = f"{base_key}:last_date"

        last_distance = self.redis.get(last_distance_key)

        if last_distance is None or float(last_distance) != distance:
            pipe = self.redis.pipeline()

            pipe.set(history_key, distance)
            pipe.set(last_distance_key, distance)
            pipe.set(last_date_key, today_utc_epoch)

            pipe.execute()

            logger.info(
                f"Distance changed → history saved for {gear_id[:4]}*** "
                f"[km={distance}][date={today_utc_epoch}]"
            )
        else:
            logger.info(
                f"No distance change for {gear_id[:4]}*** "
                f"(km={distance}) → nothing written"
            )

    def sadd_kudos_if_needed(self, username: str, activity_id: str):
        key = f"kudos:{username}"
        added = self.redis.sadd(key, activity_id)

        if added == 1:
            logger.debug(
                f"Kudos sent for {hash_sha256(username)} → {activity_id}")
        else:
            logger.debug(
                f"Kudos already sent for {hash_sha256(username)} → {activity_id}")

    def get_kudos(self):
        cursor = 0
        result = {}

        while True:
            cursor, keys = self.redis.scan(
                cursor=cursor, match="kudos:*", count=100)

            for key in keys:
                # redis-py renvoie des bytes si decode_responses=False
                key_str = key.decode() if isinstance(key, bytes) else key

                set_size = self.redis.scard(key)
                result[key_str] = set_size

            if cursor == 0:
                break

        return result
