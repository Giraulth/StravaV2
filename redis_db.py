
import redis

from models.activity import Activity
from utils.logger import logger
from utils.sanitize import decode_redis_hash, hash_sha256
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

            logger.debug(
                f"Distance changed → history saved for {gear_id[:4]}*** "
                f"[km={distance}][date={today_utc_epoch}]"
            )
        else:
            logger.debug(
                f"No distance change for {gear_id[:4]}*** "
                f"(km={distance}) → nothing written"
            )

    def set_activity_kudos_count(self, activity_id: str, nb_kudos: int):
        key = f"kudos_count:{activity_id}"
        self.redis.set(key, nb_kudos)

    def get_activity_kudos_count(self, activity_id: str) -> int:
        key = f"kudos_count:{activity_id}"
        value = self.redis.get(key)
        return int(value) if value else 0

    def sadd_kudos_if_needed(self, username: str, activity_id: str):
        key = f"kudos:{username}"
        added = self.redis.sadd(key, activity_id)

        if added == 1:
            logger.debug(
                f"Kudos sent for {hash_sha256(username)} → {activity_id}")
        else:
            logger.debug(
                f"Kudos already sent for {hash_sha256(username)} → {activity_id}")

        return added

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

    def aggregate_activity_by_city(self, activity: Activity):
        city = activity.city or "UNK_CITY"
        key = f"agg:city:{city}"

        pipe = self.redis.pipeline()
        pipe.hgetall(key)
        raw_existing = pipe.execute()[0]

        existing = decode_redis_hash(raw_existing)

        total_activity = int(existing.get("total_activity", 0))
        avg_speed = float(existing.get("average_speed", 0))
        avg_hr = float(existing.get("average_heartrate", 0))
        max_speed = float(existing.get("max_speed", 0))
        max_hr = float(existing.get("max_heartrate_max", 0))
        total_distance = float(existing.get("total_distance", 0))
        total_elapsed = int(existing.get("total_elapsed", 0))
        total_elevation = float(existing.get("total_elevation_gain", 0))
        total_achievement = int(existing.get("total_achievement", 0))
        total_comments = int(existing.get("total_comment_count", 0))
        total_kudos = int(existing.get("total_kudos_count", 0))

        new_count = total_activity + 1
        if total_activity == 0:
            new_avg_speed = activity.average_speed
            new_avg_hr = activity.average_heartrate
        else:
            new_avg_speed = (avg_speed * total_activity +
                             activity.average_speed) / new_count
            new_avg_hr = (avg_hr * total_activity +
                          activity.average_heartrate) / new_count
        new_max_speed = max(max_speed, activity.max_speed)
        new_max_hr = max(max_hr, activity.max_hearthrate)
        new_total_distance = total_distance + activity.distance
        new_total_elapsed = total_elapsed + activity.elapsed_time
        new_total_elevation = total_elevation + activity.total_elevation_gain
        new_total_achievement = total_achievement + \
            (activity.achievement_count or 0)
        new_total_comments = total_comments + (activity.comment_count or 0)
        new_total_kudos = total_kudos + (activity.kudos_count or 0)

        pipe = self.redis.pipeline()
        pipe.hset(key, mapping={
            "total_activity": new_count,
            "average_speed": new_avg_speed,
            "average_heartrate": new_avg_hr,
            "max_speed": new_max_speed,
            "max_heartrate_max": new_max_hr,
            "total_distance": new_total_distance,
            "total_elapsed": new_total_elapsed,
            "total_elevation_gain": new_total_elevation,
            "total_achievement": new_total_achievement,
            "total_comment_count": new_total_comments,
            "total_kudos_count": new_total_kudos
        })
        pipe.execute()

        logger.debug(
            "Aggregated into city=%s {total_activity=%d, "
            "avg_speed=%.2f, max_speed=%.2f, total_distance=%.1f, "
            "total_elapsed=%d, total_elevation=%.1f, "
            "achievements=%d, comments=%d, kudos=%d}",
            city,
            new_count,
            new_avg_speed,
            new_max_speed,
            new_total_distance,
            new_total_elapsed,
            new_total_elevation,
            new_total_achievement,
            new_total_comments,
            new_total_kudos
        )

        self.redis.set(f"activity:{activity.id}", "")
