import base64
import os

from dotenv import load_dotenv
from prometheus_remote_writer import RemoteWriter

from models.activity import Activity, Kudos
from models.gear import Gear
from redis_db import RedisStore
from services.strava_helpers import get_activities, get_equipments
from services.strava_token import generate_token
from utils.geoloc import retrieve_geoloc
from utils.logger import logger
from utils.sanitize import hash_sha256

if os.getenv("ENV", "DEV") == "DEV":
    load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
code = os.getenv('CODE')
refresh_token = os.getenv('REFRESH_TOKEN')
GRAFANA_URL = os.getenv("GRAFANA_PROM_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER_ID")
GRAFANA_KEY = os.getenv("GRAFANA_API_KEY")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

access_token = ""

token = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_KEY}".encode()).decode()
# Create a RemoteWriter instance
writer = RemoteWriter(
    url=GRAFANA_URL,
    headers={"Authorization": f"Basic {token}"}
)


def init_redis(run_push: bool):
    if run_push:
        redis_store = RedisStore(
            f"rediss://default:{UPSTASH_REDIS_REST_TOKEN}@{UPSTASH_REDIS_REST_URL}")
        redis_store.get_redis_quota()
        return redis_store
    logger.warning("Not connected to redis DB to save tokens")
    return None


def process_activities(
        redis_store,
        headers,
        run_extract: bool,
        run_push: bool):
    activities_data = get_activities(headers, run_extract)
    logger.info(f"Retrieved geoloc for {len(activities_data)} activities")
    activities_data = retrieve_geoloc(activities_data)
    sanitized_activities = Activity.from_dicts(activities_data)

    if not (redis_store and run_push):
        return sanitized_activities

    pushed_count = 0
    no_kudos_count = 0
    updated_kudos_count = 0

    def push_activity(activity):
        nonlocal pushed_count, no_kudos_count, updated_kudos_count
        activity_hash = hash_sha256(str(activity.id))
        try:
            logger.debug(
                f"Process activity {activity_hash} with {len(activity.kudoers)} kudos: "
                f"{[k.full_name for k in activity.kudoers]}")

            activity_key = f"activity:{str(activity.id)}"
            if not redis_store.redis.exists(activity_key):
                for agg_key in ["city", "iso_region", "day_week", "gear_id"]:
                    logger.debug(f"Aggregate activity using key {agg_key}")
                    redis_store.aggregate_activity_by_key(activity, agg_key)

                pushed_count += 1
            else:
                logger.debug(f"Activity {activity_hash} is already aggregated")

            current_kudos_count = redis_store.get_activity_kudos_count(
                activity.id)
            if current_kudos_count != len(activity.kudoers):
                updated_kudos_count += 1
                for kudos in activity.kudoers:
                    redis_store.sadd_kudos_if_needed(
                        kudos.full_name, str(activity.id))
                redis_store.set_activity_kudos_count(
                    activity.id, len(activity.kudoers))
            else:
                no_kudos_count += 1
                logger.debug(f"No kudos change for activity {activity_hash}")

        except Exception as e:
            logger.error(
                f"Failed to push activity {activity_hash} to Redis: {e}")

    for activity in sanitized_activities:
        push_activity(activity)

    logger.info(
        f"Pushed {pushed_count} activities to Redis "
        f"({updated_kudos_count} had kudos updates, {no_kudos_count} no change)")

    return sanitized_activities


def process_gears(redis_store, headers, run_extract: bool):
    raw_gear_data = get_equipments(headers, run_extract)
    sanitized_gears = Gear.from_dicts(raw_gear_data)

    if redis_store:
        for gear in sanitized_gears:
            redis_store.set_distance(gear.type, gear.id, gear.distance)

    return sanitized_gears


def push_metrics(sanitized_gears, sanitized_kudos, sanitized_cities):
    def _push(data, metric_name: str):
        send_result = writer.send(data)
        status = send_result.last_response.status_code
        if status == 200:
            logger.info(
                f"{metric_name} metrics pushed successfully to Prometheus")
        else:
            logger.error(
                f"{metric_name} metrics push failed with status code {status}")

    # Push Gear distance
    gear_payload = Gear.build_distance_payload(sanitized_gears)
    if run_push:
        _push(gear_payload, "Gear distance")

    # Push Kudos
    kudos_payload = Kudos.kudos_redis_to_remote_write(sanitized_kudos)
    if run_push:
        _push(kudos_payload, "Kudos")

    cities_payload = Activity.agg_hashes_to_remote_write(sanitized_cities)
    if run_push:
        _push(cities_payload, "Cities")


def main(run_extract=True, run_push=True):
    logger.info("Starting Strava → Grafana pipeline")
    redis_store = init_redis(run_push)

    try:
        headers = generate_token(
            refresh_token,
            client_id,
            client_secret,
            code) if run_extract else ""
        _ = process_activities(
            redis_store, headers, run_extract, run_push)
        sanitized_gears = process_gears(redis_store, headers, run_extract)
        sanitized_kudos = redis_store.get_kudos() if redis_store else {}
        sanitized_cities = redis_store.get_agg_object(
            "agg:city:*") if redis_store else {}
        push_metrics(sanitized_gears, sanitized_kudos, sanitized_cities)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_extract = True
    run_push = True
    if os.getenv("ENV") == "DEV":
        run_extract = "RUN_EXTRACT" in os.environ
        run_push = "RUN_PUSH" in os.environ

    main(run_extract=run_extract, run_push=run_push)
