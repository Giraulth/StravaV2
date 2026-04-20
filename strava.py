import base64
import json
import os

from dotenv import load_dotenv
from prometheus_remote_writer import RemoteWriter

from models.activity import Activity, Kudos
from models.gear import Gear
from redis_db import RedisStore
from services.strava_helpers import (
    get_activity,
    get_all_activities,
    get_equipments,
    get_last_activities,
)
from services.strava_token import generate_token
from utils.geoloc import gps_to_remote_write, h3_to_latlng, retrieve_geoloc
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

AGG_KEY = ["city", "iso_region", "day_week", "gear_id"]

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


def reprocess_activity_from_env(
        redis_store,
        headers,
        updates_path="fixtures/activity_update.json",
        confirm=False):
    activity_id = os.getenv("ACTIVITY_ID")
    if not activity_id:
        return None

    if not confirm:
        logger.warning(
            f"Activity {activity_id} will not be reprocessed because `confirm=False`"
        )
        return None

    act = get_activity(headers, activity_id)
    act = retrieve_geoloc([act])[0]

    try:
        with open(updates_path, encoding="utf-8") as f:
            updates = json.load(f)
    except FileNotFoundError:
        logger.warning(
            "No updates file found, skipping updates")
        updates = {}

    act.update(updates)
    activity_obj = Activity.from_dicts([act])[0]

    redis_store.redis.delete(f"activity:{activity_obj.id}")
    process_activity(redis_store, activity_obj)

    logger.info(f"Activity {activity_id} reprocessed successfully")
    return activity_obj


def process_activity(redis_store, activity):
    activity_hash = hash_sha256(str(activity.id))

    pushed = False
    kudos_updated = False

    try:
        logger.debug(
            f"Process activity {activity_hash} with {len(activity.kudoers)} kudos: "
            f"{[k.full_name for k in activity.kudoers]}")

        activity_key = f"activity:{activity.id}"

        if not redis_store.redis.exists(activity_key):
            for agg_key in AGG_KEY:
                logger.debug(f"Aggregate activity using key {agg_key}")
                ret = redis_store.aggregate_activity_by_key(activity, agg_key)
                if not ret:
                    break

            logger.info(
                f"Add {len(activity.gps_coords)} h3 hexagon for activity {activity_hash}")
            redis_store.hincrby_global_h3(
                activity.gps_coords, activity.type.lower())
            pushed = True
        else:
            logger.debug(f"Activity {activity_hash} already aggregated")

        current_kudos_count = redis_store.get_activity_kudos_count(activity.id)

        if current_kudos_count != len(activity.kudoers):
            for kudos in activity.kudoers:
                redis_store.sadd_kudos_if_needed(
                    kudos.full_name, str(activity.id))

            redis_store.set_activity_kudos_count(
                activity.id, len(activity.kudoers))
            kudos_updated = True
        else:
            logger.debug(f"No kudos change for activity {activity_hash}")

    except Exception as e:
        logger.error(f"Failed to process activity {activity_hash}: {e}")
        raise

    return pushed, kudos_updated


def process_activities(
        redis_store,
        headers,
        fetch_all: bool,
        run_extract: bool,
        run_push: bool):
    if fetch_all:
        activities_data = get_all_activities(headers, run_extract)
    else:
        activities_data = get_last_activities(headers, run_extract)
    logger.info(f"Retrieved geoloc for {len(activities_data)} activities")
    activities_data = retrieve_geoloc(activities_data)
    sanitized_activities = Activity.from_dicts(activities_data)

    if not (redis_store and run_push):
        return sanitized_activities

    pushed_count = 0
    no_kudos_count = 0
    updated_kudos_count = 0

    for activity in sanitized_activities:
        pushed, updated = process_activity(redis_store, activity)

        if pushed:
            pushed_count += 1
        if updated:
            updated_kudos_count += 1
        else:
            no_kudos_count += 1

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


def push_metrics(
        sanitized_gears,
        sanitized_kudos,
        sanitized_gps,
        aggregations: dict[str, dict]):
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

    # Push GPS coords
    gps_payload = gps_to_remote_write(sanitized_gps)
    if run_push:
        _push(gps_payload, "GPS coords")

    # Aggregation Activities
    for dimension_name, agg_dict in aggregations.items():
        if not agg_dict:
            continue

        payload = Activity.agg_hashes_to_remote_write(
            agg_dict,
            label_name=dimension_name
        )

        if run_push:
            _push(payload, f"Aggregation {dimension_name}")


def main(run_extract=True, run_push=True, fetch_all=False):
    logger.info("Starting Strava → Grafana pipeline")
    redis_store = init_redis(run_push)

    try:
        headers = generate_token(
            refresh_token,
            client_id,
            client_secret,
            code) if run_extract else ""
        activity_id = os.getenv("ACTIVITY_ID")
        if activity_id is not None:
            reprocess_activity_from_env(
                redis_store, headers, f"fixtures/{activity_id}.json")
        else:
            _ = process_activities(
                redis_store, headers, fetch_all, run_extract, run_push)
        sanitized_gears = process_gears(redis_store, headers, run_extract)
        sanitized_kudos = redis_store.get_kudos() if redis_store else {}
        sanitized_gps = h3_to_latlng(
            redis_store.get_all_h3()) if redis_store else {}
        aggregations = redis_store.get_sanitized_aggs(
            AGG_KEY) if redis_store else {}

        push_metrics(
            sanitized_gears,
            sanitized_kudos,
            sanitized_gps,
            aggregations
        )
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_extract = True
    run_push = True
    fetch_all = False
    if os.getenv("ENV") == "DEV":
        run_extract = "RUN_EXTRACT" in os.environ
        run_push = "RUN_PUSH" in os.environ
        fetch_all = "FETCH_ALL" in os.environ

    main(run_extract=run_extract, run_push=run_push, fetch_all=fetch_all)
