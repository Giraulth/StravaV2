from dotenv import load_dotenv
import os
from prometheus_remote_writer import RemoteWriter
import base64

from utils.logger import logger
from utils.sanitize import hash_sha256
from utils.geoloc import reverse_geocode

from services.strava_helpers import get_activities, get_equipments
from services.strava_token import generate_token

from models.gear import Gear
from models.activity import Activity
from models.activity import Kudos

from redis_db import RedisStore

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
        return RedisStore(f"rediss://default:{UPSTASH_REDIS_REST_TOKEN}@{UPSTASH_REDIS_REST_URL}")
    logger.warning("Not connected to redis DB to save tokens")
    return None


def process_activities(redis_store, headers, run_extract: bool, run_push: bool):
    activities_data = get_activities(headers, run_extract)
    # print(reverse_geocode(activities_data[0]["start_latlng"]))
    sanitized_activities = Activity.from_dicts(activities_data)

    if redis_store and run_push:
        for activity in sanitized_activities:
            if redis_store.get_activity_kudos_count(activity.id) != len(activity.kudoers):
                logger.info(
                    f"Pushing {len(activity.kudoers)} kudos to Redis for activity {hash_sha256(str(activity.id))}"
                )
                for kudos in activity.kudoers:
                    redis_store.sadd_kudos_if_needed(
                        kudos.full_name, activity.id)
                redis_store.set_activity_kudos_count(activity.id, len(activity.kudoers))
            else:
                logger.info(f"No kudos change for activity {hash_sha256(str(activity.id))}")

    return sanitized_activities


def process_gears(redis_store, headers, run_extract: bool):
    raw_gear_data = get_equipments(headers, run_extract)
    sanitized_gears = Gear.from_dicts(raw_gear_data)

    if redis_store:
        for gear in sanitized_gears:
            redis_store.set_distance(gear.type, gear.id, gear.distance)

    return sanitized_gears


def push_metrics(sanitized_gears, sanitized_kudos):
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
    _push(gear_payload, "Gear distance")

    # Push Kudos
    kudos_payload = Kudos.kudos_redis_to_remote_write(sanitized_kudos)
    _push(kudos_payload, "Kudos")


def main(run_extract=True, run_push=True):
    logger.info("Starting Strava → Grafana pipeline")
    redis_store = init_redis(run_push)

    try:
        headers = generate_token(
            refresh_token, client_id, client_secret, code) if run_extract else ""
        sanitized_activities = process_activities(
            redis_store, headers, run_extract, run_push)
        sanitized_gears = process_gears(redis_store, headers, run_extract)
        sanitized_kudos = redis_store.get_kudos()
        push_metrics(sanitized_gears, sanitized_kudos)
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_extract = True
    run_push = True
    if os.getenv("ENV") == "DEV":
        run_extract = True
        run_push = True

    main(run_extract=run_extract, run_push=run_push)
