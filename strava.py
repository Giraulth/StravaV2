import requests
import time
import json
from dotenv import load_dotenv
import os
from prometheus_remote_writer import RemoteWriter
import base64
from datetime import datetime
from utils.logger import logger
from utils.time_utils import TimeUtils
from utils.fixtures import load_fixture

from gear import Gear, build_distance_payload, from_dicts
from redis_db import RedisStore

STRAVA_DEFAULT_URL = 'https://www.strava.com/api/v3'

NB_KILOMETERS = 3

WORKOUT_TYPE_MAPPING = {
    0: 'Aucun',
    1: 'Course',
    3: 'Entraînement',
    2: 'Sortie longue',
    10: 'Aucun',
    11: 'Course',
    12: 'Entraînement'
}

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


def generate_token():

    if refresh_token is not None:

        token_response = requests.post(
            url='https://www.strava.com/oauth/token',
            data={
                'client_id': client_id,
                'client_secret': client_secret,
                'code': code,
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token
            }
        )

        token_response_json = token_response.json()
        access_token = token_response_json["access_token"]
        if access_token:
            logger.info(
                f"Strava token successfully retrieved: {access_token[:4]}***")
        else:
            logger.error("Failed to retrieve Strava token!")

    else:

        token_response = requests.post(
            url='https://www.strava.com/oauth/token',
            data={
                'client_id': client_id,
                'client_secret': client_secret,
                'code': code,
                'grant_type': 'authorization_code'
            }
        )

        token_response_json = token_response.json()
        with open('.env', 'a') as file:
            file.write(
                f"REFRESH_TOKEN='{token_response_json['refresh_token']}'\n")
        access_token = token_response_json["access_token"]

    return {
        'Authorization': f'Bearer {access_token}'
    }


def get_data_from_url(strava_url, headers):
    strava_response = requests.get(strava_url, headers=headers)
    return strava_response.json()


def get_activities(headers):
    last_week_utc = TimeUtils.subtract_days_utc(7)

    activities_data = get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities?after={last_week_utc}', headers)


def get_equipments(headers, run_extract):

    athlete_data = {}
    gear_data = {}
    if run_extract:
        athlete_data = get_data_from_url(
            f'{STRAVA_DEFAULT_URL}/athlete', headers)
    else:
        athlete_data = load_fixture("fixtures/athlete.json")

    equipments = []
    # Count bikes
    num_bikes = len(athlete_data.get('bikes', []))

    # Count shoes
    num_shoes = len(athlete_data.get('shoes', []))
    for gear in ['bikes', 'shoes']:
        for data in athlete_data[gear]:
            gear_id = data['id']
            if run_extract:
                gear_data = get_data_from_url(
                    f'{STRAVA_DEFAULT_URL}/gear/{gear_id}', headers)
            else:
                gear_data = load_fixture(f"fixtures/{gear_id}.json")
            for value in gear_data:
                if gear_data[value] is None:
                    gear_data[value] = ''
            equipments.append(gear_data)
    logger.info(
        f"Retrieve data for {len(equipments)} equipments ({num_bikes} bikes and {num_shoes} shoes)")
    return equipments


# def insert_segments(segment_list):
#     for segment in segment_list:
#         athlete_url = 'https://www.strava.com/api/v3/segments/' + \
#             str(segment['id'])
#         athlete_response = requests.get(athlete_url, headers=headers)
#         segment = athlete_response.json()
#         segment['polyline'] = segment['map']['polyline']
#         segment['kom'] = segment['xoms']['overall']
#         if segment['athlete_segment_stats']['pr_activity_id']:
#             segment['pr_elapsed_time'] = time.strftime("%M:%S", time.gmtime(
#                 segment['athlete_segment_stats']['pr_elapsed_time']))
#             minutes, seconds = segment['pr_elapsed_time'].split(':')

#             # Format time to match kom
#             if int(minutes) == 0:
#                 segment['pr_elapsed_time'] = f"{int(seconds)}s"
#             else:
#                 segment['pr_elapsed_time'] = f"{int(minutes)}:{seconds}"
#             segment['pr_activity_id'] = segment['athlete_segment_stats']['pr_activity_id']
#         else:
#             segment['pr_elapsed_time'] = ''
#             segment['pr_activity_id'] = 0
#         for value in segment:
#             if segment[value] is None:
#                 segment[value] = ''


# def get_segments(strava_url, headers, data_path=''):
#     segment_data = get_data_from_url(strava_url, headers)
#     if data_path:
#         segment_data = segment_data[data_path]
#     insert_segments(segment_data)


def main(run_extract=True, run_transform=True, run_push=True):
    logger.info("Starting Strava → Grafana pipeline")

    redis_store = RedisStore(
        f"rediss://default:{UPSTASH_REDIS_REST_TOKEN}@{UPSTASH_REDIS_REST_URL}")

    try:
        headers = generate_token() if run_extract else ""

        activities_data = get_activities(
            headers) if run_extract else load_fixture("fixtures/activity.json")
        raw_gear_data = get_equipments(
            headers, run_extract) if run_extract else load_fixture("fixtures/athlete.json")

        sanitized_gears = from_dicts(raw_gear_data)

        # REDIS PART
        if run_push:
            for gear in sanitized_gears:
                redis_store.set_distance(gear.type, gear.id, gear.distance)

            # PROMETHEUS/GRAFANA PART
            data = build_distance_payload(sanitized_gears)
            send_result = writer.send(data)

            status = send_result.last_response.status_code
            if status == 200:
                logger.info("Metrics pushed successfully to Prometheus")
            else:
                logger.error(
                    f"Prometheus push failed with status code {status}")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_extract = True
    run_push = True
    if os.getenv("ENV") == "DEV":
        run_extract = False
        run_push = False

    main(run_extract=run_extract, run_push=run_push)
