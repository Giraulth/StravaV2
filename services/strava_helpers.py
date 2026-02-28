import requests

from models.kudos import Kudos
from utils.constant import STRAVA_DEFAULT_URL
from utils.fixtures import load_fixture
from utils.logger import logger
from utils.sanitize import sanitize_strava_data
from utils.time_utils import TimeUtils


def get_data_from_url(strava_url, headers):
    strava_response = requests.get(strava_url, headers=headers, timeout=5)
    return strava_response.json()


def fetch_activities_from_api(headers, after_utc):
    raw_data = get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities?after={after_utc}', headers)
    return sanitize_strava_data(raw_data)


def fetch_kudos_from_api(activity_id, headers):
    return get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities/{activity_id}/kudos', headers)


def build_kudoers(kudos_data):
    return [Kudos(k) for k in kudos_data or []]


def get_activities(headers, run_extract=True):
    last_week_utc = TimeUtils.subtract_days_utc(7)
    activities_raw = fetch_activities_from_api(
        headers,
        last_week_utc) if run_extract else load_fixture("fixtures/activities.json")
    activities_objs = []

    for act in activities_raw:
        kudos_data = fetch_kudos_from_api(
            act.get("id"),
            headers) if run_extract else load_fixture(f"fixtures/kudos{act.get('id')}.json")
        act["kudoers"] = build_kudoers(kudos_data)
        act["day_week"] = TimeUtils.to_day_week(act["start_date_local"])
        activities_objs.append(act)

    return activities_objs


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

            gear_data = sanitize_strava_data(gear_data)
            equipments.append(gear_data)
    logger.info(
        f"Retrieve data for {len(equipments)} equipments ({num_bikes} bikes and {num_shoes} shoes)")
    return equipments
