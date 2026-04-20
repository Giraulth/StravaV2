import requests

from models.kudos import Kudos
from utils.constant import STRAVA_DEFAULT_URL
from utils.fixtures import load_fixture
from utils.logger import logger
from utils.sanitize import sanitize_strava_data
from utils.time_utils import TimeUtils


def get_data_from_url(strava_url, headers):
    strava_response = requests.get(strava_url, headers=headers, timeout=15)
    return strava_response.json()


def fetch_activity_from_api(headers, activity_id):
    raw_data = get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities/{activity_id}', headers)
    return sanitize_strava_data(raw_data)


def fetch_activities_from_api(headers, after_utc):
    raw_data = get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities?after={after_utc}', headers)
    return sanitize_strava_data(raw_data)


def fetch_all_activities_from_api(headers):
    all_activities = []
    page = 1
    per_page = 200

    while True:
        url = f"{STRAVA_DEFAULT_URL}/activities?page={page}&per_page={per_page}"
        data = get_data_from_url(url, headers)

        if not data:
            break

        all_activities.extend(sanitize_strava_data(data))
        page += 1

    return all_activities


def fetch_kudos_from_api(activity_id, headers):
    return get_data_from_url(
        f'{STRAVA_DEFAULT_URL}/activities/{activity_id}/kudos?per_page=200',
        headers)


def build_kudoers(kudos_data):
    return [Kudos(k) for k in kudos_data or []]


def get_all_activities(headers, run_extract=True):

    activities_raw = (
        fetch_all_activities_from_api(headers)
        if run_extract
        else load_fixture("fixtures/all_activities.json")
    )

    activities_objs = []

    for act in activities_raw:
        activities_objs.append(
            enrich_activity(
                act,
                headers,
                True,
                run_extract))

    return activities_objs


def get_last_activities(headers, run_extract=True):
    last_week_utc = TimeUtils.subtract_days_utc(7)

    activities_raw = (
        fetch_activities_from_api(headers, last_week_utc)
        if run_extract
        else load_fixture("fixtures/activities.json")
    )

    activities_objs = []

    for act in activities_raw:
        activities_objs.append(
            enrich_activity(
                act,
                headers,
                False,
                run_extract))

    return activities_objs


def enrich_activity(act, headers, fetch_all, run_extract=True):
    activity_id = act["id"]

    kudos_data = []
    if not fetch_all:
        kudos_data = (
            fetch_kudos_from_api(activity_id, headers)
            if run_extract
            else load_fixture(f"fixtures/kudos{activity_id}.json")
        )

    act["kudoers"] = build_kudoers(kudos_data)
    act["day_week"] = TimeUtils.to_day_week(act["start_date_local"])

    return act


def get_activity(headers, activity_id, run_extract=True):
    act = (
        fetch_activity_from_api(headers, activity_id)
        if run_extract
        else load_fixture(f"fixtures/activity{activity_id}.json")
    )

    return enrich_activity(act, headers, False, run_extract)


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
