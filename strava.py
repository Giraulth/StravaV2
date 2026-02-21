import requests
import time
from dotenv import load_dotenv
import os
from prometheus_remote_writer import RemoteWriter
import base64

STRAVA_DEFAULT_URL = 'https://www.strava.com/api/v3'

NB_KILOMETERS = 3
BIKE_MAPPING = {
    1: 'mtb',              # VTT
    2: 'cyclocross',       # Vélo de cyclo-cross
    3: 'road_bike',        # Vélo de route
    4: 'time_trial',       # Vélo de contre-la-montre
    5: 'gravel_bike'       # Vélo Gravel
}

WORKOUT_TYPE_MAPPING = {
    0: 'Aucun',
    1: 'Course',
    3: 'Entraînement',
    2: 'Sortie longue',
    10: 'Aucun',
    11: 'Course',
    12: 'Entraînement'
}


if os.getenv("ENV", "dev") == "dev":
    from dotenv import load_dotenv
    load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
CODE = os.getenv("CODE")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
GRAFANA_URL = os.getenv("GRAFANA_PROM_URL")
GRAFANA_USER = os.getenv("GRAFANA_USER_ID")
GRAFANA_KEY = os.getenv("GRAFANA_API_KEY")
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


def get_equipements(headers):

    athlete_data = get_data_from_url(f'{STRAVA_DEFAULT_URL}/athlete', headers)
    equipements = []
    for gear in ['bikes', 'shoes']:
        for data in athlete_data[gear]:
            bike_id = data['id']
            gear_data = get_data_from_url(
                f'{STRAVA_DEFAULT_URL}/gear/{bike_id}', headers)
            for value in gear_data:
                if gear_data[value] is None:
                    gear_data[value] = ''

            if gear == 'shoes':
                gear_data['frame_type'] = 0
                gear_data['weight'] = 0
                gear_data['frame_type'] = 'shoes'
            elif gear == 'bikes':
                gear_data['frame_type'] = BIKE_MAPPING.get(
                    gear_data['frame_type'], '')
            for data_to_convert in ['brand_name', 'model_name']:
                gear_data[data_to_convert] = gear_data[data_to_convert].replace(
                    ' ', '_')
            equipements.append(gear_data)
    return equipements


def build_distance_traveled_data(equipments):
    data = []
    timestamp = int(time.time() * 1000)

    for eq in equipments:
        metric_dict = {
            "__name__": "distance_traveled",
            "type": eq["frame_type"],
            "brand": eq["brand_name"],
            "model": eq["model_name"],
            "weight": str(eq["weight"])
        }

        entry = {
            "metric": metric_dict,
            "values": [eq["converted_distance"]],
            "timestamps": [timestamp]
        }
        data.append(entry)

    return data


def insert_segments(segment_list):
    for segment in segment_list:
        athlete_url = 'https://www.strava.com/api/v3/segments/' + \
            str(segment['id'])
        athlete_response = requests.get(athlete_url, headers=headers)
        segment = athlete_response.json()
        segment['polyline'] = segment['map']['polyline']
        segment['kom'] = segment['xoms']['overall']
        if segment['athlete_segment_stats']['pr_activity_id']:
            segment['pr_elapsed_time'] = time.strftime("%M:%S", time.gmtime(
                segment['athlete_segment_stats']['pr_elapsed_time']))
            minutes, seconds = segment['pr_elapsed_time'].split(':')

            # Format time to match kom
            if int(minutes) == 0:
                segment['pr_elapsed_time'] = f"{int(seconds)}s"
            else:
                segment['pr_elapsed_time'] = f"{int(minutes)}:{seconds}"
            segment['pr_activity_id'] = segment['athlete_segment_stats']['pr_activity_id']
        else:
            segment['pr_elapsed_time'] = ''
            segment['pr_activity_id'] = 0
        for value in segment:
            if segment[value] is None:
                segment[value] = ''


def get_segments(strava_url, headers, data_path=''):
    segment_data = get_data_from_url(strava_url, headers)
    if data_path:
        segment_data = segment_data[data_path]
    insert_segments(segment_data)


def main():
    headers = generate_token()
    equipments = get_equipements(headers)
    data = build_distance_traveled_data(equipments)
    writer.send(data)
    print("Metrics pushed successfully!")


if __name__ == "__main__":
    main()
