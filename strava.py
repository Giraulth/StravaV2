import requests
import time
from clickhouse_driver import Client
from dotenv import load_dotenv
import geoloc

import os

client = Client('localhost')

STRAVA_DEFAULT_URL = 'https://www.strava.com/api/v3'

NB_KILOMETERS = 3

BIKE_MAPPING = {
    1: 'VTT',
    2: 'Vélo de cyclo-cross',
    3: 'Vélo de route',
    4: 'Vélo de contre-la-montre',
    5: 'Vélo Gravel'
}

WORKOUT_TYPE_MAPPING = {
    0 : 'Aucun',
    1 : 'Course',
    3 : 'Entraînement',
    2 : 'Sortie longue',
    10 : 'Aucun',
    11 : 'Course',
    12 : 'Entraînement'
}


load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
code = os.getenv('CODE')
refresh_token = os.getenv('REFRESH_TOKEN')
address = os.getenv('ADDRESS')
get_equipement = os.getenv('GET_EQUIPEMENT')
get_starred = os.getenv('GET_STARRED')
explore = os.getenv('EXPLORE')
access_token = ""

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
            file.write(f"REFRESH_TOKEN='{token_response_json['refresh_token']}'\n")
        access_token = token_response_json["access_token"]

    return {
        'Authorization': f'Bearer {access_token}'
    }

def get_data_from_url(strava_url, headers):
    strava_response = requests.get(strava_url, headers=headers)
    return strava_response.json()


def get_equipements(headers):

    athlete_data = get_data_from_url(f'{STRAVA_DEFAULT_URL}/athlete', headers)
    for gear in ['bikes', 'shoes']:
        for bike in athlete_data[gear]:
            bike_id = bike['id']
            gear_data = get_data_from_url(f'{STRAVA_DEFAULT_URL}/gear/{bike_id}', headers)
            for value in gear_data:
                if gear_data[value] is None:
                    gear_data[value] = ''

            if gear == 'shoes':
                gear_data['frame_type'] = 0
                gear_data['weight'] = 0
                gear_data['frame_type'] = 'Chaussure'
            elif gear == 'bikes':
                gear_data['frame_type'] = BIKE_MAPPING.get(
                    gear_data['frame_type'], '')
            client.execute(
                'INSERT INTO equipement(id, converted_distance, brand_name, model_name, name, frame_type, description, weight) VALUES',
                [gear_data])

def insert_segments(segment_list):
    for segment in segment_list:
        athlete_url = 'https://www.strava.com/api/v3/segments/' + str(segment['id'])
        athlete_response = requests.get(athlete_url, headers=headers)
        segment = athlete_response.json()
        print(segment)
        segment['polyline'] = segment['map']['polyline']
        segment['kom'] = segment['xoms']['overall']
        if segment['athlete_segment_stats']['pr_activity_id']:
            segment['pr_elapsed_time'] = time.strftime("%M:%S", time.gmtime(segment['athlete_segment_stats']['pr_elapsed_time']))
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
        client.execute('INSERT INTO segment(id, activity_type, name, distance, elevation_profile, effort_count, polyline, kom, city, state, country, pr_elapsed_time, pr_activity_id, start_latlng, end_latlng) VALUES', [segment])


def get_segments(strava_url, headers, data_path=''):
    segment_data = get_data_from_url(strava_url, headers)
    if data_path:
        segment_data = segment_data[data_path]
    insert_segments(segment_data)

headers = generate_token()
if get_equipement == "1":
    get_equipements(headers)

if get_starred == "1":
    get_segments(f'{STRAVA_DEFAULT_URL}/segments/starred', headers)

if explore == "1":
    address_coordonates = geoloc.get_coordinates(address)
    square_coords = geoloc.get_bounding_box(address_coordonates, NB_KILOMETERS)
    get_segments(f'https://www.strava.com/api/v3/segments/explore?bounds={square_coords[0]}, {square_coords[1]}, '
                 f'{square_coords[2]}, {square_coords[3]}&activity_type=running', headers, 'segments')