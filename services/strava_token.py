import requests
import os
from utils.logger import logger


def generate_token(refresh_token, client_id, client_secret, code):

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
        if os.getenv("ENV", "DEV") == "DEV":
            with open('.env', 'a') as file:
                file.write(
                    f"REFRESH_TOKEN='{token_response_json['refresh_token']}'\n")
        access_token = token_response_json["access_token"]

    return {
        'Authorization': f'Bearer {access_token}'
    }
