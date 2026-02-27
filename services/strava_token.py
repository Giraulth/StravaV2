import os

import requests

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
            },
            timeout=5
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
            },
            timeout=5
        )

        token_response_json = token_response.json()
        if os.getenv("ENV", "DEV") == "DEV":
            with open('.env', 'a') as file:
                file.write(
                    f"REFRESH_TOKEN='{token_response_json['refresh_token']}'\n")
        access_token = token_response_json["access_token"]

    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    get_strava_quota_status(headers=headers)
    return headers


def get_strava_quota_status(headers):
    url = "https://www.strava.com/api/v3/athlete"

    response = requests.get(url, headers=headers, timeout=5)

    if response.status_code != 200:
        logger.error(
            f"Failed to fetch rate limit: {response.status_code} {response.text}")

    limit_header = response.headers.get("X-RateLimit-Limit", "0,0")
    usage_header = response.headers.get("X-RateLimit-Usage", "0,0")
    try:
        app_quota, user_quota = map(int, limit_header.split(","))
        app_used, user_used = map(int, usage_header.split(","))
        app_remaining = app_quota - app_used
        user_remaining = user_quota - user_used

        logger.info(
            f"[STRAVA-API] App quota: {app_used}/{app_quota} used, "
            f"User quota: {user_used}/{user_quota} used | "
            f"Remaining → App: {app_remaining}, User: {user_remaining}"
        )
    except Exception as e:
        logger.warning(
            f"[STRAVA-API] Failed to parse rate limit headers: "
            f"Limit={limit_header}, Usage={usage_header} | Error: {e}"
        )
