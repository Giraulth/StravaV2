import time

import h3
import polyline
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim

from utils import constant

GEOLOCATOR = Nominatim(user_agent="Strava-App-V2")
GEOCODE = RateLimiter(GEOLOCATOR.reverse, min_delay_seconds=1)
cache = {}


def retrieve_geoloc(activities_data):
    for activity in activities_data:
        if len(activity["start_latlng"]) == 2:
            geoloc_infos = reverse_geocode(activity["start_latlng"])
            activity["iso_region"] = geoloc_infos["iso_region"]
            activity["city"] = geoloc_infos["city"]
        activity["gps_coords"] = reduce_polyline(
            activity["map"]["summary_polyline"], 5, 8)
    return activities_data


def reverse_geocode(coords):
    # Set key using h3 to increase cache HIT rate
    key = h3.latlng_to_cell(coords[0], coords[1], 9)

    # CACHE HIT
    if key in cache:
        return cache[key]

    default = {
        "city": constant.DEFAULT_CITY,
        "iso_region": constant.DEFAULT_ISO_REGION,
    }

    try:
        location = GEOCODE(coords, timeout=10)

        if not location or not getattr(location, "raw", None):
            cache[key] = default
            return default

        address = location.raw.get("address", {})

        result = {
            "city": (
                address.get("town")
                or address.get("village")
                or address.get("city")
                or constant.DEFAULT_CITY
            ),
            "iso_region": address.get(
                "ISO3166-2-lvl4",
                constant.DEFAULT_ISO_REGION,
            ),
        }

    except (GeocoderTimedOut, GeocoderUnavailable):
        result = default

    cache[key] = result
    return result


def reduce_polyline(
    encoded_polyline: str,
    precision: int = 5,
    h3_resolution: int = 9,
):

    coords = polyline.decode(encoded_polyline, precision=precision)

    h3_cells = {
        h3.latlng_to_cell(lat, lng, h3_resolution)
        for lat, lng in coords
    }

    return h3_cells


def h3_to_latlng(h3_data: dict[str, dict[str, int]]) -> list[dict]:
    result = []

    for activity_type, cells in h3_data.items():
        for cell, count in cells.items():
            lat, lng = h3.cell_to_latlng(cell)

            result.append({
                "cell": cell,
                "lat": lat,
                "lng": lng,
                "count": count,
                "type": activity_type
            })

    return result


def gps_to_remote_write(h3_data: list[dict]):
    series = []
    timestamp_ms = int(time.time() * 1000)

    for item in h3_data:
        series.append({
            "metric": {
                "__name__": "h3_cell",
                "cell": item["cell"],
                "activity_type": item["type"],
                "lat": str(item["lat"]),
                "lng": str(item["lng"]),
            },
            "values": [item["count"]],
            "timestamps": [timestamp_ms],
        })

    return series
