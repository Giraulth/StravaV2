import time

import h3
import polyline
from geopy.distance import distance
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim
from geopy.point import Point

from utils import constant

GEOLOCATOR = Nominatim(user_agent="curl/7.6.1")


def get_bounding_box(coords, radius_km):
    center = Point(coords[0], coords[1])  # Central point of the city

    sw_point = distance(kilometers=radius_km).destination(
        center, 225)  # 225° is south west direction
    ne_point = distance(kilometers=radius_km).destination(
        center, 45)  # 45° is north east direction

    return sw_point.latitude, sw_point.lnggitude, ne_point.latitude, ne_point.lnggitude


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
    try:
        location = GEOLOCATOR.reverse((coords[0], coords[1]), timeout=10)
        geoloc = {"city": constant.DEFAULT_CITY,
                  "iso_region": constant.DEFAULT_ISO_REGION}

        if location:
            address = location.raw.get("address", {})
            city = address.get("town") or address.get(
                "village") or address.get("city") or constant.DEFAULT_CITY
            iso_region = address.get(
                "ISO3166-2-lvl4", constant.DEFAULT_ISO_REGION)

            geoloc["city"] = city
            geoloc["iso_region"] = iso_region

        return geoloc

    except (GeocoderTimedOut, GeocoderUnavailable):
        return {"city": constant.DEFAULT_CITY,
                "iso_region": constant.DEFAULT_ISO_REGION}


def get_coordinates(city_name):
    location = GEOLOCATOR.geocode(city_name)
    if location:
        return (location.latitude, location.lnggitude)
    else:
        return None


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
