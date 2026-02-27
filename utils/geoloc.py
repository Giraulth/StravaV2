from geopy.distance import distance
from geopy.geocoders import Nominatim
from geopy.point import Point

GEOLOCATOR = Nominatim(user_agent="curl/7.6.1")


def get_bounding_box(coords, radius_km):
    center = Point(coords[0], coords[1])  # Central point of the city

    sw_point = distance(kilometers=radius_km).destination(
        center, 225)  # 225° is south west direction
    ne_point = distance(kilometers=radius_km).destination(
        center, 45)  # 45° is north east direction

    return sw_point.latitude, sw_point.longitude, ne_point.latitude, ne_point.longitude


def retrieve_geoloc(activities_data):
    for activity in activities_data:
        if len(activity["start_latlng"]) == 2:
            geoloc_infos = reverse_geocode(activity["start_latlng"])
            activity["code"] = geoloc_infos["code"]
            activity["city"] = geoloc_infos["city"]
    return activities_data


def reverse_geocode(coords):
    location = GEOLOCATOR.reverse((coords[0], coords[1]))
    geoloc = {}
    city = "Unknown"
    code = ""
    if location:
        address = location.raw.get('address', {})
        city = address.get('town', '')
        code = address.get('ISO3166-2-lvl4', '')
        if city == '':
            city = address.get('village', '')

    geoloc["city"] = city
    geoloc["code"] = code
    return geoloc


def get_coordinates(city_name):
    location = GEOLOCATOR.geocode(city_name)
    if location:
        return (location.latitude, location.longitude)
    else:
        return None
