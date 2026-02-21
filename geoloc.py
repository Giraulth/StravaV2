from geopy.geocoders import Nominatim
from geopy.point import Point
from geopy.distance import distance


GEOLOCATOR = Nominatim(user_agent="curl/7.6.1")

def get_bounding_box(coords, radius_km):
    center = Point(coords[0], coords[1]) # Central point of the city


    sw_point = distance(kilometers=radius_km).destination(center, 225)  # 225° is south west direction
    ne_point = distance(kilometers=radius_km).destination(center, 45)  # 45° is north east direction

    return sw_point.latitude, sw_point.longitude, ne_point.latitude, ne_point.longitude


def reverse_geocode(coords):
    location = GEOLOCATOR.reverse((coords[0], coords[1]))
    
    if location:
        address = location.raw.get('address', {})
        city = address.get('town', '')
        if city == '':
            city = address.get('village', '')
        return city
    else:
        return "Unknown"
    
def get_coordinates(city_name):
    location = GEOLOCATOR.geocode(city_name)
    if location:
        return (location.latitude, location.longitude)
    else:
        return None