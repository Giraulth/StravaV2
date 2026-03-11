from unittest.mock import patch, MagicMock
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from utils.geoloc import reverse_geocode
from utils import constant

def test_reverse_geocode_success():
    coords = (48.8566, 2.3522)

    fake_location = MagicMock()
    fake_location.raw = {
        "address": {
            "city": "Paris",
            "ISO3166-2-lvl4": "FR-IDF"
        }
    }

    with patch("utils.geoloc.GEOLOCATOR.reverse", return_value=fake_location):
        result = reverse_geocode(coords)

    assert result["city"] == "Paris"
    assert result["iso_region"] == "FR-IDF"

def test_reverse_geocode_none():
    coords = (0, 0)

    with patch("utils.geoloc.GEOLOCATOR.reverse", return_value=None):
        result = reverse_geocode(coords)

    assert result["city"] == constant.DEFAULT_CITY
    assert result["iso_region"] == constant.DEFAULT_ISO_REGION

def test_reverse_geocode_timeout():
    coords = (0, 0)

    with patch("utils.geoloc.GEOLOCATOR.reverse", side_effect=GeocoderTimedOut):
        result = reverse_geocode(coords)

    assert result["city"] == constant.DEFAULT_CITY
    assert result["iso_region"] == constant.DEFAULT_ISO_REGION