import time
import pytest
from unittest.mock import MagicMock, patch

from geopy.exc import GeocoderTimedOut

from utils import constant
from utils.geoloc import gps_to_remote_write, h3_to_latlng, reduce_polyline, reverse_geocode


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


def test_decode_polyline():
    coords = reduce_polyline(
        "ki{eFvqfiVsBmA`Feh@qg@iX`B}JeCcCqGjIq~@kf@cM{KeHeX`@_GdGkSeBiXtB}YuEkPwFyDeAzAe@pC~DfGc@bIOsGmCcEiD~@oBuEkFhBcBmDiEfAVuDiAuD}NnDaNiIlCyDD_CtJKv@wGhD]YyEzBo@g@uKxGmHpCGtEtI~AuLrHkAcAaIvEgH_EaDR_FpBuBg@sNxHqEtHgLoTpIiCzKNr[sB|Es\\`JyObYeMbGsMnPsAfDxAnD}DBu@bCx@{BbEEyAoD`AmChNoQzMoGhOwX|[yIzBeFKg[zAkIdU_LiHxK}HzEh@vM_BtBg@xGzDbCcF~GhArHaIfByAhLsDiJuC?_HbHd@nL_Cz@ZnEkDDy@hHwJLiCbIrNrIvN_EfAjDWlEnEiAfBxDlFkBfBtEfDaAzBvDKdFx@|@XgJmDsHhAgD`GfElEzOwBnYdBxXgGlSc@bGdHpW|HdJztBnhAgFxc@HnCvBdA",
        5,
        8)

    assert coords == {
        '8828308ae5fffff',
        '8828308a31fffff',
        '8828308ac5fffff',
        '8828308ae7fffff',
        '8828308aebfffff',
        '8828308ae9fffff',
        '8828308a33fffff',
        '8828308ac7fffff',
        '8828308135fffff',
        '8828308a37fffff',
        '8828308ac9fffff',
        '8828308123fffff',
        '8828308aedfffff',
        '8828308ae1fffff'}


def test_decode_empty_polyline():
    coords = reduce_polyline("")
    assert coords == set()


def test_h3_to_prometheus(monkeypatch):

    # Mock du timestamp
    fixed_time = 1773423231.502

    monkeypatch.setattr(time, "time", lambda: fixed_time)
    expected_timestamp = int(fixed_time * 1000)

    h3_cells = {
        "run": {
            '8828308ae5fffff': 1,
            '8828308a31fffff': 1,
            '8828308ac5fffff': 1,
            '8828308ae7fffff': 1}}
    result = h3_to_latlng(h3_cells)
    expected_result = [{'cell': '8828308ae5fffff',
                       'lat': 37.85765747365914,
                        'lng': -122.21574064639363,
                        'count': 1,
                        'type': 'run'},
                       {'cell': '8828308a31fffff',
                       'lat': 37.84595372793507,
                        'lng': -122.2009949140769,
                        'count': 1,
                        'type': 'run'},
                       {'cell': '8828308ac5fffff',
                       'lat': 37.84466435135811,
                        'lng': -122.23962675007617,
                        'count': 1,
                        'type': 'run'},
                       {'cell': '8828308ae7fffff',
                       'lat': 37.863507840210794,
                        'lng': -122.22311423912456,
                        'count': 1,
                        'type': 'run'}]

    for r, e in zip(result, expected_result):
        assert r["cell"] == e["cell"]
        assert r["count"] == e["count"]
        assert r["type"] == e["type"]
        assert r["lat"] == pytest.approx(e["lat"], rel=1e-12)
        assert r["lng"] == pytest.approx(e["lng"], rel=1e-12)

    prom_data = gps_to_remote_write(result)
    expected_prom_data = [{'metric': {'__name__': 'h3_cell',
                                      'cell': '8828308ae5fffff',
                                      'activity_type': 'run',
                                      'lat': '37.85765747365914',
                                      'lng': '-122.21574064639363'},
                          'values': [1],
                           'timestamps': [expected_timestamp]},
                          {'metric': {'__name__': 'h3_cell',
                                      'cell': '8828308a31fffff',
                                      'activity_type': 'run',
                                      'lat': '37.84595372793507',
                                      'lng': '-122.2009949140769'},
                          'values': [1],
                           'timestamps': [expected_timestamp]},
                          {'metric': {'__name__': 'h3_cell',
                                      'cell': '8828308ac5fffff',
                                      'activity_type': 'run',
                                      'lat': '37.84466435135811',
                                      'lng': '-122.23962675007617'},
                          'values': [1],
                           'timestamps': [expected_timestamp]},
                          {'metric': {'__name__': 'h3_cell',
                                      'cell': '8828308ae7fffff',
                                      'activity_type': 'run',
                                      'lat': '37.863507840210794',
                                      'lng': '-122.22311423912456'},
                          'values': [1],
                           'timestamps': [expected_timestamp]}]

    for r, e in zip(prom_data, expected_prom_data):
        assert r["metric"]["__name__"] == e["metric"]["__name__"]
        assert r["metric"]["cell"] == e["metric"]["cell"]
        assert r["metric"]["activity_type"] == e["metric"]["activity_type"]

        assert float(
            r["metric"]["lat"]) == pytest.approx(
            float(
                e["metric"]["lat"]),
            rel=1e-12)
        assert float(
            r["metric"]["lng"]) == pytest.approx(
            float(
                e["metric"]["lng"]),
            rel=1e-12)

        assert r["values"] == e["values"]
        assert r["timestamps"] == e["timestamps"]
