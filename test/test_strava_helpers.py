from services.strava_helpers import get_all_activities


def test_fetch_all_activities_pagination(monkeypatch):
    responses = {
        1: [
            {"id": 1, "start_date_local": "2023-01-01T10:00:00Z"},
            {"id": 2, "start_date_local": "2023-01-02T10:00:00Z"},
        ],
        2: [
            {"id": 3, "start_date_local": "2023-01-03T10:00:00Z"},
        ],
        3: []
    }

    def mock_get_data_from_url(url, headers):
        import re
        match = re.search(r"page=(\d+)", url)
        page = int(match.group(1))
        return responses[page]

    def mock_sanitize(data):
        return data

    monkeypatch.setattr(
        "services.strava_helpers.get_data_from_url",
        mock_get_data_from_url)
    monkeypatch.setattr(
        "services.strava_helpers.sanitize_strava_data",
        mock_sanitize)

    result = get_all_activities(headers={})

    assert result == [{'id': 1,
                       'start_date_local': '2023-01-01T10:00:00Z',
                       'kudoers': [],
                       'day_week': 'Sunday'},
                      {'id': 2,
                       'start_date_local': '2023-01-02T10:00:00Z',
                       'kudoers': [],
                       'day_week': 'Monday'},
                      {'id': 3,
                       'start_date_local': '2023-01-03T10:00:00Z',
                       'kudoers': [],
                       'day_week': 'Tuesday'}]
