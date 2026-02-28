import time

from models.kudos import Kudos


class Activity:

    def __init__(self, data: dict):
        self.raw = data
        self.id = data.get("id")
        self.distance = data.get("distance")
        self.elapsed_time = data.get("elapsed_time")
        self.total_elevation_gain = data.get("total_elevation_gain")
        self.city = data.get("city")
        self.iso_region = data.get("iso_region")
        self.average_heartrate = 0
        self.max_hearthrate = 0
        self.achievement_count = data.get("achievement_count")
        self.gear_id = data.get("gear_id")
        self.comment_count = data.get("comment_count")
        self.kudos_count = data.get("kudos_count")
        self.type = data.get("type")
        self.day_week = data.get("day_week")
        self.gear_id = data.get("gear_id")
        if data.get("has_heartrate"):
            self.average_heartrate = data.get("average_heartrate")
            self.max_hearthrate = data.get("max_heartrate")

        self.sport_type = data.get("sport_type")
        self.average_speed = data.get("average_speed", 0)
        self.max_speed = data.get("max_speed", 0)

        raw_kudoers = data.get("kudoers") or []
        self.kudoers: list[Kudos] = [
            Kudos(k) if not isinstance(k, Kudos) else k
            for k in raw_kudoers
        ]

    def from_dicts(activities_list: list):
        activities = []
        for activity in activities_list:
            activities.append(Activity(activity))

        return activities

    def agg_hashes_to_remote_write(agg_dict: dict):
        series = []
        timestamp_ms = int(time.time() * 1000)

        for key, fields in agg_dict.items():
            parts = key.split(":")
            city = parts[2] if len(parts) >= 4 else "unknown"
            activity_type = parts[3] if len(parts) >= 4 else "unknown"

            for field, value in fields.items():
                try:
                    numeric_value = float(value)
                except (ValueError, TypeError):
                    continue

                series.append({
                    "metric": {
                        "__name__": f"{field}",
                        "city": city,
                        "type": activity_type
                    },
                    "values": [numeric_value],
                    "timestamps": [timestamp_ms],
                })

        return series
