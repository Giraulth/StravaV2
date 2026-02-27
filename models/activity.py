from models.kudos import Kudos


class Activity:

    def __init__(self, data: dict):
        self.raw = data
        self.id = data.get("id")
        self.distance = data.get("distance")
        self.elapsed_time = data.get("elapsed_time")
        self.total_elevation_gain = data.get("total_elevation_gain")
        self.city = data.get("city")
        self.code = data.get("code")
        self.average_heartrate = 0
        self.max_hearthrate = 0
        self.achievement_count = data.get("achievement_count")
        self.gear_id = data.get("gear_id")
        self.comment_count = data.get("comment_count")
        self.kudos_count = data.get("kudos_count")
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
