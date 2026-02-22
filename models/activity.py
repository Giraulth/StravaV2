class Activity:

    def __init__(self, data: dict):
        self.raw = data
        self.id = data.get("id")
        self.distance = data.get("distance")
        self.elapsed_time = data.get("elapsed_time")
        self.kudoers = data.get("kudoers")

    def from_dicts(activities_list: list):
        activities = []
        for activity in activities_list:
            activities.append(Activity(activity))

        return activities
