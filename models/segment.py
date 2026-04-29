

class Segment:

    def __init__(self, data: dict):
        self.raw = data

        self.id = data.get("id")
        self.distance = data.get("distance")
        self.activity_type = data.get("activity_type")
        self.name = data.get("name")
        self.city = data.get("city")
        self.region = data.get("state")

        effort = data.get("athlete_pr_effort") or {}
        self.athlete_pr_effort_id = effort.get("id")

        self.effort_count = int(data.get("effort_count") or 0)
        self.best_effort_rank = data.get("rank")

        self.pr_elapsed_time = data.get("pr_elapsed_time")
        self.pr_date = data.get("pr_date")

    @staticmethod
    def from_dicts(segment_list: list):
        return [Segment(segment) for segment in segment_list]

    @staticmethod
    def extract_rank(data: dict):
        achievements = data.get("achievements") or []

        if not achievements:
            return None

        best = min(
            achievements,
            key=lambda a: a.get("type_id", float("inf"))
        )
        return best.get("rank")
