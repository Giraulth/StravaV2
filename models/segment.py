import time

from utils.time_utils import TimeUtils


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
            return -1

        best = min(
            achievements,
            key=lambda a: a.get("type_id", float("inf"))
        )
        return best.get("rank")

    def segments_redis_to_remote_write(segments):
        timestamp_ms = int(time.time() * 1000)
        series = []

        def add(value, labels):
            series.append({
                "metric": {
                    "__name__": "segment_data",
                    **labels,
                },
                "values": [value],
                "timestamps": [timestamp_ms],
            })

        for segment in segments:

            labels = {
                "segment_id": str(segment["id"]),
                "name": segment.get("name", ""),
                "activity_type": segment.get("activity_type", ""),
                "city": segment.get("city", ""),
                "region": segment.get("region", ""),
                "country": segment.get("country", ""),
            }

            add(1, {
                **labels,
                "distance": str(segment.get("distance")),
                "avg_grade": str(segment.get("average_grade")),
                "max_grade": str(segment.get("maximum_grade")),
                "elevation_high": str(segment.get("elevation_high")),
                "elevation_low": str(segment.get("elevation_low")),
                "pr_elapsed_time": str(segment.get("elapsed_time") or segment.get("pr_time")),
                "pr_date": TimeUtils.iso_to_epoch_ms(str(segment.get("pr_date"))),
                "is_kom": str(bool(segment.get("is_kom"))),
                "rank": str(segment.get("rank") or ""),
            })

        return series
