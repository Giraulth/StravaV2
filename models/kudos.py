import time


class Kudos:
    def __init__(self, data: dict):

        raw_firstname = data.get("firstname") or ""
        raw_lastname = data.get("lastname") or ""

        self.firstname: str = raw_firstname.replace(" ", "").strip()
        self.lastname: str = raw_lastname.replace(" ", "").rstrip(".").strip()

    @property
    def full_name(self) -> str:
        return f"{self.firstname}{self.lastname}"

    def __repr__(self) -> str:
        return f"Kudos(firstname={self.firstname}, lastname={self.lastname})"

    def kudos_redis_to_remote_write(kudos_dict: dict):
        series = []
        timestamp_ms = int(time.time() * 1000)
        for key, count in kudos_dict.items():
            username = key.split(":", 1)[1]

            series.append({
                "metric": {
                    "__name__": "kudos_sent_total",
                    "username": username,
                },
                "values": [count],
                "timestamps": [timestamp_ms],
            })

        return series
