import time


class Gear:

    BIKE_MAPPING = {
        1: 'mtb',              # VTT
        2: 'cyclocross',       # Vélo de cyclo-cross
        3: 'road_bike',        # Vélo de route
        4: 'time_trial',       # Vélo de contre-la-montre
        5: 'gravel_bike'       # Vélo Gravel
    }

    def __init__(self, data: dict):
        self.raw = data
        self.type = self._resolve_type(data.get("frame_type"))
        self.brand = self._sanitize(data.get("brand_name"))
        self.model = self._sanitize(data.get("model_name"))
        self.id = self._sanitize(data.get("id"))
        self.weight = self._resolve_weight(
            data.get("frame_type"), data.get("weight"))
        self.distance = data.get("converted_distance", 0)

    def _sanitize(self, value):
        if not value:
            return ""
        return value.replace(" ", "_")

    def _resolve_type(self, gear_type):
        if gear_type is None:
            return "shoes"
        return self.BIKE_MAPPING.get(gear_type, "bikes")

    def _resolve_weight(self, gear_type, weight):
        if gear_type is None:
            return "0"
        return str(weight or "")

    def to_prom_labels(self):
        return {
            "id": self.id,
            "type": self.type,
            "brand": self.brand,
            "model": self.model,
            "weight": self.weight,
        }

    def to_remote_write(self, timestamp_ms: int):
        return {
            "metric": {
                "__name__": "distance_traveled",
                **self.to_prom_labels()
            },
            "values": [self.distance],
            "timestamps": [timestamp_ms]
        }


def build_distance_payload(gears):
    now = int(time.time() * 1000)
    return [gear.to_remote_write(now) for gear in gears]


def from_dicts(equipments: list):
    gears = []
    for eq in equipments:
        gears.append(Gear(eq))

    return gears
