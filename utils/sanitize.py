import unicodedata
import re
import hashlib


def hash_sha256(to_hash: str) -> str:
    return hashlib.sha256(to_hash.encode()).hexdigest()[:8]


def normalize_string(s: str) -> str:
    nfkd = unicodedata.normalize('NFKD', s)
    ascii_str = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    clean = re.sub(r'[^a-zA-Z0-9]+', '_', ascii_str)
    return clean.strip('_').lower()


def sanitize_strava_data(data):
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if key == "map":
                sanitized[key] = ""
            else:
                sanitized[key] = sanitize_strava_data(value)
        return sanitized

    elif isinstance(data, list):
        return [sanitize_strava_data(item) for item in data]

    elif data is None:
        return ""

    else:
        return data
