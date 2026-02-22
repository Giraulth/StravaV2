import json


def load_fixture(path):
    with open(path) as f:
        return json.load(f)
