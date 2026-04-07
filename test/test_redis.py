from unittest.mock import patch

import pytest
from fakeredis import FakeStrictRedis

from redis_db import RedisStore
from utils.time_utils import TimeUtils


class TestSetDistance:
    @pytest.fixture
    def redis_db(self):
        with patch("redis_db.redis.Redis.from_url"), \
        patch.object(RedisStore, "__init__", lambda self, url: None):
            redis_db = RedisStore(url="redis://localhost")
        redis_db.redis = FakeStrictRedis()
        return redis_db

    def test_set_distance_first_time(self, redis_db):
        """Test setting distance when no previous distance exists"""
        gear_type = "bike"
        gear_id = "gear_123"
        distance = 100.5

        with patch.object(TimeUtils, 'today_utc_epoch', return_value=1000):
            redis_db.set_distance(gear_type, gear_id, distance)

        assert redis_db.redis.get("bike:gear_123:last_distance") == b"100.5"
        assert redis_db.redis.get("bike:gear_123:last_date") == b"1000"
        assert redis_db.redis.get("bike:gear_123:1000") == b"100.5"

    def test_set_distance_changed(self, redis_db):
        """Test setting distance when distance has changed"""
        gear_type = "bike"
        gear_id = "gear_123"

        with patch.object(TimeUtils, 'today_utc_epoch', return_value=1000):
            redis_db.set_distance(gear_type, gear_id, 100.0)
            redis_db.set_distance(gear_type, gear_id, 150.5)

        assert redis_db.redis.get("bike:gear_123:last_distance") == b"150.5"
        assert redis_db.redis.get("bike:gear_123:1000") == b"150.5"

    def test_set_distance_unchanged(self, redis_db):
        """Test setting distance when distance hasn't changed"""
        gear_type = "bike"
        gear_id = "gear_123"
        distance = 100.5

        with patch.object(TimeUtils, 'today_utc_epoch', return_value=1000):
            redis_db.set_distance(gear_type, gear_id, distance)
            redis_db.set_distance(gear_type, gear_id, distance)

        assert redis_db.redis.get("bike:gear_123:last_distance") == b"100.5"
        assert redis_db.redis.get("bike:gear_123:last_date") == b"1000"

    def test_set_distance_multiple_dates(self, redis_db):
        """Test setting distance on different dates saves history"""
        gear_type = "bike"
        gear_id = "gear_123"

        with patch.object(TimeUtils, 'today_utc_epoch', return_value=1000):
            redis_db.set_distance(gear_type, gear_id, 100.0)

        with patch.object(TimeUtils, 'today_utc_epoch', return_value=2000):
            redis_db.set_distance(gear_type, gear_id, 150.0)

        assert redis_db.redis.get("bike:gear_123:1000") == b"100.0"
        assert redis_db.redis.get("bike:gear_123:2000") == b"150.0"
        assert redis_db.redis.get("bike:gear_123:last_distance") == b"150.0"

    def test_set_distance_different_gear_types(self, redis_db):
        """Test setting distance for different gear types"""
        with patch.object(TimeUtils, 'today_utc_epoch', return_value=1000):
            redis_db.set_distance("bike", "id1", 100.0)
            redis_db.set_distance("shoes", "id2", 50.0)

        assert redis_db.redis.get("bike:id1:last_distance") == b"100.0"
        assert redis_db.redis.get("shoes:id2:last_distance") == b"50.0"