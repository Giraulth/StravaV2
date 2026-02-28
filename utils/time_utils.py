from datetime import UTC, datetime, timedelta


class TimeUtils:

    @staticmethod
    def to_epoch(utc_dt: datetime) -> int:
        return int(utc_dt.timestamp())

    @staticmethod
    def today_utc() -> datetime:
        return datetime.now(UTC).replace(
            hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def today_utc_epoch() -> int:
        today_midnight = datetime.now(UTC).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return TimeUtils.to_epoch(today_midnight)

    @staticmethod
    def date_str(dt: datetime = None) -> str:
        if dt is None:
            dt = TimeUtils.today_utc()
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def add_days_utc(n_days: int = 1) -> datetime:
        dt = TimeUtils.today_utc() + timedelta(days=n_days)
        return TimeUtils.to_epoch(dt)

    @staticmethod
    def subtract_days_utc(n_days: int = 1) -> datetime:
        dt = TimeUtils.today_utc() - timedelta(days=n_days)
        return TimeUtils.to_epoch(dt)

    @staticmethod
    def daily_key(prefix: str, id_value: str, dt: datetime = None) -> str:
        date_part = TimeUtils.date_str(dt)
        return f"{prefix}:{id_value}:{date_part}"

    @staticmethod
    def to_day_week(date_str):
        date_str_clean = date_str.replace(" ", "")
        dt = datetime.strptime(date_str_clean, '%Y-%m-%dT%H:%M:%SZ')
        return dt.strftime('%A')
