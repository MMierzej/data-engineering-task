from datetime import (datetime as dt, timedelta, timezone as tz,
                      MINYEAR, MAXYEAR)


YEAR_TO_DAYS = 365.25
MINDATETIME = dt(MINYEAR, 1, 1, tzinfo=tz.utc)
MAXDATETIME = dt(MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=tz.utc)


def dt_to_millis(dt):
    """
    Takes `datetime` object `dt` and returns its POSIX
    timestamp in milliseconds.
    """
    return int(dt.timestamp() * 1000)


def timestamp_from_age(now, age):
    """
    Takes `datetime` object `now` and a `float` `age` and returns
    POSIX timestamp of the date and time `age` years ago from `now`.
    """
    return dt_to_millis(now - timedelta(age * YEAR_TO_DAYS))


def age_from_timestamp(now, timestamp):
    """
    Takes `datetime` object `now` and a POSIX `timestamp` in milliseconds
    and returns the (possibly not integer) number of years from `timestamp`
    to `now`.
    """
    dt_from_ts = dt.fromtimestamp(timestamp / 1000, tz.utc)
    return (now - dt_from_ts) / timedelta(YEAR_TO_DAYS)
