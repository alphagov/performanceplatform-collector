from datetime import datetime, time, timedelta, date
import pytz


MONDAY = 0


def to_datetime(a_date):
    return datetime.combine(a_date, time(0)).replace(tzinfo=pytz.UTC)


def to_date(a_datetime):
    if a_datetime is None:
        return None
    elif isinstance(a_datetime, datetime):
        return a_datetime.date()
    elif isinstance(a_datetime, date):
        return a_datetime
    else:
        raise ValueError("{0!r} ({1}) isn't a date or datetime"
                         .format(a_datetime, type(a_datetime)))


def to_utc(a_datetime):
    return a_datetime.astimezone(pytz.UTC)


def period_range(start, end, repeat=None):
    # and start_date/end_date currently never passed in
    if not repeat:
        start_date = to_date(start) or a_week_ago()
        end_date = to_date(end) or a_week_ago()

        if start_date > end_date:
            raise ValueError("Bad period: !(start_date={0} <= end_date={1})"
                             .format(start_date, end_date))

        if start_date.weekday != MONDAY:
            start_date = start_date - timedelta(days=start_date.weekday())

        period = timedelta(days=7)
        while start_date <= end_date:
            yield (start_date, start_date + timedelta(days=6))
            start_date += period
    if repeat == 'daily':
        start_date = to_date(start) or a_day_ago()
        end_date = to_date(end) or a_day_ago()

        if start_date > end_date:
            raise ValueError("Bad period: !(start_date={0} <= end_date={1})"
                             .format(start_date, end_date))

        period = timedelta(days=2)
        while start_date <= end_date:
            yield (start_date, start_date + timedelta(days=1))
            start_date += period
    if repeat == 'hourly':
        start_time = start or an_hour_ago()
        end_time = end or an_hour_ago()

        start_time = to_start_of_hour(start_time)
        end_time = to_start_of_hour(end_time)

        if start_time > end_time:
            raise ValueError("Bad period: !(start_time={0} <= end_time={1})"
                             .format(start_time, end_time))

        period = timedelta(hours=2)
        while start_time <= end_time:
            print start_time, start_time + timedelta(hours=1)
            yield (start_time, start_time + timedelta(hours=1))
            start_time += period


def a_week_ago():
    return date.today() - timedelta(days=7)


def a_day_ago():
    return date.today() - timedelta(days=1)


def an_hour_ago():
    return datetime.now() - timedelta(hours=1)


def to_start_of_hour(datetime):
    return datetime - timedelta(
        minutes=datetime.minute,
        seconds=datetime.second)
