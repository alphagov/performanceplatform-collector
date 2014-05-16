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


def period_range(start_date, end_date):
    start_date = to_date(start_date) or a_week_ago()
    end_date = to_date(end_date) or a_week_ago()

    if start_date > end_date:
        raise ValueError("Bad period: !(start_date={0} <= end_date={1})"
                         .format(start_date, end_date))

    if start_date.weekday != MONDAY:
        start_date = start_date - timedelta(days=start_date.weekday())

    period = timedelta(days=7)
    while start_date <= end_date:
        yield (start_date, start_date + timedelta(days=6))
        start_date += period


def a_week_ago():
    return date.today() - timedelta(days=7)
