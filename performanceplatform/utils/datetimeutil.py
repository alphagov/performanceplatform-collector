from datetime import datetime, time, timedelta, date
from dateutil.relativedelta import relativedelta
import pytz


MONDAY = 0


def to_datetime(a_date):
    if type(a_date) == datetime:
        return a_date.replace(tzinfo=pytz.UTC)
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


def period_range(start, end, frequency):
    start_date = to_date(start)
    end_date = to_date(end)
    if start_date > end_date:
        raise ValueError("Bad dates: !(start_date={0} <= end_date={1})"
                         .format(start_date, end_date))
    if frequency == 'daily':
        start_date = start_date or a_day_ago()
        end_date = end_date or a_day_ago()
        period = timedelta(days=1)
        return generate_date_range(start_date, end_date, period)
    elif frequency == 'weekly':
        start_date = start_date or a_week_ago()
        start_date = start_of_week(start_date)
        end_date = end_date or a_week_ago()
        period = timedelta(days=7)
        return generate_date_range(start_date, end_date, period)
    elif frequency == 'monthly':
        start_date = start_date or a_month_ago()
        start_date = start_of_month(start_date)
        end_date = end_date or a_month_ago()
        period = relativedelta(months=+1)
        return generate_date_range(start_date, end_date, period)
    else:
        raise ValueError('Bad value of frequency, should be daily, weekly '
                         'or monthly')


def generate_date_range(start_date, end_date, period):
    while start_date <= end_date:
        next_start_date = start_date + period
        yield (to_date(start_date),
               to_date(next_start_date - timedelta(days=1)))
        start_date = next_start_date


def a_week_ago():
    return date.today() - timedelta(days=7)


def a_day_ago():
    return date.today() - timedelta(days=1)


def an_hour_ago():
    return datetime.now() - timedelta(hours=1)


def start_of_month(date):
    return date.replace(day=1)


def a_month_ago():
    return date.today() + relativedelta(months=-1)


def start_of_week(date):
    if date.weekday != MONDAY:
        return date - timedelta(days=date.weekday())
    return date
