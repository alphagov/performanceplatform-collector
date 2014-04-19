from datetime import datetime
import pytz


def dt(year, month, day, hours, minutes, seconds, tz):
    _dt = datetime(year, month, day, hours, minutes, seconds)
    return pytz.timezone(tz).localize(_dt)
