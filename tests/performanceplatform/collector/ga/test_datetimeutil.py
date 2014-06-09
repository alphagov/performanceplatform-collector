from datetime import date, datetime
import pytz
from hamcrest import assert_that, only_contains, is_, equal_to
from nose.tools import raises
from freezegun import freeze_time
from performanceplatform.collector.ga.datetimeutil import(
    period_range,
    to_date,
    to_datetime)


def test_to_date_passes_none_through():
    assert_that(to_date(None), is_(None))


def test_to_date_converts_datetime_to_date():
    assert_that(
        to_date(datetime(2012, 12, 12)),
        equal_to(date(2012, 12, 12)))


def test_to_date_passes_date_through():
    assert_that(
        to_date(date(2012, 12, 12)),
        equal_to(date(2012, 12, 12)))


@raises(ValueError)
def test_to_date_raises_error_on_invalid_input():
    to_date("")


def test_period_range():
    range = period_range(date(2013, 4, 1), date(2013, 4, 7))
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))

    another_range = period_range(date(2013, 4, 1), date(2013, 4, 21))
    assert_that(another_range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7)),
        (date(2013, 4, 8), date(2013, 4, 14)),
        (date(2013, 4, 15), date(2013, 4, 21)),
    ))


def test_period_range_between_datetime_and_date():
    range = period_range(datetime(2013, 4, 1), date(2013, 4, 7))
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))


def test_period_range_adjusts_dates():
    range = period_range(date(2013, 4, 3), date(2013, 4, 10))
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7)),
        (date(2013, 4, 8), date(2013, 4, 14))
    ))


@freeze_time("2013-04-10")
def test_period_range_defaults_to_a_week_ago():
    range = period_range(None, None)
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))


@raises(ValueError)
def test_period_range_fails_when_end_is_before_start():
    list(period_range(date(2013, 4, 8), date(2013, 4, 1)))


def test_period_range_returns_the_containing_week_when_start_equals_end():
    range = period_range(date(2013, 4, 8), date(2013, 4, 8))
    assert_that(range, only_contains(
        (date(2013, 4, 8), date(2013, 4, 14))
    ))


# daily
@freeze_time("2013-04-2")
def test_daily_period_range_defaults_to_a_day_ago():
    range = period_range(None, None, 'daily')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 1))
    ))


def test_daily_period_range():
    range = period_range(date(2013, 4, 1), date(2013, 4, 1), 'daily')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 1))
    ))

    another_range = period_range(date(2013, 4, 1), date(2013, 4, 3), 'daily')
    assert_that(another_range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 1)),
        (date(2013, 4, 2), date(2013, 4, 2)),
        (date(2013, 4, 3), date(2013, 4, 3)),
    ))


def test_daily_period_range_between_datetime_and_date():
    range = period_range(datetime(2013, 4, 1), date(2013, 4, 2), 'daily')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 1)),
        (date(2013, 4, 2), date(2013, 4, 2))
    ))


@raises(ValueError)
def test_daily_period_range_fails_when_end_is_before_start():
    list(period_range(date(2013, 4, 8), date(2013, 4, 1), 'daily'))


def test_daily_period_range_returns_the_containing_day_when_start_equals_end():
    range = period_range(date(2013, 4, 8), date(2013, 4, 8), 'daily')
    assert_that(range, only_contains(
        (date(2013, 4, 8), date(2013, 4, 8))
    ))


# hourly, something to worry about with utc
@freeze_time("2013-04-02 11:53:00", tz_offset=0)
def test_hourly_period_range_defaults_to_a_day_ago():
    range = period_range(None, None, 'hourly')
    assert_that(range, only_contains(
        (datetime(2013, 4, 2, 10), datetime(2013, 4, 2, 11))
    ))

"""
def test_hourly_period_range():
    range = period_range(date(2013, 4, 1), date(2013, 4, 2), 'hourly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 2))
    ))

    another_range = period_range(date(2013, 4, 1), date(2013, 4, 3), 'hourly')
    assert_that(another_range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 2)),
        (date(2013, 4, 3), date(2013, 4, 4)),
        (date(2013, 4, 5), date(2013, 4, 6)),
    ))


def test_hourly_period_range_between_datetime_and_date():
    range = period_range(datetime(2013, 4, 1), date(2013, 4, 2), 'hourly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 2))
    ))


@raises(ValueError)
def test_hourly_period_range_fails_when_end_is_before_start():
    list(period_range(date(2013, 4, 8), date(2013, 4, 1), 'hourly'))


def test_hourly_period_range_returns_the_containing_day_when_start_equals_end():
    range = period_range(date(2013, 4, 8), date(2013, 4, 8), 'hourly')
    assert_that(range, only_contains(
        (date(2013, 4, 8), date(2013, 4, 9))
    ))
"""
