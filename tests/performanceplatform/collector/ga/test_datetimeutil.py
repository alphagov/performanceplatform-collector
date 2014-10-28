from datetime import date, datetime
from hamcrest import assert_that, only_contains, is_, equal_to
from nose.tools import raises
from freezegun import freeze_time
from performanceplatform.utils.datetimeutil import(
    period_range,
    to_date)


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


def test_weekly_period_range():
    range = period_range(date(2013, 4, 1), date(2013, 4, 7), 'weekly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))

    another_range = period_range(date(2013, 4, 1), date(2013, 4, 21), 'weekly')
    assert_that(another_range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7)),
        (date(2013, 4, 8), date(2013, 4, 14)),
        (date(2013, 4, 15), date(2013, 4, 21)),
    ))


def test_weekly_period_range_between_datetime_and_date():
    range = period_range(datetime(2013, 4, 1), date(2013, 4, 7), 'weekly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))


def test_weekly_period_range_adjusts_dates():
    range = period_range(date(2013, 4, 3), date(2013, 4, 10), 'weekly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7)),
        (date(2013, 4, 8), date(2013, 4, 14))
    ))


@freeze_time("2013-04-11")
def test_weekly_period_range_defaults_to_a_week_ago():
    range = period_range(None, None, 'weekly')
    assert_that(range, only_contains(
        (date(2013, 4, 1), date(2013, 4, 7))
    ))


@raises(ValueError)
def test_weekly_period_range_fails_when_end_is_before_start():
    list(period_range(date(2013, 4, 8), date(2013, 4, 1), 'weekly'))


def test_weekly_period_range_gets_the_containing_week_when_start_equals_end():
    range = period_range(date(2013, 4, 8), date(2013, 4, 8), 'weekly')
    assert_that(range, only_contains(
        (date(2013, 4, 8), date(2013, 4, 14))
    ))


@freeze_time("2013-04-02", tz_offset=0)
def test_monthly_period_range_defaults_to_a_month_ago():
    range = period_range(None, None, 'monthly')
    # from pprint import pprint
    # pprint(range)
    assert_that(range, only_contains(
        (date(2013, 3, 1), date(2013, 3, 31))
    ))


def check_monthly_period_range(start_date, end_date, frequency, expected):
    range = period_range(date(2013, 3, 1), date(2013, 3, 31), 'monthly')
    assert_that(range, only_contains(
        (date(2013, 3, 1), date(2013, 3, 31))
    ))

    another_range = list(
        period_range(
            date(2012, 12, 1), date(2013, 2, 28),
            'monthly')
    )
    print another_range
    assert_that(another_range, only_contains(
        (date(2012, 12, 1), date(2012, 12, 31)),
        (date(2013, 1, 1), date(2013, 1, 31)),
        (date(2013, 2, 1), date(2013, 2, 28)),
    ))

    another_range = list(
        period_range(
            date(2012, 12, 20), date(2013, 2, 28),
            'monthly')
    )
    print another_range
    assert_that(another_range, only_contains(
        (date(2012, 12, 1), date(2012, 12, 31)),
        (date(2013, 1, 1), date(2013, 1, 31)),
        (date(2013, 2, 1), date(2013, 2, 28)),
    ))


@raises(ValueError)
def test_monthly_period_range_fails_when_end_is_before_start():
    list(period_range(date(2013, 4, 8), date(2013, 3, 1), 'monthly'))
