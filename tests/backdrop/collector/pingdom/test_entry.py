from datetime import datetime, timedelta
import unittest
from hamcrest import assert_that, has_entry, is_
import pytz
from backdrop.collector.pingdom import \
    convert_from_pingdom_to_backdrop, truncate_hour_fraction, \
    parse_time_range

from freezegun import freeze_time


class TestCollect(unittest.TestCase):
    def test_converting_from_pingdom_to_backdrop_records(self):
        hourly_stats = {
            u'avgresponse': 721,
            u'downtime': 523,
            u'starttime': datetime(2013, 6, 15, 22, 0, tzinfo=pytz.UTC),
            u'unmonitored': 12,
            u'uptime': 3599
        }

        name_of_check = 'testCheck'
        doc = convert_from_pingdom_to_backdrop(hourly_stats, name_of_check)

        assert_that(doc,
                    has_entry('_id', 'testCheck.2013-06-15T22:00:00+00:00'))
        assert_that(doc, has_entry('_timestamp', '2013-06-15T22:00:00+00:00'))
        assert_that(doc, has_entry('check', 'testCheck'))
        assert_that(doc, has_entry('avgresponse', 721))
        assert_that(doc, has_entry('uptime', 3599))
        assert_that(doc, has_entry('downtime', 523))
        assert_that(doc, has_entry('unmonitored', 12))

    def test_converting_to_backdrop_record_removes_whitespace_from_id(self):
        hourly_stats = {
            u'avgresponse': 721,
            u'downtime': 523,
            u'starttime': datetime(2013, 6, 15, 22, 0, tzinfo=pytz.UTC),
            u'unmonitored': 12,
            u'uptime': 3599
        }
        name_of_check = "name with whitespace"

        doc = convert_from_pingdom_to_backdrop(hourly_stats, name_of_check)

        assert_that(doc, has_entry('_id', 'name_with_whitespace.'
                                          '2013-06-15T22:00:00+00:00'))

    def test_truncate_hour_fraction(self):
        assert_that(
            truncate_hour_fraction(datetime(2013, 6, 15, 22, 0, 0, 0)),
            is_(datetime(2013, 6, 15, 22, 0, 0, 0))
        )
        assert_that(
            truncate_hour_fraction(datetime(2013, 6, 15, 22, 1, 2, 3)),
            is_(datetime(2013, 6, 15, 22, 0, 0, 0))
        )

    def test_start_date_no_end_date_results_in_period_until_now(self):
        start_dt = datetime(2013, 6, 15, 13, 0)
        end_dt = None

        now = datetime(2014, 2, 15, 11, 0)

        with freeze_time(now):
            assert_that(
                parse_time_range(start_dt, end_dt),
                is_((start_dt, now)))

    def test_end_date_no_start_date_results_in_2005_until_start_date(self):
        start_dt = None
        end_dt = datetime(2013, 6, 15, 13, 0)

        earliest = datetime(2005, 1, 1)

        assert_that(
            parse_time_range(start_dt, end_dt),
            is_((earliest, end_dt)))

    def test_start_and_end_results_in_correct_period(self):
        start_dt = datetime(2013, 6, 10, 10, 0)
        end_dt = datetime(2013, 6, 15, 13, 0)

        assert_that(
            parse_time_range(start_dt, end_dt),
            is_((start_dt, end_dt)))

    def test_parse_time_range_no_start_or_end_uses_last_24_hours(self):
        now = datetime(2013, 12, 27, 13, 0, 0)
        one_day_ago = now - timedelta(days=1)

        with freeze_time(now):
            assert_that(
                parse_time_range(None, None),
                is_((one_day_ago, now)))

    def test_parse_time_range_truncates_the_hour(self):
        start_dt = datetime(2013, 6, 27, 10, 15, 34)
        end_dt = datetime(2013, 6, 30, 13, 45, 30)

        expected_start_dt = datetime(2013, 6, 27, 10, 0, 0)
        expected_end_dt = datetime(2013, 6, 30, 13, 0, 0)

        assert_that(
            parse_time_range(start_dt, end_dt),
            is_((expected_start_dt, expected_end_dt)))
