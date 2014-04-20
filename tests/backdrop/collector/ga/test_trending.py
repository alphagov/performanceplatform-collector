import unittest

from freezegun import freeze_time
from datetime import date

from backdrop.collector.ga.trending import *


class test_data_calculations(unittest.TestCase):

    collapse_key = 'pageTitle'
    metric = 'pageviews'
    floor = 500

    data = [{'metrics': {u'pageviews': u'1000'},
             'dimensions': {u'pagePath': u'/foo/page1',
                            u'pageTitle': u'foo',
                            u'day': u'29',
                            u'month': u'01',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'100'},
             'dimensions': {u'pagePath': u'/foo/page1',
                            u'pageTitle': u'foo',
                            u'day': u'31',
                            u'month': u'01',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'120'},
             'dimensions': {u'pagePath': u'/foo/page1',
                            u'pageTitle': u'foo',
                            u'day': u'05',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'200'},
             'dimensions': {u'pagePath': u'/foo',
                            u'pageTitle': u'foo',
                            u'day': u'31',
                            u'month': u'01',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'500'},
             'dimensions': {u'pagePath': u'/foo',
                            u'pageTitle': u'foo',
                            u'day': u'05',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'89'},
             'dimensions': {u'pagePath': u'/foo/page2',
                            u'pageTitle': u'foo',
                            u'day': u'31',
                            u'month': u'01',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'98'},
             'dimensions': {u'pagePath': u'/foo/page2',
                            u'pageTitle': u'foo',
                            u'day': u'05',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'520'},
             'dimensions': {u'pagePath': u'/bar',
                            u'pageTitle': u'bar',
                            u'day': u'04',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'1209'},
             'dimensions': {u'pagePath': u'/bar',
                            u'pageTitle': u'bar',
                            u'day': u'11',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'07'},
             'dimensions': {u'pagePath': u'/baz',
                            u'pageTitle': u'baz',
                            u'day': u'04',
                            u'month': u'02',
                            u'year': u'2014'}},
            {'metrics': {u'pageviews': u'0'},
             'dimensions': {u'pagePath': u'/baz',
                            u'pageTitle': u'baz',
                            u'day': u'04',
                            u'month': u'02',
                            u'year': u'2014'}}]

    def test_encode_id(self):
        url = '/performance'

        self.assertEqual('L3BlcmZvcm1hbmNl', encode_id(url))

    @freeze_time("2014-02-12 01:00:00")
    def test_sum_by_day_with_floor(self):

        dates = get_date()

        collapsed_data = sum_data(self.data, self.metric, self.collapse_key,
                                  dates, self.floor)

        self.assertEqual(len(collapsed_data), 3)
        self.assertEqual(collapsed_data['foo'], {u'pageTitle': u'foo',
                                                 'week1': 1389, 'week2': 718,
                                                 u'pagePath': u'/foo'})
        self.assertEqual(collapsed_data['bar'], {u'pageTitle': u'bar',
                                                 'week1': 520, 'week2': 1209,
                                                 u'pagePath': u'/bar'})
        self.assertEqual(collapsed_data['baz'], {u'pageTitle': u'baz',
                                                 'week1': 500, 'week2': 500,
                                                 u'pagePath': u'/baz'})

    @freeze_time("2014-02-12 01:00:00")
    def test_get_percentage_trends(self):

        dates = get_date()

        collapsed_data = sum_data(self.data, self.metric, self.collapse_key,
                                  dates, self.floor)
        trended_data = get_trends(collapsed_data)

        self.assertEqual(trended_data['foo']['percent_change'],
                         -48.30813534917206)
        self.assertEqual(trended_data['bar']['percent_change'], 132.5)
        self.assertEqual(trended_data['baz']['percent_change'], 0)

    @freeze_time("2014-02-12 01:00:00")
    def test_flatten_data_and_assign_ids(self):

        dates = get_date()

        collapsed_data = sum_data(self.data, self.metric, self.collapse_key,
                                  dates, self.floor)
        trended_data = get_trends(collapsed_data)
        flattened_data = flatten_data_and_assign_ids(trended_data)

        flattened_keys = [k['_id'] for k in flattened_data]

        self.assertEqual(len(flattened_data), 3)
        self.assertIn('L2Jheg==', flattened_keys)
        self.assertIn('L2Zvbw==', flattened_keys)
        self.assertIn('L2Jhcg==', flattened_keys)


class test_dates(unittest.TestCase):

    @freeze_time("2014-02-12 01:00:00")
    def test_assign_day_to_week(self):

        dates = get_date()

        self.assertEqual(assign_day_to_week('29', '01', '2014', dates), 1)
        self.assertEqual(assign_day_to_week('04', '02', '2014', dates), 1)
        self.assertEqual(assign_day_to_week('05', '02', '2014', dates), 2)
        self.assertEqual(assign_day_to_week('11', '02', '2014', dates), 2)

    @freeze_time("2013-01-05 01:00:00")
    def test_assign_day_to_week_across_year_boundaries(self):

        dates = get_date()

        self.assertEqual(assign_day_to_week('22', '12', '2012', dates), 1)
        self.assertEqual(assign_day_to_week('28', '12', '2012', dates), 1)
        self.assertEqual(assign_day_to_week('29', '12', '2012', dates), 2)
        self.assertEqual(assign_day_to_week('04', '01', '2013', dates), 2)


class test_query_parsing(unittest.TestCase):
    def test_query_parsing_when_no_metric(self):
        query = {}
        self.assertRaises(
            Exception,
            parse_query, query
        )

    def test_query_parsing_when_empty_metric(self):
        query = {'metric': ''}
        self.assertRaises(
            Exception,
            parse_query, query
        )

    def test_when_just_a_metric(self):
        query = {'metric': 'pageview'}
        parsed_query = parse_query(query)

        self.assertEquals(
            parsed_query['dimensions'],
            ['day', 'month', 'year']
        )

    def test_when_a_metric_and_dimensions(self):
        query = {
            'metric': 'pageview',
            'dimensions': ['pageTitle', 'pagePath']
        }
        parsed_query = parse_query(query)

        self.assertEquals(
            parsed_query['dimensions'],
            ['pageTitle', 'pagePath', 'day', 'month', 'year']
        )


class test_date_picking(unittest.TestCase):

    @freeze_time("2014-02-12 01:00:00")
    def test_returns_last_two_weeks(self):
        (start, middle, end) = get_date()

        self.assertEqual(
            end,
            date(2014, 02, 11)
        )
        self.assertEqual(
            middle,
            date(2014, 02, 05)
        )
        self.assertEqual(
            start,
            date(2014, 01, 29)
        )
