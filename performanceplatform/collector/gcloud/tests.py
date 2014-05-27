
# encoding: utf-8

from __future__ import unicode_literals

import mock
import unittest

from nose.tools import assert_equal, assert_is_instance
from os.path import join, dirname, abspath

from sales_parser import (process_csv, parse_latest_csv_url,
                          UNKNOWN_SECTOR_KEY)

from aggregate import (
    SpendingGroupKey,
    make_spending_group_keys, make_company_type_keys,
    make_aggregated_sales_row, get_monthly_spend_and_count,
    get_cumulative_spend, get_cumulative_count, get_distinct_month_years)


SAMPLE_DIR = join(dirname(abspath(__file__)), 'sample_data')


class ExtractLatestUrlTestCase(unittest.TestCase):

    def test_parse_latest_csv_url_index_page(self):
        with open(join(SAMPLE_DIR, 'index_page.html'), 'r') as f:
            assert_equal(
                'http://gcloud.civilservice.gov.uk/files/2013/11/'
                'G-Cloud-Total-Spend-22-11-13.csv',
                parse_latest_csv_url(f))


class SpendingDataTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        with open(join(
                SAMPLE_DIR,
                'G-Cloud Spend 10-04-14 v1 For Publication.csv'), 'r') as f:
            cls.rows = list(process_csv(f))

    def test_correct_number_of_rows(self):
        num_rows = len(self.rows)
        assert_equal(9227, num_rows)

    def test_correct_keys(self):
        first_row = self.rows[0]
        assert_equal(
            set([
                'customer_name',
                'customer_sector',
                'supplier_name',
                'supplier_type',
                'lot',
                'month',
                'year',
                'total_ex_vat',
                'date',
            ]),
            set(first_row.keys()))

    def test_correct_number_of_customer_names(self):
        names = set(row['customer_name'] for row in self.rows)
        assert_equal(437, len(names))

    def test_all_valid_customer_sector(self):
        sectors = set(row['customer_sector'] for row in self.rows)
        assert_equal(
            set([
                'central-gov',
                'local-gov',
                'not-for-profit',
                'wider-public-sector',
                'unknown-sector',
            ]),
            sectors)

    def test_expected_number_of_rows_with_unknown_sector(self):
        unknown_sectors = [row['customer_name'] for row in self.rows
                           if row['customer_sector'] == UNKNOWN_SECTOR_KEY]
        assert_equal(7, len(unknown_sectors))

    def test_correct_number_of_supplier_names(self):
        names = set(row['supplier_name'] for row in self.rows)
        assert_equal(300, len(names))

    def test_all_valid_supplier_types(self):
        supplier_types = set(row['supplier_type'] for row in self.rows)
        assert_equal(
            set([
                'sme',
                'large',
            ]),
            supplier_types)

    def test_all_valid_lot_names(self):
        lot_names = set(row['lot'] for row in self.rows)
        assert_equal(set(['iaas', 'paas', 'saas', 'css']), lot_names)

    def test_all_valid_months(self):
        months = set(row['month'] for row in self.rows)
        assert_equal(set([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]), months)

    def test_all_valid_years(self):
        years = set(row['year'] for row in self.rows)
        assert_equal(set([2012, 2013, 2014]), years)

    def test_totals_are_all_floats(self):
        totals = set(row['total_ex_vat'] for row in self.rows)
        for total in totals:
            assert_is_instance(total, float)  # TODO: Decimal?

    def test_totals_add_up_to_correct_amount(self):
        totals = (row['total_ex_vat'] for row in self.rows)
        assert_equal(154635951.7974, sum(totals))


class AggregationTest(unittest.TestCase):
    def test_make_spending_group_keys(self):
        # 5 sectors x 2 sme|large x 4 lots x 6 month+year combos
        with mock.patch('scraperwiki.sqlite.select') as mocked:
            # mustn't make keys for months 3..12 for 2010
            fake_months_years = [
                {'year': 2009, 'month': 9},
                {'year': 2009, 'month': 10},
                {'year': 2009, 'month': 11},
                {'year': 2009, 'month': 12},
                {'year': 2010, 'month': 1},
                {'year': 2010, 'month': 2},
            ]
            mocked.return_value = fake_months_years

            all_keys = list(make_spending_group_keys())
            assert_equal(sorted(list(set(all_keys))), sorted(all_keys))
            assert_equal(
                5 * 2 * 4 * 6,
                len(list(all_keys))
            )

    def test_make_company_type(self):
        # 2 sme|large x 6 month+year combos
        with mock.patch('scraperwiki.sqlite.select') as mocked:
            # mustn't make keys for months 3..12 for 2010
            fake_months_years = [
                {'year': 2009, 'month': 9},
                {'year': 2009, 'month': 10},
                {'year': 2009, 'month': 11},
                {'year': 2009, 'month': 12},
                {'year': 2010, 'month': 1},
                {'year': 2010, 'month': 2},
            ]
            mocked.return_value = fake_months_years

            all_keys = list(make_company_type_keys())
            assert_equal(sorted(list(set(all_keys))), sorted(all_keys))
            assert_equal(
                2 * 6,
                len(list(all_keys))
            )

    def test_distinct_month_years(self):
        with mock.patch('scraperwiki.sqlite.select') as mocked:
            # mustn't make keys for months 3..12 for 2010
            fake_months_years = [
                {'year': 2009, 'month': 9},
                {'year': 2009, 'month': 10},
                {'year': 2009, 'month': 11},
                {'year': 2009, 'month': 12},
                {'year': 2010, 'month': 1},
                {'year': 2010, 'month': 2},
            ]
            mocked.return_value = fake_months_years
            assert_equal(
                [
                    (9, 2009),
                    (10, 2009),
                    (11, 2009),
                    (12, 2009),
                    (1, 2010),
                    (2, 2010),
                ],
                list(get_distinct_month_years()))

    def test_make_aggregated_sales_row(self):
        fake_key = SpendingGroupKey(
            month=1,
            year=2009,
            lot='paas',
            sector='central-gov',
            sme_large='sme')

        result = make_aggregated_sales_row(fake_key, 1234.56, 5, 4567.89, 10)
        assert_equal([
            '_id',
            '_timestamp',
            'lot',
            'sector',
            'sme_large',
            'monthly_spend',
            'count',
            'cumulative_spend',
            'cumulative_count'],
            result.keys())
        # TODO: test the values

    def test_get_monthly_spend_and_count(self):
        fake_key = SpendingGroupKey(
            month=1,
            year=2009,
            lot='paas',
            sector='central-gov',
            sme_large='sme')

        with mock.patch('scraperwiki.sqlite.select') as mocked:
            mocked.return_value = [
                {'total_spend': 1234.56,
                 'invoice_count': 3}]

            spend, count = get_monthly_spend_and_count(fake_key)
            expected_sql = ('ROUND(SUM(total_ex_vat), 2) AS total_spend, '
                            'COUNT(*) AS invoice_count '
                            'FROM raw_sales '
                            'WHERE year=2009 AND month=1 AND lot="paas" '
                            'AND customer_sector="central-gov" '
                            'AND supplier_type="sme"')
            assert_equal(
                [mock.call(expected_sql)],
                mocked.call_args_list)

        assert_equal(1234.56, spend)
        assert_equal(3, count)

    def test_get_cumulative_spend(self):
        fake_key = SpendingGroupKey(
            month=1,
            year=2009,
            lot="paas",
            sector='central-gov',
            sme_large='sme')

        with mock.patch('scraperwiki.sqlite.select') as mocked:
            mocked.return_value = [{'total': 1234.56}]

            spend = get_cumulative_spend(fake_key)
            expected_sql = ('ROUND(SUM(total_ex_vat), 2) AS total '
                            'FROM raw_sales '
                            'WHERE date <= "2009-01-01" '
                            'AND lot="paas" AND customer_sector="central-gov" '
                            'AND supplier_type="sme"')

            assert_equal(
                [mock.call(expected_sql)],
                mocked.call_args_list)

        assert_equal(1234.56, spend)

    def test_get_cumulative_count(self):
        fake_key = SpendingGroupKey(
            month=1,
            year=2009,
            lot='paas',
            sector='central-gov',
            sme_large='sme')

        with mock.patch('scraperwiki.sqlite.select') as mocked:
            mocked.return_value = [{'total': 17}]

            count = get_cumulative_count(fake_key)
            expected_sql = ('COUNT(*) AS total FROM raw_sales '
                            'WHERE date <= "2009-01-01" AND lot="paas" AND '
                            'customer_sector="central-gov" '
                            'AND supplier_type="sme"')

            assert_equal(
                [mock.call(expected_sql)],
                mocked.call_args_list)

        assert_equal(17, count)
