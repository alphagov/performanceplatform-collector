#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals

import itertools
import datetime
import logging

from collections import OrderedDict, namedtuple

import scraperwiki

from table_names import _RAW_SALES_TABLE

_SECTOR_NAME = {
    'central-gov':         'Central government',
    'local-gov':           'Local government',
    'not-for-profit':      'Not for profit',
    'wider-public-sector': 'Other wider public sector',
    'unknown-sector':      'Unknown',
}

_SME_LARGE_NAME = {
    'sme':               'Small and medium enterprises',
    'large':             'Large enterprises',
}

_LOT_NAME = {
    'iaas': 'Cloud Infrastructure as a Service (IaaS)',
    'paas': 'Cloud Platform as a Service (PaaS)',
    'saas': 'Cloud Software as a Service (SaaS)',
    'css':  'Cloud Support Services (CSS)',
}


class SpendingGroupKey(namedtuple('SpendingGroupKey',
                                  'month,year,lot,sector,sme_large')):
    """
    A 'spending group' is a specific combination of month, year, lot (type),
    government sector and sme/large.
    """

    def __str__(self):
        return str(self.key())

    def __unicode__(self):
        return unicode(self.key())

    def key(self):
        return "{year}_{month:02d}_lot{lot}_{sector}_{sme_large}".format(
            year=self.year, month=self.month, lot=self.lot, sector=self.sector,
            sme_large=self.sme_large)


class CompanyTypeKey(namedtuple('CompanyTypeKey',
                                'month,year,sme_large')):
    def __str__(self):
        return str(self.key())

    def __unicode__(self):
        return unicode(self.key())

    def key(self):
        return "{year}_{month:02d}_{sme_large}".format(
            year=self.year, month=self.month, sme_large=self.sme_large)


def calculate_aggregated_sales(keys):
    for key in keys:
        monthly_spend, transaction_count = get_monthly_spend_and_count(key)
        cumulative_spend = get_cumulative_spend(key)
        cumulative_count = get_cumulative_count(key)
        yield make_aggregated_sales_row(key, monthly_spend, transaction_count,
                                        cumulative_spend, cumulative_count)


def make_aggregated_sales_row(key, monthly_total, transaction_count,
                              cumulative_spend, cumulative_count):
    return OrderedDict([
        ('_id', unicode(key)),
        ('_timestamp', datetime.datetime(key.year, key.month, 1)),
        ('lot', _LOT_NAME[key.lot]),
        ('sector', _SECTOR_NAME[key.sector]),
        ('sme_large', _SME_LARGE_NAME[key.sme_large]),
        ('monthly_spend', monthly_total),
        ('count', transaction_count),
        ('cumulative_spend', cumulative_spend),
        ('cumulative_count', cumulative_count),
    ])


def get_distinct_month_years():
    for row in scraperwiki.sqlite.select(
            'month, year FROM {table} GROUP BY year, month'
            ' ORDER BY year, month'.format(table=_RAW_SALES_TABLE)):
        yield (row['month'], row['year'])


def get_monthly_spend_and_count(key):
    query = ('ROUND(SUM(total_ex_vat), 2) AS total_spend, '
             'COUNT(*) AS invoice_count '
             'FROM {table} '
             'WHERE year={year} '
             'AND month={month} '
             'AND lot="{lot}" '
             'AND customer_sector="{sector}" '
             'AND supplier_type="{sme_large}"'.format(
                 table=_RAW_SALES_TABLE,
                 year=key.year,
                 month=key.month,
                 lot=key.lot,
                 sector=key.sector,
                 sme_large=key.sme_large))
    logging.debug(query)
    result = scraperwiki.sqlite.select(query)[0]
    logging.debug(result)

    spend, count = 0.0, 0

    if result['total_spend'] is not None:
        spend = float(result['total_spend'])

    if result['invoice_count'] is not None:
        count = int(result['invoice_count'])

    return (spend, count)


def get_cumulative_spend(key):
    """
    Get the sum of spending for this category up to and including the given
    month.
    """
    query = ('ROUND(SUM(total_ex_vat), 2) AS total '
             'FROM {table} '
             'WHERE date <= "{year}-{month:02}-01" '
             'AND lot="{lot}" '
             'AND customer_sector="{sector}" '
             'AND supplier_type="{sme_large}"'.format(
                 table=_RAW_SALES_TABLE,
                 year=key.year,
                 month=key.month,
                 lot=key.lot,
                 sector=key.sector,
                 sme_large=key.sme_large))
    logging.debug(query)
    result = scraperwiki.sqlite.select(query)
    logging.debug(result)
    value = result[0]['total']
    return float(result[0]['total']) if value is not None else 0.0


def get_cumulative_count(key):
    """
    Get the sum of spending for this category up to and including the given
    month.
    """
    query = ('COUNT(*) AS total '
             'FROM {table} '
             'WHERE date <= "{year}-{month:02}-01" '
             'AND lot="{lot}" '
             'AND customer_sector="{sector}" '
             'AND supplier_type="{sme_large}"'.format(
                 table=_RAW_SALES_TABLE,
                 year=key.year,
                 month=key.month,
                 lot=key.lot,
                 sector=key.sector,
                 sme_large=key.sme_large))
    logging.debug(query)
    result = scraperwiki.sqlite.select(query)
    logging.debug(result)
    value = result[0]['total']
    return float(result[0]['total']) if value is not None else 0


def make_spending_group_keys():

    month_and_years = get_distinct_month_years()

    all_lots = _LOT_NAME.keys()              # ie [1, 2, 3, 4]
    all_sectors = _SECTOR_NAME.keys()        # ie ['central-gov', 'local-gov']
    all_sme_large = _SME_LARGE_NAME.keys()   # ie ['sme', 'large']

    for (month, year), lot, sector, sme_large in itertools.product(
            month_and_years, all_lots, all_sectors, all_sme_large):

        yield SpendingGroupKey(month=month, year=year, lot=lot, sector=sector,
                               sme_large=sme_large)


def make_company_type_keys():
    month_and_years = get_distinct_month_years()

    all_sme_large = _SME_LARGE_NAME.keys()   # ie ['sme', 'large']
    for (month, year), sme_large in itertools.product(
            month_and_years, all_sme_large):
        yield CompanyTypeKey(month=month, year=year, sme_large=sme_large)
