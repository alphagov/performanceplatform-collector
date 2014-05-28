#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals

import datetime
import logging
import re

from collections import OrderedDict

import lxml.html
import unicodecsv
from dshelpers import download_url


_ENCODING = 'cp1252'

UNKNOWN_SECTOR_KEY = 'unknown-sector'

_SECTOR_KEY = {
    'Central Government':       'central-gov',

    'Local Government':         'local-gov',

    'Not for Profit':           'not-for-profit',

    'Health':                   'wider-public-sector',
    'Fire and Rescue':          'wider-public-sector',
    'Devolved Administrations': 'wider-public-sector',
    'Education':                'wider-public-sector',
    'Police':                   'wider-public-sector',
    'Defence':                  'wider-public-sector',
    'Wider Public Sector':      'wider-public-sector',
    'Utility (Historic)':       'wider-public-sector',

    'Private Sector':           UNKNOWN_SECTOR_KEY,
}

_SME_LARGE_KEY = {
    'SME': 'sme',
    'Large': 'large',
}

_LOT_KEY = {
    'Infrastructure as a Service (IaaS)': 'iaas',
    'Platform as a Service (PaaS)': 'paas',
    'Software as a Service (SaaS)': 'saas',
    'Specialist Cloud Services': 'css'
}


def get_latest_csv_url(index_page_url):
    """
    Download the index page and extract the URL of the current CSV.
    """
    index_fobj = download_url(index_page_url)
    csv_url = parse_latest_csv_url(index_fobj)
    logging.info("Latest CSV URL: {}".format(csv_url))
    return csv_url


def parse_latest_csv_url(index_fobj):
    root = lxml.html.fromstring(index_fobj.read())
    elements = root.xpath("//*[contains(text(), 'Current')]"
                          "/a[contains(@href, '.csv')]/@href")
    assert len(elements) == 1, elements
    return elements[0]


def process_csv(f):
    """
    Take a file-like object and yield OrderedDicts to be inserted into raw
    spending database.
    """
    reader = unicodecsv.DictReader(f, encoding=_ENCODING)
    for row in reader:
        month, year = parse_month_year(row['Return Month'])

        yield OrderedDict([
            ('customer_name', row['CustomerName']),
            ('supplier_name', row['SupplierName']),
            ('month', month),
            ('year', year),
            ('date', datetime.date(year, month, 1)),
            ('total_ex_vat', parse_price(row['EvidencedSpend'])),
            ('lot', parse_lot_name(row['LotDescription'])),
            ('customer_sector', parse_customer_sector(row['Sector'])),
            ('supplier_type', parse_sme_or_large(row['SME or Large'])),
        ])


def parse_month_year(date_string):
    """
    >>> parse_month_year('01/10/2012')
    (10, 2012)
    """
    match = re.match('\d{2}/(?P<month>\d{2})/(?P<year>\d{4})$',
                     date_string.lower())
    if not match:
        raise ValueError("Not format 'dd/mm/yyyy': '{}'".format(date_string))
    month = int(match.group('month'))
    year = int(match.group('year'))
    return month, year


def parse_price(price_string):
    """
    >>> parse_price('£16,000.00')
    16000.0
    >>> parse_price('-£16,000.00')
    -16000.0
    """
    return float(re.sub('[£,]', '', price_string))

    match = re.match('(?P<amount>-?£[\d,]+(\.\d{2})?)', price_string)
    if not match:
        raise ValueError("Charge not in format '(-)£16,000(.00)' : {}".format(
            repr(price_string)))
    return float(re.sub('[£,]', '', match.group('amount')))


def parse_customer_sector(raw_sector):
    if raw_sector not in _SECTOR_KEY:
        logging.warning('Unknown sector: "{}"'.format(raw_sector))
    return _SECTOR_KEY.get(raw_sector, UNKNOWN_SECTOR_KEY)


def parse_sme_or_large(raw_sme_large):
    try:
        return _SME_LARGE_KEY[raw_sme_large]
    except KeyError:
        raise RuntimeError('Unknown sme/large: "{}"'.format(raw_sme_large))


def parse_lot_name(raw_lot_name):
    return _LOT_KEY[raw_lot_name]
