# encoding: utf-8

from __future__ import unicode_literals

import json
import logging
import re

import scraperwiki
from dshelpers import batch_processor
from dateutil.parser import parse as parse_datetime

from performanceplatform.client.base import JsonEncoder

from sales_parser import process_csv
from table_names import _RAW_SALES_TABLE, _AGGREGATED_SALES_TABLE
from aggregate import calculate_aggregated_sales, make_spending_group_keys


def nuke_local_database():
    for table in (_RAW_SALES_TABLE, _AGGREGATED_SALES_TABLE):
        _clear_database_table(table)


def save_raw_data(fobj):
    def save(rows):
        scraperwiki.sqlite.save(
            unique_keys=[],
            data=rows,
            table_name=_RAW_SALES_TABLE)

    with batch_processor(save) as b:
        for row in process_csv(fobj):
            logging.debug(','.join(['{}'.format(v) for v in row.values()]))
            b.push(row)


def aggregate_and_save():
    def save(rows):
        scraperwiki.sqlite.save(
            table_name=_AGGREGATED_SALES_TABLE,
            data=rows,
            unique_keys=['_id'])

    with batch_processor(save) as saver:
        for row in calculate_aggregated_sales(make_spending_group_keys()):
            saver.push(row)


def push_aggregates(data_set):
    table = _AGGREGATED_SALES_TABLE

    with batch_processor(data_set.post) as uploader:
        for row in scraperwiki.sqlite.select('* FROM {}'.format(table)):
            uploader.push(format_timestamp(row))


def format_timestamp(row):
    """
    >>> format_timestamp({'_timestamp': '2012-12-12 00:00'})
    {u'_timestamp': u'2012-12-12T00:00:00Z'}
    """
    row['_timestamp'] = parse_datetime(row['_timestamp']).isoformat() + 'Z'
    return row


def save_to_json():
    table = _AGGREGATED_SALES_TABLE
    with open('{}.json'.format(table), 'w') as f:
        records = [row for row in scraperwiki.sqlite.select(
                   '* FROM {}'.format(table))]

        f.write(json.dumps(records, cls=JsonEncoder, indent=1))


def _clear_database_table(table_name):
    logging.info("Dropping table '{}'".format(table_name))
    scraperwiki.sqlite.execute('DROP TABLE IF EXISTS "{}"'.format(table_name))
    scraperwiki.sqlite.commit()


def normalise(name):
    """
    >>> normalise('Foo  Bar Ltd.')
    u'foo bar ltd'
    >>> normalise('BAZ JIM LTD')
    u'baz jim ltd'
    >>> normalise('BAZ-JIM (South) LTD.')
    u'baz jim south ltd'
    """
    return re.sub('[^a-z]+', ' ', name.lower()).strip()
