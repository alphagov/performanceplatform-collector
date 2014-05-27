#!/usr/bin/env python
# encoding: utf-8

from __future__ import unicode_literals

import codecs
import json
import logging
import os
import re
import sys

import scraperwiki
from dshelpers import (update_status, download_url, install_cache,
                       batch_processor)

from backdrop.collector.write import Bucket
from backdrop.collector.write import JsonEncoder

from sales_parser import process_csv, get_latest_csv_url
from table_names import _RAW_SALES_TABLE, _AGGREGATED_SALES_TABLE
from aggregate import calculate_aggregated_sales, make_spending_group_keys


INDEX_URL = ('https://digitalmarketplace.blog.gov.uk'
             '/sales-accreditation-information/')


def main(filename=None):
    logging.basicConfig(level=logging.INFO)
    install_cache()

    nuke_local_database()

    if filename:
        with open(filename, 'r') as f:
            save_raw_data(f)
    else:
        save_raw_data(download_url(get_latest_csv_url(INDEX_URL)))

    aggregate_and_save()

    save_to_json()

    push_aggregates()

    update_status(_RAW_SALES_TABLE, 'date')


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


def push_aggregates():
    table = _AGGREGATED_SALES_TABLE
    url = _SALES_WRITE_URL
    token = _SALES_TOKEN

    bucket = Bucket(url, token)

    with batch_processor(bucket.post) as uploader:
        for row in scraperwiki.sqlite.select('* FROM {}'.format(table)):
            uploader.push(row)


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


if __name__ == '__main__':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

    _SALES_WRITE_URL = os.environ['GCLOUD_SALES_WRITE_URL']
    _SALES_TOKEN = os.environ['GCLOUD_SALES_BEARER_TOKEN']

    if len(sys.argv) > 1:
        main(sys.argv[1])
    else:
        main()
