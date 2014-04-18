from hamcrest import assert_that, has_entries
import os
import logging
import json
from backdrop.collector.logging_setup import set_up_logging
import unittest


class TestJsonLogging(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()

    def test_json_log_written_when_logger_called(self):

        set_up_logging('collector_foo', logging.DEBUG, './log')
        logging.info('Writing out JSON formatted logs m8')

        with open('log/collector.log.json') as log_file:
            data = json.loads(log_file.readlines()[-1])

        assert_that(data, has_entries({
            '@message': 'Writing out JSON formatted logs m8'
        }))

        assert_that(data, has_entries({
            '@tags': ['collector', 'collector_foo']
        }))

        # Only remove file if assertion passes
        os.remove('log/collector.log.json')
        os.remove('log/collector.log')
