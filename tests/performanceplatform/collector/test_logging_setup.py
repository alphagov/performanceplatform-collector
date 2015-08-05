from hamcrest import assert_that, has_entries
import os
import logging
import json
from performanceplatform.collector.logging_setup import (
    set_up_logging,
    extra_fields_from_exception)
import unittest


class TestJsonLogging(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()

    def test_json_log_written_when_logger_called(self):

        set_up_logging('collector_foo', logging.DEBUG, './log')
        logging.info('Writing out JSON formatted logs m8')

        with open('log/production.json.log') as log_file:
            data = json.loads(log_file.readlines()[-1])

        assert_that(data, has_entries({
            '@message': 'Writing out JSON formatted logs m8'
        }))

        assert_that(data, has_entries({
            '@tags': ['collector', 'collector_foo']
        }))

        # Only remove file if assertion passes
        os.remove('log/production.json.log')
        os.remove('log/production.log')

    def test_extra_fields_from_exception(self):
        try:
            raise Exception('test')
        except Exception as e:
            assert_that(extra_fields_from_exception(e), has_entries({
                'exception_class': 'Exception',
                'exception_message': 'test'
            }))
