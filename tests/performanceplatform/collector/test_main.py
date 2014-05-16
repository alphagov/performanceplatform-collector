import unittest

from hamcrest import assert_that, equal_to

from performanceplatform.collector import main


class TestMain(unittest.TestCase):

    def test_merge_performanceplatform_config(self):
        performanceplatform = {
            'url': 'http://foo/data',
        }
        data_set = {
            'data-group': 'group',
            'data-type': 'type'
        }
        token = {
            'token': 'foo',
        }

        merged = main.merge_performanceplatform_config(
            performanceplatform, data_set, token)

        assert_that(merged['url'], equal_to('http://foo/data/group/type'))
        assert_that(merged['dry_run'], equal_to(False))

    def test_merge_performanceplatform_config_with_dry_run(self):
        performanceplatform = {
            'url': 'http://foo/data',
        }
        data_set = {
            'data-group': 'group',
            'data-type': 'type'
        }
        token = {
            'token': 'foo',
        }

        merged = main.merge_performanceplatform_config(
            performanceplatform, data_set, token, dry_run=True)

        assert_that(merged['url'], equal_to('http://foo/data/group/type'))
        assert_that(merged['dry_run'], equal_to(True))
