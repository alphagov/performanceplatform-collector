import unittest

from hamcrest import assert_that, equal_to

import main


class TestMain(unittest.TestCase):

    def test_merge_backdrop_config(self):
        base = {
            'url': 'http://foo/data',
            'token': 'foo',
        }
        data_set = {
            'data-group': 'group',
            'data-type': 'type'
        }

        merged = main.merge_backdrop_config(base, data_set)

        assert_that(merged['url'], equal_to('http://foo/data/group/type'))
