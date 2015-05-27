import unittest
from nose.tools import assert_raises
from hamcrest import assert_that, equal_to
from performanceplatform.collector.ga.contrib.content.table import \
    get_department


class ContribContentTableTestCase(unittest.TestCase):
    def test_can_pass_custom_value_to_get_department(self):
        filters = ["customVarValue9=~^<D1>"]

        department = get_department(filters)

        assert_that(department, equal_to("attorney-generals-office"))

    def test_can_pass_organisation_to_get_department(self):
        filters = ["Organisation=~^<D1>"]

        department = get_department(filters)

        assert_that(department, equal_to("attorney-generals-office"))

    def test_get_value_error_for_invalid_filter(self):
        filters = ["customVarValue9=<D1>"]

        assert_raises(ValueError, get_department, filters)
