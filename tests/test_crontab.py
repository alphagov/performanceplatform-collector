import unittest
from hamcrest import *

from tests.tools import temp_file
from backdrop.collector import crontab


class TestManageCrontab(unittest.TestCase):
    def test_other_cronjobs_are_preserved(self):
        with temp_file("") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                "app-name",
                ['other cronjob'],
                jobs_path,
                "", "")
            assert_that(generated_jobs, has_item("other cronjob"))

    def test_some_cronjobs_are_added(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                "app-name",
                [],
                jobs_path,
                "app", "python")
            assert_that(generated_jobs,
                        has_item(contains_string("schedule")))
            assert_that(generated_jobs,
                        has_item(contains_string("-q app/query.json")))
            assert_that(generated_jobs,
                        has_item(contains_string("-c app/config.json")))

    def test_added_jobs_get_tagged_with_comment(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                "app-name",
                [],
                jobs_path,
                "app", "python")

            assert_that(generated_jobs,
                        has_item(
                            ends_with('# app-name')))

    def test_existing_tagged_cronjobs_are_purged(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                "app-name",
                ['other job # app-name'],
                jobs_path,
                "app", "python"
            )
            assert_that(generated_jobs,
                        is_not(has_item(contains_string("other job"))))

    def test_invalid_jobs_file_causes_failure(self):
        with temp_file("afeg    ;efhv   2[\-r u1\-ry1r") as jobs_path:
            self.assertRaises(crontab.ParseError,
                              crontab.generate_crontab,
                              "app-name",
                              [],
                              jobs_path,
                              "app", "python")
