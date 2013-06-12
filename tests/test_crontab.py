import subprocess
from nose.tools import *
from hamcrest import *
import sys

from tests.tools import temp_file
from backdrop.collector import crontab


class TestGenerateCrontab(object):
    def test_other_cronjobs_are_preserved(self):
        with temp_file("") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                ['other cronjob'],
                jobs_path,
                "/path/to/app")
            assert_that(generated_jobs, has_item("other cronjob"))

    def test_some_cronjobs_are_added(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/app")
            assert_that(generated_jobs,
                        has_item(contains_string("schedule")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-q /path/to/app/query.json")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-c /path/to/app/config.json")))

    def test_added_jobs_run_the_crontab_module(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/app")
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-m backdrop.collector.crontab")))

    def test_added_jobs_get_tagged_with_comment(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/app")

            assert_that(generated_jobs,
                        has_item(
                            ends_with('# app')))

    def test_existing_tagged_cronjobs_are_purged(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                ['other job # app-name'],
                jobs_path,
                "/path/to/app")
            assert_that(generated_jobs,
                        is_not(has_item(contains_string("other job"))))

    def test_invalid_jobs_file_causes_failure(self):
        with temp_file("afeg    ;efhv   2[\-r u1\-ry1r") as jobs_path:
            assert_raises(crontab.ParseError,
                          crontab.generate_crontab,
                          [],
                          jobs_path,
                          "/path/to/app")


class TestCrontabScript(object):
    def run_crontab_script(self, current_crontab, path_to_app, path_to_jobs):
        with temp_file(current_crontab) as stdin_path:
            args = [
                sys.executable,
                '-m', 'backdrop.collector.crontab',
                path_to_app, path_to_jobs
            ]
            return subprocess.check_output(
                args,
                stdin=open(stdin_path),
                stderr=subprocess.STDOUT)

    def test_happy_path(self):
        with temp_file('') as path_to_jobs:
            output = self.run_crontab_script(
                'current crontab', '/path/to/app', path_to_jobs)
            assert_that(output.strip(),
                        is_('current crontab'))

    def test_with_jobs(self):
        with temp_file('one,two,three') as path_to_jobs:
            output = self.run_crontab_script(
                'current crontab', '/path/to/app', path_to_jobs)

            cronjobs = output.split("\n")
            assert_that(cronjobs,
                        has_item(ends_with("# app")))

            assert_that(cronjobs,
                        has_item(contains_string(sys.executable)))

    def test_failure_results_in_non_zero_exit_status(self):
        assert_raises(subprocess.CalledProcessError,
                      self.run_crontab_script,
                      'current crontab', '/path/to/app', 'invalid path')

    def test_failure_returns_original_crontab(self):
        try:
            self.run_crontab_script(
                'current crontab', '/path/to/app', 'invalid path')
        except subprocess.CalledProcessError as e:
            assert_that(e.output, contains_string('current crontab'))
