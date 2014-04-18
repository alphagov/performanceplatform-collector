import subprocess
from nose.tools import *
from hamcrest import *
import sys
from backdrop.collector.crontab import remove_existing_crontab_for_app

from tests.tools import temp_file
from backdrop.collector import crontab


def test_jobs_are_not_removed_for_other_apps():
    crontab = [
        '# Begin backdrop.collector jobs for my-app',
        'foobar',
        '# End backdrop.collector jobs for my-app',
        '# Begin backdrop.collector jobs for other-app',
        'barfoo',
        '# End backdrop.collector jobs for other-app',
    ]
    new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
    assert_that(new_crontab,
                has_item(contains_string('barfoo')))
    assert_that(new_crontab,
                is_not(has_item(contains_string('foobar'))))


def test_crontab_end_before_begin():
    crontab = [
        '# End backdrop.collector jobs for my-app',
        'foobar',
        '# Begin backdrop.collector jobs for my-app',
        'other'
    ]
    new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
    assert_that(new_crontab,
                has_item(contains_string('foobar')))
    assert_that(new_crontab,
                is_not(has_item(contains_string('other'))))


def test_crontab_begin_with_no_end():
    crontab = [
        'foobar',
        '# Begin backdrop.collector jobs for my-app',
        'other',
    ]
    new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
    assert_that(new_crontab,
                has_item(contains_string('foobar')))
    assert_that(new_crontab,
                is_not(has_item(contains_string('other'))))


class TestGenerateCrontab(object):
    def test_other_cronjobs_are_preserved(self):
        with temp_file("") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                ['other cronjob'],
                jobs_path,
                "/path/to/app",
                "some_id")
            assert_that(generated_jobs, has_item("other cronjob"))

    def test_some_cronjobs_are_added_between_containing_comments(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/my-app",
                'some_id')
            assert_that(generated_jobs,
                        has_item('# Begin backdrop.collector jobs for some_id'))

            assert_that(generated_jobs,
                        has_item(contains_string("schedule")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-q /path/to/my-app/query.json")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-c /path/to/my-app/config.json")))

            assert_that(generated_jobs,
                        has_item('# End backdrop.collector jobs for some_id'))

    def test_added_jobs_run_the_crontab_module(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/app",
                "some_id")
            assert_that(generated_jobs,
                        has_item(
                            contains_string("collect.py")))

    def test_existing_backdrop_cronjobs_are_purged(self):
        with temp_file("schedule,query.json,config.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [
                    '# Begin backdrop.collector jobs for some_id',
                    'other job',
                    '# End backdrop.collector jobs for some_id'
                ],
                jobs_path,
                "/path/to/my-app",
                'some_id')
            assert_that(generated_jobs,
                        is_not(has_item(contains_string("other job"))))

    def test_invalid_jobs_file_causes_failure(self):
        with temp_file("afeg    ;efhv   2[\-r u1\-ry1r") as jobs_path:
            assert_raises(crontab.ParseError,
                          crontab.generate_crontab,
                          [],
                          jobs_path,
                          "/path/to/app",
                          "some_id")

    def test_can_use_id_for_generating_crontab_entries(self):
        with temp_file("something, something, dark side") as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )
            assert_that(generated_jobs,
                        has_item('# Begin backdrop.collector jobs '
                                 'for unique-id-of-my-app'))
            assert_that(generated_jobs,
                        has_item('# End backdrop.collector jobs '
                                 'for unique-id-of-my-app'))

    def test_can_overide_collection_script_to_use(self):
        temp_contents = "schedule,query,config,custom-collect.py"
        with temp_file(temp_contents) as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )
            job_contains = "/custom-collect.py -q /path/to/my-app/query"
            assert_that(generated_jobs,
                        has_item(
                            contains_string(job_contains)))


class ProcessFailureError(StandardError):
    def __init__(self, code, command, output):
        self.code = code
        self.command = command
        self.output = output


class TestCrontabScript(object):
    def run_crontab_script(self, current_crontab, path_to_app, path_to_jobs,
                           unique_id):
        with temp_file(current_crontab) as stdin_path:
            args = [
                sys.executable,
                '-m', 'backdrop.collector.crontab',
                path_to_app, path_to_jobs, unique_id
            ]
            # Bleh Python 2.6 :(
            proc = subprocess.Popen(args,
                                    stdin=open(stdin_path),
                                    stderr=subprocess.STDOUT,
                                    stdout=subprocess.PIPE)
            output = proc.communicate()
            if proc.returncode != 0:
                raise ProcessFailureError(
                    proc.returncode, ' '.join(args), output=output[0])
            return output[0]

    def test_happy_path(self):
        with temp_file('') as path_to_jobs:
            output = self.run_crontab_script(
                'current crontab', '/path/to/app', path_to_jobs, 'some_id')
            assert_that(output.strip(),
                        is_('current crontab'))

    def test_with_jobs(self):
        with temp_file('one,two,three') as path_to_jobs:
            output = self.run_crontab_script(
                'current crontab', '/path/to/app', path_to_jobs, 'some_id')

            cronjobs = output.split("\n")

            assert_that(cronjobs,
                        has_item(contains_string(sys.executable)))

    def test_failure_results_in_non_zero_exit_status(self):
        assert_raises(ProcessFailureError,
                      self.run_crontab_script,
                      'current crontab', '/path/to/app', 'invalid path',
                      'some_id')

    def test_failure_returns_original_crontab(self):
        try:
            self.run_crontab_script(
                'current crontab', '/path/to/app', 'invalid path', 'some_id')
        except ProcessFailureError as e:
            assert_that(e.output, contains_string('current crontab'))
