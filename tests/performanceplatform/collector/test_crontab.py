import subprocess
from nose.tools import *
from hamcrest import *
import sys
from performanceplatform.collector.crontab import remove_existing_crontab_for_app

from tests.performanceplatform.collector.tools import temp_file
from performanceplatform.collector import crontab
from mock import patch


class TestRemoveExistingCrontab(object):

    def setUp(self):
        self.patcher = patch('socket.gethostname')
        self.mock_gethostname = self.patcher.start()
        self.mock_gethostname.return_value = 'nonnumerichostname'

    def test_jobs_are_not_removed_for_other_apps(self):
        crontab = [
            '# Begin performanceplatform.collector jobs for my-app',
            'foobar',
            '# End performanceplatform.collector jobs for my-app',
            '# Begin performanceplatform.collector jobs for other-app',
            'barfoo',
            '# End performanceplatform.collector jobs for other-app',
        ]
        new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
        assert_that(new_crontab,
                    has_item(contains_string('barfoo')))
        assert_that(new_crontab,
                    is_not(has_item(contains_string('foobar'))))

    def test_crontab_end_before_begin(self):
        crontab = [
            '# End performanceplatform.collector jobs for my-app',
            'foobar',
            '# Begin performanceplatform.collector jobs for my-app',
            'other'
        ]
        new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
        assert_that(new_crontab,
                    has_item(contains_string('foobar')))
        assert_that(new_crontab,
                    is_not(has_item(contains_string('other'))))

    def test_crontab_begin_with_no_end(self):
        crontab = [
            'foobar',
            '# Begin performanceplatform.collector jobs for my-app',
            'other',
        ]
        new_crontab = remove_existing_crontab_for_app(crontab, 'my-app')
        assert_that(new_crontab,
                    has_item(contains_string('foobar')))
        assert_that(new_crontab,
                    is_not(has_item(contains_string('other'))))

    def tearDown(self):
        self.patcher.stop()


class TestGenerateCrontab(object):

    def setUp(self):
        self.patcher = patch('socket.gethostname')
        self.mock_gethostname = self.patcher.start()
        self.mock_gethostname.return_value = 'nonnumerichostname'

    def test_other_cronjobs_are_preserved(self):
        with temp_file("") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                ['other cronjob'],
                jobs_path,
                "/path/to/app",
                "some_id")
            assert_that(generated_jobs, has_item("other cronjob"))

    def test_some_cronjobs_are_added_between_containing_comments(self):
        with temp_file("schedule,query.json,creds.json,token.json,performanceplatform.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/my-app",
                'some_id')
            assert_that(generated_jobs,
                        has_item('# Begin performanceplatform.collector jobs for some_id'))

            assert_that(generated_jobs,
                        has_item(contains_string("schedule")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-q /path/to/my-app/config/query.json")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-c /path/to/my-app/config/creds.json")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-t /path/to/my-app/config/token.json")))
            assert_that(generated_jobs,
                        has_item(
                            contains_string("-b /path/to/my-app/config/performanceplatform.json")))

            assert_that(generated_jobs,
                        has_item('# End performanceplatform.collector jobs for some_id'))

    def test_added_jobs_run_the_crontab_module(self):
        with temp_file("schedule,query.json,creds.json,token.json,performanceplatform.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [],
                jobs_path,
                "/path/to/my-app",
                "some_id")
            assert_that(generated_jobs,
                        has_item(
                            contains_string("pp-collector")))

    def test_existing_pp_cronjobs_are_purged(self):
        with temp_file("schedule,query.json,creds.json,token.json,performanceplatform.json") as jobs_path:
            generated_jobs = crontab.generate_crontab(
                [
                    '# Begin performanceplatform.collector jobs for some_id',
                    'other job',
                    '# End performanceplatform.collector jobs for some_id'
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
        with temp_file("something, something, something, something, dark side") as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )
            assert_that(generated_jobs,
                        has_item('# Begin performanceplatform.collector jobs '
                                 'for unique-id-of-my-app'))
            assert_that(generated_jobs,
                        has_item('# End performanceplatform.collector jobs '
                                 'for unique-id-of-my-app'))

    def test_can_handle_whitespace_and_comments(self):
        temp_contents = ("# some comment\n"
                         "          \n"
                         "schedule,query,creds,token,performanceplatforn\n")

        with temp_file(temp_contents) as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )
            job_contains = "pp-collector -q /path/to/my-app/config/query"
            assert_that(generated_jobs,
                        has_item(
                            contains_string(job_contains)))

    @patch('socket.gethostname')
    def test_jobs_based_on_hostname_with_1(self, hostname):
        hostname.return_value = 'development-1'

        temp_contents = ("schedule,query,creds,token,performanceplatforn\n"
                         "schedule2,query2,creds2,token2,performanceplatforn\n"
                         "schedule3,query2,creds3,token3,performanceplatforn\n"
                         )

        with temp_file(temp_contents) as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )

            assert_that(
                removeCommentsFromCrontab(generated_jobs), has_length(1))
            assert_that(removeCommentsFromCrontab(
                generated_jobs)[0], contains_string("schedule2 "))

    @patch('socket.gethostname')
    def test_jobs_based_on_hostname_with_2(self, hostname):
        hostname.return_value = 'development-2'

        temp_contents = ("schedule,query,creds,token,performanceplatforn\n"
                         "schedule2,query2,creds2,token2,performanceplatforn\n"
                         "schedule3,query2,creds3,token3,performanceplatforn\n"
                         )

        with temp_file(temp_contents) as something:
            generated_jobs = crontab.generate_crontab(
                [],
                something,
                "/path/to/my-app",
                "unique-id-of-my-app"
            )

            assert_that(
                removeCommentsFromCrontab(generated_jobs), has_length(2))
            assert_that(removeCommentsFromCrontab(
                generated_jobs)[0], contains_string("schedule"))
            assert_that(removeCommentsFromCrontab(
                generated_jobs)[1], contains_string("schedule3 "))

    def tearDown(self):
        self.patcher.stop()


def removeCommentsFromCrontab(generated_jobs):
    return filter(lambda job: not job.startswith("#"), generated_jobs)


class ProcessFailureError(StandardError):

    def __init__(self, code, command, output):
        self.code = code
        self.command = command
        self.output = output


class TestCrontabScript(object):

    def setUp(self):
        self.patcher = patch('socket.gethostname')
        self.mock_gethostname = self.patcher.start()
        self.mock_gethostname.return_value = 'nonnumerichostname'

    def run_crontab_script(self, current_crontab, path_to_app, path_to_jobs,
                           unique_id):
        with temp_file(current_crontab) as stdin_path:
            args = [
                sys.executable,
                '-m', 'performanceplatform.collector.crontab',
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

    def tearDown(self):
        self.patcher.stop()
