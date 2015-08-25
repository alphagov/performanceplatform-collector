from nose.tools import *
from hamcrest import equal_to, assert_that
from tests.performanceplatform.collector.tools \
    import temp_file, json_file, capture_stderr
from performanceplatform.collector.arguments import parse_args


class TestParseArgs(object):
    def test_happy_path(self):
        with json_file({}) as config_path:
            args = parse_args(
                args=["-c", config_path, "-l", "test-collector-slug",
                      "-t", config_path, "-b", config_path]
            )

            assert_that(args.collector_slug, equal_to("test-collector-slug"))

    def test_both_query_file_and_collector_slug_not_allowed(self):
        with json_file({}) as config_path:
            with json_file({}) as query_path:
                assert_raises(
                    SystemExit, parse_args, args=["-c", config_path,
                                                  "-l", "test-collector-slug",
                                                  "-q", query_path,
                                                  "-t", config_path,
                                                  "-b", config_path])

    def test_query_file_is_a_valid_option(self):
        with json_file({}) as config_path:
            with json_file({}) as query_path:
                parse_args(
                    args=["-c", config_path, "-q", query_path,
                          "-t", config_path, "-b", config_path]
                )

    def test_collector_slug_or_query_file_is_required(self):
        with json_file({}) as config_path:
            assert_raises(
                SystemExit, parse_args, args=["-c", config_path,
                                              "-t", config_path,
                                              "-b", config_path])

    def test_config_path_is_required(self):
        with json_file({}) as query_path:
            assert_raises(
                SystemExit, parse_args, args=["-l", "test-collector-slug",
                                              "-t", query_path,
                                              "-b", query_path])

    def test_start_and_end_fields_are_allowed(self):
        with json_file({}) as config_path:
            parse_args(
                args=[
                    "-c", config_path, "-l", "test-collector-slug",
                    "-t", config_path, "-b", config_path,
                    "-s", "2013-10-10", "-e", "2013-10-10"])

    def test_start_at_must_be_parsable_as_a_date(self):
        with json_file({}) as config_path:
            assert_raises(
                SystemExit, parse_args, args=[
                    "-c", config_path,
                    "-l", "test-collector-slug",
                    "-t", config_path, "-b", config_path,
                    "-s", "not-a-date"])

    def test_end_at_must_be_parsable_as_a_date(self):
        with json_file({}) as config_path:
            assert_raises(
                SystemExit, parse_args, args=[
                    "-c", config_path,
                    "-l", "test-collector-slug",
                    "-t", config_path, "-b", config_path,
                    "-e", "not-a-date"])

    def test_config_file_must_be_json(self):
        with capture_stderr() as stderr:
            with temp_file("not json") as config_path:
                args = ["-c", config_path]
                assert_raises(
                    SystemExit, parse_args, args=args)
                ok_("invalid _load_json_file value" in stderr.getvalue())
