from nose.tools import *
from tools import temp_file, json_file, capture_stderr
from backdrop.collector.arguments import parse_args


class TestParseArgs(object):
    def test_happy_path(self):
        with json_file({}) as credentials_path:
            with json_file({}) as query_path:
                parse_args(
                    args=["-c", credentials_path, "-q", query_path]
                )

    def test_query_arg_is_required(self):
        with json_file({}) as credentials_path:
            assert_raises(
                SystemExit, parse_args, args=["-c", credentials_path])

    def test_credentials_path_is_required(self):
        with json_file({}) as query_path:
            assert_raises(
                SystemExit, parse_args, args=["-q", query_path])

    def test_start_and_end_fields_are_allowed(self):
        with json_file({}) as credentials_path:
            with json_file({}) as query_path:
                parse_args(
                    args=[
                        "-c", credentials_path, "-q", query_path,
                        "-s", "2013-10-10", "-e", "2013-10-10"])

    def test_start_at_must_be_parsable_as_a_date(self):
        with json_file({}) as credentials_path:
            with json_file({}) as query_path:
                assert_raises(
                    SystemExit, parse_args, args=[
                        "-c", credentials_path, "-q", query_path,
                        "-s", "not-a-date"])

    def test_end_at_must_be_parsable_as_a_date(self):
        with json_file({}) as credentials_path:
            with json_file({}) as query_path:
                assert_raises(
                    SystemExit, parse_args, args=[
                        "-c", credentials_path, "-q", query_path,
                        "-e", "not-a-date"])

    def test_query_file_must_be_json(self):
        with capture_stderr() as stderr:
            with temp_file("not json") as query_path:
                assert_raises(
                    SystemExit, parse_args, args=["-q", query_path])
                ok_("invalid _load_json_file value" in stderr.getvalue())

    def test_credentials_file_must_be_json(self):
        with capture_stderr() as stderr:
            with temp_file("not json") as credentials_path:
                with json_file({}) as query_path:
                    args = ["-c", credentials_path, "-q", query_path]
                    assert_raises(
                        SystemExit, parse_args, args=args)
                    ok_("invalid _load_json_file value" in stderr.getvalue())
