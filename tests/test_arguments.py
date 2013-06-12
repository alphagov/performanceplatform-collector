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