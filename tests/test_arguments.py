from StringIO import StringIO
from contextlib import contextmanager
import json
import os
from tempfile import mkstemp
from nose.tools import *
import sys
from backdrop.collector.arguments import parse_args

@contextmanager
def temp_file(contents):
    fd, filename = mkstemp()
    os.write(fd, contents)
    yield filename
    os.close(fd)
    os.remove(filename)


@contextmanager
def json_file(contents):
    with temp_file(json.dumps(contents)) as filename:
        yield filename


@contextmanager
def capture_stderr():
    old_stderr = sys.stderr
    new_stderr = StringIO()
    sys.stderr = new_stderr
    yield new_stderr
    sys.stderr = old_stderr
    new_stderr.close()


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