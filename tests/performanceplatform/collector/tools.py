from StringIO import StringIO
from contextlib import contextmanager
import json
import os
import sys
from tempfile import mkstemp


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
