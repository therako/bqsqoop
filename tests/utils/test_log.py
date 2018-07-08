import logging
import sys
import unittest
from contextlib import contextmanager
from io import StringIO
from testfixtures import LogCapture
from bqsqoop.utils.log import setup as log_setup


@contextmanager
def captured_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err


class TestLog(unittest.TestCase):
    def test_log_setup(self):
        with LogCapture() as lc:
            with captured_output() as (out, err):
                log_setup(True)
                logging.info('a message')
                lc.check(
                    ('root', 'DEBUG', 'Verbose logging enabled'),
                    ('root', 'INFO', 'a message')
                )
                output = err.getvalue().strip()
                self.assertTrue(
                    '[MainThread  ] [DEBUG]  Verbose logging enabled'
                    in output)
                self.assertTrue(
                    '[MainThread  ] [INFO ]  a message' in output)
