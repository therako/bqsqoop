import unittest

from bqsqoop.utils import typed
from bqsqoop.utils.errors import MissingDataError, InvalidTypeError


class TestTypedNonEmptyString(unittest.TestCase):
    def test_errors_for_empty_string(self):
        err = typed.non_empty_string(None)
        self.assertEqual(err.__class__, MissingDataError)
        err = typed.non_empty_string("")
        self.assertEqual(err.__class__, MissingDataError)

    def test_invalid_type(self):
        err = typed.non_empty_string(123)
        self.assertEqual(err.__class__, InvalidTypeError)

    def test_valid_case(self):
        err = typed.non_empty_string("hello")
        self.assertIsNone(err)
