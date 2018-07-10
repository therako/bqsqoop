import pytest
import unittest
from bqsqoop.extractor import Extractor


class ValidExtractorClass(Extractor):
    def validate_config(self):
        return True

    def extract_to_parquet(self):
        return []


class InvalidExtractorClass(Extractor):
    pass


class TestIExtractor(unittest.TestCase):
    def test_valid_abc_class_implementation(self):
        # No error raised
        ValidExtractorClass({})

    def test_invalid_abc_class_implementation(self):
        with pytest.raises(
            TypeError,
            match=r'Can\'t instantiate abstract class InvalidExtractorClass ' +
                'with abstract methods .*'):
            InvalidExtractorClass({})

    def test_abstract_method_calls(self):
        _class = ValidExtractorClass({})
        self.assertEqual(True, _class.validate_config())
        self.assertEqual([], _class.extract_to_parquet())
