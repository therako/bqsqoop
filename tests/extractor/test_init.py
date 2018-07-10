import unittest
import pytest

from bqsqoop.extractor.elasticsearch import ElasticSearchExtractor
from bqsqoop.extractor import get_extractor_for


class TestGetExtractorFor(unittest.TestCase):
    def test_error_of_no_extractor_given(self):
        _config = {'extractor': {}}
        with pytest.raises(
                Exception, match=r'Job takes only one extractor, given'):
            get_extractor_for(_config)

    def test_error_on_unknown_extractor(self):
        _config = {'extractor': {'unknown_extractor': {}}}
        with pytest.raises(
                Exception, match=r'Unknown extractor given'):
            get_extractor_for(_config)

    def test_get_es_extractor(self):
        _config = {'extractor': {'es': {}}}
        _class = get_extractor_for(_config)
        self.assertEquals(type(_class), ElasticSearchExtractor)

    def test_get_elasticsearch_extractor(self):
        _config = {'extractor': {'elasticsearch': {}}}
        _class = get_extractor_for(_config)
        self.assertEquals(type(_class), ElasticSearchExtractor)
