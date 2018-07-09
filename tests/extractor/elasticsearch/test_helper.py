import unittest

from unittest.mock import patch, MagicMock
from bqsqoop.extractor.elasticsearch.helper import ESHelper


class TestESHelper(unittest.TestCase):
    @patch('elasticsearch.Elasticsearch')
    def test_get_fields(self, elasticsearch):
        _mock_es = MagicMock()
        elasticsearch.return_value = _mock_es
        _mappings = {
            'some_es_index': {
                'mappings': {
                    'some_es_index': {
                        'properties': {
                            'field1': {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            'field2': {
                                "type": "long"
                            },
                            'field3': {
                                "type": "date"
                            }
                        }
                    }
                }
            }
        }
        _mock_es.indices.get_mapping = MagicMock(return_value=_mappings)

        _fields = ESHelper.get_fields('es_endpoint', 'some_es_index')
        self.assertEqual(_fields, {
            'field1': 'text', 'field2': 'long', 'field3': 'date'})
