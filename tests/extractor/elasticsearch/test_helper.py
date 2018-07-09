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
        _mock_es.indices.get_mapping.assert_called_with(index='some_es_index')

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('elasticsearch.Elasticsearch')
    def test_scroll_and_extract_data(self, elasticsearch, parquet_util, uuid):
        _mock_es = MagicMock()
        elasticsearch.return_value = _mock_es
        _search_args = {'index': 'some_es_index'}
        _fields = {'field1': 'text'}
        _search_result = {
            '_scroll_id': '_scroll_id1',
            'hits': {
                'total': 1200,
                'hits': [{
                    '_index': 'some_es_index',
                    '_source': {
                        'field1': 'value1',
                        'field2': 2
                    }
                }]
            }
        }
        _mock_es.search = MagicMock(return_value=_search_result)
        _mock_es.scroll = MagicMock(return_value={})

        _mock_parquet_util = MagicMock()
        parquet_util.return_value = _mock_parquet_util

        _output_file = ESHelper.scroll_and_extract_data(
            worker_id=0, total_worker_count=1, es_hosts=['url'],
            es_timeout='60s', search_args=_search_args, fields=_fields,
            output_folder='_output_folder'
        )
        self.assertEqual(_output_file,
                         "_output_folder/some_es_index_F43C2651.parq")

        elasticsearch.assert_called_with(['url'])
        _mock_es.search.assert_called_with(**_search_args)
        _mock_es.scroll.assert_called_with(
            scroll='60s', scroll_id='_scroll_id1')
        parquet_util.assert_called_with(
            '_output_folder/some_es_index_F43C2651.parq')
        _call_list = _mock_parquet_util.append_df_to_parquet.call_args_list
        _args, _ = _call_list[0]
        _df = _args[0]
        print(_df.to_json())
        self.assertEqual(
            _df.to_json(), '{"field1":{"0":"value1"},"field2":{"0":2}}')
