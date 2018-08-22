import unittest

from unittest.mock import patch, MagicMock, call
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
    @patch('bqsqoop.utils.pandas_util.PandasUtil')
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('elasticsearch.Elasticsearch')
    def test_scroll_and_extract_data(self, elasticsearch, parquet_util,
                                     pandas_util, uuid):
        _mock_es = MagicMock()
        elasticsearch.return_value = _mock_es
        _search_args = {'index': 'some_es_index'}
        _fields = {
            'field1': 'text',
            'field2': 'long',
            'field3': 'date'
        }
        _search_result = {
            '_scroll_id': '_scroll_id1',
            'hits': {
                'total': 1200,
                'hits': [{
                    '_index': 'some_es_index',
                    '_source': {
                        'field1': 'value1',
                        'field2': 2,
                        'field3': 'some_date'
                    }
                }]
            }
        }
        _mock_es.search = MagicMock(return_value=_search_result)
        _mock_es.scroll = MagicMock(return_value={})

        _mock_parquet_util = MagicMock()
        parquet_util.return_value = _mock_parquet_util
        parquet_util.fix_dataframe_for_schema.side_effect = [
            "df_after_corrections"
        ]
        _mock_parquet_util.build_pyarrow_schema.return_value = "pyarrow_schema"

        _output_file = ESHelper.scroll_and_extract_data(
            worker_id=0, total_worker_count=1, es_hosts=['url'],
            es_timeout='60s', search_args=_search_args.copy(), fields=_fields,
            output_folder='_output_folder',
            type_cast={'field2': "string"}
        )
        self.assertEqual(_output_file,
                         "_output_folder/some_es_index_F43C2651.parq")

        elasticsearch.assert_called_with(['url'])
        _mock_es.search.assert_called_once()
        _args, _kwargs = _mock_es.search.call_args_list[0]
        self.assertEquals(_kwargs, {'index': 'some_es_index'})
        _mock_es.scroll.assert_called_once_with(
            scroll='60s', scroll_id='_scroll_id1')
        parquet_util.assert_called_with(
            '_output_folder/some_es_index_F43C2651.parq')
        _mock_parquet_util.build_pyarrow_schema.assert_called_with(
            {'field1': 'text', 'field2': 'long', 'field3': 'date'}
        )
        _mock_parquet_util.append_df_to_parquet.assert_called_once_with(
            "df_after_corrections", schema='pyarrow_schema')

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.pandas_util.PandasUtil')
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('elasticsearch.Elasticsearch')
    def test_scroll_untill_no_data_left(
            self, elasticsearch, parquet_util, pandas_util, uuid):
        _mock_es = MagicMock()
        elasticsearch.return_value = _mock_es
        _search_args = {'index': 'some_es_index'}
        _fields = {'field1': 'text'}
        _search_result = {
            '_scroll_id': '_scroll_id1',
            'hits': {
                'total': 3,
                'hits': [{
                    '_index': 'some_es_index',
                    '_source': {
                        'field1': 'value1',
                        'field2': 2
                    }
                }]
            }
        }
        _scroll_result1 = {
            '_scroll_id': '_scroll_id1',
            'hits': {
                'total': 3,
                'hits': [{
                    '_index': 'some_es_index',
                    '_source': {
                        'field1': 'value2',
                        'field2': 3
                    }
                }]
            }
        }
        _scroll_result2 = {
            '_scroll_id': '_scroll_id1',
            'hits': {
                'total': 3,
                'hits': [{
                    '_index': 'some_es_index',
                    '_source': {
                        'field1': 'value3',
                        'field2': 4
                    }
                }]
            }
        }
        _mock_es.search = MagicMock(return_value=_search_result)
        _mock_es.scroll = MagicMock(
            side_effect=[_scroll_result1, _scroll_result2, {}])

        _mock_parquet_util = MagicMock()
        parquet_util.return_value = _mock_parquet_util
        _mock_parquet_util.build_pyarrow_schema.return_value = "pyarrow_schema"
        parquet_util.fix_dataframe_for_schema.side_effect = [
            "df_after_corrections1", "df_after_corrections2",
            "df_after_corrections3"
        ]

        ESHelper.scroll_and_extract_data(
            worker_id=0, total_worker_count=1, es_hosts=['url'],
            es_timeout='60s', search_args=_search_args.copy(), fields=_fields,
            output_folder='_output_folder'
        )
        _mock_es.search.assert_called_once()
        _args, _kwargs = _mock_es.search.call_args_list[0]
        self.assertEquals(_kwargs, {'index': 'some_es_index'})
        self.assertEqual(_mock_es.scroll.call_count, 3)
        _mock_parquet_util.build_pyarrow_schema.assert_called_with(
            {'field1': 'text'}
        )
        _mock_es.scroll.assert_called_with(
            scroll='60s', scroll_id='_scroll_id1')
        _mock_parquet_util.append_df_to_parquet.assert_has_calls([
            call("df_after_corrections1", schema='pyarrow_schema'),
            call("df_after_corrections2", schema='pyarrow_schema'),
            call("df_after_corrections3", schema='pyarrow_schema')
        ])

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.pandas_util.PandasUtil')
    @patch('bqsqoop.utils.parquet_util.ParquetUtil')
    @patch('elasticsearch.Elasticsearch')
    def test_slicing_for_sharded_calls(
            self, elasticsearch, parquet_util, pandas_util, uuid):
        _mock_es = MagicMock()
        elasticsearch.return_value = _mock_es
        _search_args = {
            'index': 'some_es_index',
            'body': {}
        }
        _fields = {'field1': 'date'}
        _search_result = {}
        _mock_es.search = MagicMock(return_value=_search_result)

        _mock_parquet_util = MagicMock()
        parquet_util.return_value = _mock_parquet_util
        parquet_util.fix_dataframe_for_schema = self.mock_fix_dataframe_for_schema  # noqa
        _mock_parquet_util.build_pyarrow_schema.return_value = "pyarrow_schema"
        ESHelper.scroll_and_extract_data(
            worker_id=1, total_worker_count=2, es_hosts=['url'],
            es_timeout='60s', search_args=_search_args.copy(), fields=_fields,
            output_folder='_output_folder'
        )
        _mock_es.search.assert_called_once()
        _args, _kwargs = _mock_es.search.call_args_list[0]
        self.assertEquals(
            _kwargs,
            {'body': {'slice': {'id': 1, 'max': 2}}, 'index': 'some_es_index'})

    def mock_fix_dataframe_for_schema(self, df, arrow_schema,
                                      datetime_format=None):
        return df
