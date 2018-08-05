import pytest
import unittest

from unittest.mock import patch, MagicMock
from bqsqoop.utils.errors import MissingConfigError
from bqsqoop.extractor.elasticsearch import ElasticSearchExtractor


_valid_config = {'url': 'es_endpoint', 'index': 'some_es_index'}


class TestConfig(unittest.TestCase):
    def test_with_minimum_required_configs(self):
        _e = ElasticSearchExtractor(_valid_config)
        # No error raised
        _e.validate_config()

    def test_no_url(self):
        _config = {'index': 'some_es_index'}
        _e = ElasticSearchExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* url .* Elasticsearch'):
            _e.validate_config()

    def test_no_index(self):
        _config = {'url': 'es_endpoint'}
        _e = ElasticSearchExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* index .* Elasticsearch'):
            _e.validate_config()

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    def test_default_confgs(self, mock_uuid):
        _e = ElasticSearchExtractor(_valid_config)
        self.assertEqual(_e._no_of_workers, 1)
        self.assertEqual(_e._timeout, '60s')
        self.assertEqual(_e._scroll_size, 1000)
        self.assertEqual(_e._fields, ['_all'])
        self.assertEqual(_e._output_folder, './F43C2651')


class TestExtractToParquet(unittest.TestCase):
    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.extractor.elasticsearch.helper.ESHelper')
    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_async_extract_call(self, async_worker, es_helper, mock_uuid):
        _valid_config['no_of_workers'] = 2
        _e = ElasticSearchExtractor(_valid_config)
        es_helper.get_fields = MagicMock()
        es_helper.get_fields.return_value = {"fieldA": "int", "fieldB": "bool"}
        _mock_worker = MagicMock()
        async_worker.return_value = _mock_worker
        _mock_send_data_to_worker = MagicMock()
        _mock_worker.send_data_to_worker = _mock_send_data_to_worker
        _mock_job_results = MagicMock(return_value=["file1.parq"])
        _mock_worker.get_job_results = _mock_job_results
        _search_args = {
            'index': 'some_es_index', 'scroll': '60s',
            'size': 1000, 'body': {'query': {'match_all': {}}},
            '_source_include': 'fieldA,fieldB'
        }

        self.assertEqual(_e.extract_to_parquet(), ["file1.parq"])
        async_worker.assert_called_with(2)
        _mock_send_data_to_worker.assert_called()
        _call_args = _mock_send_data_to_worker.call_args_list
        _, _kwargs1 = _call_args[0]
        self.assertEqual(_kwargs1['worker_id'], 0)
        self.assertEqual(_kwargs1['es_hosts'], 'es_endpoint')
        self.assertEqual(_kwargs1['es_timeout'], '60s')
        self.assertEqual(_kwargs1['output_folder'], './F43C2651')
        self.assertEqual(_kwargs1['search_args'], _search_args)
        self.assertEqual(_kwargs1['total_worker_count'], 2)
        _, _kwargs2 = _call_args[1]
        self.assertEqual(_kwargs2['worker_id'], 1)
        self.assertEqual(_kwargs2['es_hosts'], 'es_endpoint')
        self.assertEqual(_kwargs2['es_timeout'], '60s')
        self.assertEqual(_kwargs2['output_folder'], './F43C2651')
        self.assertEqual(_kwargs2['search_args'], _search_args)
        self.assertEqual(_kwargs2['total_worker_count'], 2)
        _mock_job_results.assert_called_with()
        es_helper.get_fields.assert_called_with(
            'es_endpoint', 'some_es_index')

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.extractor.elasticsearch.helper.ESHelper')
    def test_extract_specific_fields_with_single_worker(
            self, es_helper, mock_uuid):
        config = _valid_config.copy()
        config['no_of_workers'] = 1
        config['fields'] = ['field1', 'field2']
        _e = ElasticSearchExtractor(config)
        es_helper.get_fields = MagicMock()
        es_helper.get_fields.return_value = {
            "field1": "int", "field2": "bool", "ignored_field": "text"}
        es_helper.scroll_and_extract_data.return_value = "file1.parq"

        self.assertEqual(_e.extract_to_parquet(), ["file1.parq"])
        es_helper.scroll_and_extract_data.assert_called_with(
            es_hosts='es_endpoint', es_timeout='60s',
            fields={
                "field1": "int", "field2": "bool"},
            search_args={
                'index': 'some_es_index', 'scroll': '60s', 'size': 1000,
                'body': {'query': {'match_all': {}}},
                '_source_include': 'field1,field2'},
            total_worker_count=1,
            worker_id=0,
            output_folder='./F43C2651',
            type_cast={}
        )

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.extractor.elasticsearch.helper.ESHelper')
    def test_custom_datetime_format_and_type_cast(self, es_helper, mock_uuid):
        config = _valid_config.copy()
        config['no_of_workers'] = 1
        config['fields'] = ['field1', 'field2']
        config['datetime_format'] = "some datetime_format"
        config['type_cast'] = {
            'field1': "string"
        }
        _e = ElasticSearchExtractor(config)
        es_helper.get_fields = MagicMock()
        es_helper.get_fields.return_value = {
            "field1": "int", "field2": "bool", "ignored_field": "text"}
        es_helper.scroll_and_extract_data.return_value = "file1.parq"

        self.assertEqual(_e.extract_to_parquet(), ["file1.parq"])
        es_helper.scroll_and_extract_data.assert_called_with(
            es_hosts='es_endpoint', es_timeout='60s',
            fields={
                "field1": "int", "field2": "bool"},
            search_args={
                'index': 'some_es_index', 'scroll': '60s', 'size': 1000,
                'body': {'query': {'match_all': {}}},
                '_source_include': 'field1,field2'},
            total_worker_count=1,
            worker_id=0,
            output_folder='./F43C2651',
            type_cast={
                'field1': "string"
            },
            datetime_format='some datetime_format'
        )
