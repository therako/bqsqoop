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
        _e = ElasticSearchExtractor({})
        self.assertEqual(_e._no_of_workers, 1)
        self.assertEqual(_e._timeout, '60s')
        self.assertEqual(_e._scroll_size, 1000)
        self.assertEqual(_e._fields, ['_all'])
        self.assertEqual(_e._output_folder, './F43C2651')


class TestExtractToParquet(unittest.TestCase):
    @patch('bqsqoop.extractor.elasticsearch.helper.ESHelper')
    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_async_extract_call(self, async_worker, es_helper):
        _e = ElasticSearchExtractor(_valid_config)
        _mock_worker = MagicMock()
        async_worker.return_value = _mock_worker
        _mock_worker.get_job_results = MagicMock(return_value=["file1.parq"])

        self.assertEqual(_e.extract_to_parquet(), ["file1.parq"])
