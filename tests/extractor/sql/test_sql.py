import pytest
import unittest

from unittest.mock import patch, MagicMock
from bqsqoop.utils.errors import MissingConfigError
from bqsqoop.extractor.sql import SQLExtractor


_valid_config = {'sql_bind': 'sql_bind', 'query': 'query %s',
                 'no_of_workers': 2, 'filter_field': 'filter_field'}


class TestConfig(unittest.TestCase):
    def test_with_minimum_required_configs(self):
        e = SQLExtractor(_valid_config)
        # No error raised
        e.validate_config()

    def test_no_sql_bind(self):
        _config = {}
        e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* sql_bind .* SQL'):
            e.validate_config()

    def test_no_index(self):
        _config = {'sql_bind': 'sql_bind'}
        e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* query .* SQL'):
            e.validate_config()

    def test_filter_field_not_required_with_one_worker(self):
        _config = {'sql_bind': 'sql_bind',
                   'query': 'query', 'no_of_workers': 1}
        e = SQLExtractor(_config)
        e.validate_config()

    def test_no_filter_field_with_more_worker(self):
        _config = {'sql_bind': 'sql_bind',
                   'query': 'query', 'no_of_workers': 2}
        e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* filter_field .* SQL'):
            e.validate_config()

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    def test_default_confgs(self, mock_uuid):
        e = SQLExtractor(_valid_config)
        self.assertEqual(e._no_of_workers, 2)
        self.assertEqual(e._output_folder, './F43C2651')


class TestExtractToParquet(unittest.TestCase):
    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.extractor.sql.helper.get_results_cursor')
    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_async_extract_call(self, async_worker, get_results_cursor,
                                mock_uuid):
        _valid_config['no_of_workers'] = 2
        e = SQLExtractor(_valid_config)
        mock_results = MagicMock()
        get_results_cursor.return_value = mock_results
        mock_results.fetchone.return_value = [3, 6]
        mock_worker = MagicMock()
        async_worker.return_value = mock_worker
        mock_worker.send_data_to_worker = MagicMock()
        mock_job_results = MagicMock(return_value=["file1.parq"])
        mock_worker.get_job_results = mock_job_results

        self.assertEqual(e.extract_to_parquet(), ["file1.parq"])
        async_worker.assert_called_with(2)
        mock_worker.send_data_to_worker.assert_called()
        _call_args = mock_worker.send_data_to_worker.call_args_list
        _, kwargs1 = _call_args[0]
        self.assertEqual(kwargs1['worker_id'], 0)
        self.assertEqual(
            kwargs1['worker_callback'].__name__, 'export_to_parquet')
        self.assertEqual(kwargs1['sql_bind'], 'sql_bind')
        self.assertEqual(kwargs1['query'], 'query %s')
        self.assertEqual(kwargs1['filter_field'], 'filter_field')
        self.assertEqual(kwargs1['output_folder'], './F43C2651')
        self.assertEqual(kwargs1['start_pos'], 3)
        self.assertEqual(kwargs1['end_pos'], 4)
        _, kwargs2 = _call_args[1]
        self.assertEqual(kwargs2['worker_id'], 1)
        self.assertEqual(
            kwargs2['worker_callback'].__name__, 'export_to_parquet')
        self.assertEqual(kwargs2['sql_bind'], 'sql_bind')
        self.assertEqual(kwargs2['query'], 'query %s')
        self.assertEqual(kwargs2['filter_field'], 'filter_field')
        self.assertEqual(kwargs2['output_folder'], './F43C2651')
        self.assertEqual(kwargs2['start_pos'], 5)
        self.assertEqual(kwargs2['end_pos'], 6)
        mock_job_results.assert_called_with()

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.extractor.sql.helper.export_to_parquet')
    def test_sync_extract_call_for_single_worker(self, export_to_parquet,
                                                 mock_uuid):
        _valid_config['no_of_workers'] = 1
        e = SQLExtractor(_valid_config)
        export_to_parquet.return_value = "file1.parq"

        self.assertEqual(e.extract_to_parquet(), ["file1.parq"])
        export_to_parquet.assert_called_with(
            worker_id=0, filter_field='filter_field',
            output_folder='./F43C2651', query='query %s', sql_bind='sql_bind',
            start_pos=None, end_pos=None,
        )
