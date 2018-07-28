import pytest
import unittest

from unittest.mock import patch
from bqsqoop.utils.errors import MissingConfigError
from bqsqoop.extractor.sql import SQLExtractor


_valid_config = {'sql_bind': 'sql_bind', 'query': 'query',
                 'no_of_workers': 2, 'filter_field': 'filter_field'}


class TestConfig(unittest.TestCase):
    def test_with_minimum_required_configs(self):
        _e = SQLExtractor(_valid_config)
        # No error raised
        _e.validate_config()

    def test_no_sql_bind(self):
        _config = {}
        _e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* sql_bind .* SQL'):
            _e.validate_config()

    def test_no_index(self):
        _config = {'sql_bind': 'sql_bind'}
        _e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* query .* SQL'):
            _e.validate_config()

    def test_filter_field_not_required_with_one_worker(self):
        _config = {'sql_bind': 'sql_bind',
                   'query': 'query', 'no_of_workers': 1}
        _e = SQLExtractor(_config)
        _e.validate_config()

    def test_no_filter_field_with_more_worker(self):
        _config = {'sql_bind': 'sql_bind',
                   'query': 'query', 'no_of_workers': 2}
        _e = SQLExtractor(_config)
        with pytest.raises(
                MissingConfigError, match=r'.* filter_field .* SQL'):
            _e.validate_config()

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    def test_default_confgs(self, mock_uuid):
        _e = SQLExtractor(_valid_config)
        self.assertEqual(_e._no_of_workers, 2)
        self.assertEqual(_e._output_folder, './F43C2651')
