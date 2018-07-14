import pytest
import unittest

from unittest.mock import patch, MagicMock
from bqsqoop.utils.gcloud.bigquery import load_parquet_files


class TestloadParquetFilesFromGCS(unittest.TestCase):
    @patch('google.cloud.bigquery.Client')
    def test_valid_gcs_path(self, bigquery_client):
        _valid_path = "gs://gcs_bucket/tmp_folder/*.parq"
        load_parquet_files(_valid_path, "", "", "")

    @patch('google.cloud.bigquery.Client')
    def test_invalid_gcs_path(self, bigquery_client):
        _invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception,
                match=r'Not a valid GCS files path for .parq files'):
            load_parquet_files(_invalid_path, "", "", "")
        _invalid_path = "gs://"
        with pytest.raises(
                Exception,
                match=r'Not a valid GCS files path for .parq files'):
            load_parquet_files(_invalid_path, "", "", "")
        _invalid_path = "gs://gcs_bucket/tmp_folder/*.csv"
        with pytest.raises(
                Exception,
                match=r'Not a valid GCS files path for .parq files'):
            load_parquet_files(_invalid_path, "", "", "")

    @patch('google.cloud.bigquery.LoadJobConfig')
    @patch('google.cloud.bigquery.Client')
    def test_load_parquet_files(self, bigquery_client, load_job_config):
        _gcs_path = "gs://gcs_bucket/tmp_folder/*.parq"
        _gcs_project = "gcs_project"
        _dataset_name = "dataset_name"
        _table_name = "table_name"

        _mock_bigquery = MagicMock()
        bigquery_client.return_value = _mock_bigquery
        _mock_job_config = MagicMock()
        load_job_config.return_value = _mock_job_config
        _mock_dataset = MagicMock()
        _mock_bigquery.dataset.return_value = _mock_dataset
        _mock_table = MagicMock()
        _mock_dataset.table.return_value = _mock_table
        _mock_job = MagicMock()
        _mock_bigquery.load_table_from_uri.return_value = _mock_job

        load_parquet_files(_gcs_path, _gcs_project, _dataset_name, _table_name)

        bigquery_client.assert_called_with(project=_gcs_project)
        load_job_config.assert_called()
        self.assertEqual(_mock_job_config.source_format, "PARQUET")
        # Default should be WRITE_TRUNCATE
        self.assertEqual(_mock_job_config.write_disposition, "WRITE_TRUNCATE")
        _mock_bigquery.dataset.assert_called_with(_dataset_name)
        _mock_dataset.table.assert_called_with(_table_name)
        _mock_bigquery.load_table_from_uri.assert_called_with(
            _gcs_path, _mock_table, job_config=_mock_job_config)
        _mock_job.result.assert_called()

    @patch('google.cloud.bigquery.LoadJobConfig')
    @patch('google.cloud.bigquery.Client')
    def test_load_parquet_files_write_append(self, bigquery_client,
                                             load_job_config):
        _gcs_path = "gs://gcs_bucket/tmp_folder/*.parq"
        _mock_job_config = MagicMock()
        load_job_config.return_value = _mock_job_config
        load_parquet_files(_gcs_path, "", "", "", write_truncate=False)
        load_job_config.assert_called()
        self.assertEqual(_mock_job_config.source_format, "PARQUET")
        self.assertEqual(_mock_job_config.write_disposition, "WRITE_APPEND")
