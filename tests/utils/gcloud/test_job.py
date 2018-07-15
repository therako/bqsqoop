import unittest
from mock import patch
from bqsqoop.utils.gcloud.job import BigqueryParquetLoadJob


class TestBigqueryParquetLoadJob(unittest.TestCase):
    def test_init_invalid_configs(self):
        _configs = {}
        _job = BigqueryParquetLoadJob(_configs)
        self.assertFalse(_job.is_config_valid)
        self.assertEqual(list(_job.errors.keys()), [
                         "_project_id", "_dataset_name",
                         "_table_name", "_gcs_tmp_path"])

    def test_valid_configs(self):
        _configs = dict(
            project_id="gcp_project_1",
            dataset_name="dataset_1",
            table_name="table_1",
            gcs_tmp_path="gcs_tmp_path",
        )
        _job = BigqueryParquetLoadJob(_configs)
        self.assertTrue(_job.is_config_valid)
        self.assertDictEqual(_job.errors, {})

    @patch('bqsqoop.utils.gcloud.auth.setup_credentials')
    @patch('bqsqoop.utils.gcloud.storage.copy_files_to_gcs')
    @patch('bqsqoop.utils.gcloud.bigquery.load_parquet_files')
    @patch('bqsqoop.utils.gcloud.storage.delete_files_in')
    def test_execute(self, delete_files_in, load_parquet_files,
                     copy_files_to_gcs, setup_credentials):
        _configs = dict(
            project_id="gcp_project_1",
            dataset_name="dataset_1",
            table_name="table_1",
            gcs_tmp_path="gcs_tmp_path",
            service_account_key="{'a': 'b@c.com'}",
            write_truncate=False
        )
        _files = ["file1", "file2"]
        copy_files_to_gcs.return_value = "gs://gcs_tmp_path/sacdf/"

        _job = BigqueryParquetLoadJob(_configs)
        _job.execute(_files)
        setup_credentials.assert_called_with(_configs["service_account_key"])
        copy_files_to_gcs.assert_called_with(_files, _configs["gcs_tmp_path"],
                                             use_new_tmp_folder=True)
        load_parquet_files.assert_called_with(
            copy_files_to_gcs.return_value + "*.parq",
            _configs["project_id"],
            _configs["dataset_name"],
            _configs["table_name"],
            write_truncate=False
        )
        delete_files_in.assert_called_with(copy_files_to_gcs.return_value)
