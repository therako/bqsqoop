import pytest
import unittest

from unittest.mock import patch, MagicMock, call
from bqsqoop.utils.gcloud.storage import (
    copy_files_to_gcs, _get_details_from_gcs_path
)


class TestCopyFiles(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    def test_valid_gcs_path(self, storage_client):
        _files = ["file1", "file2"]
        _valid_path = "gs://gcs_bucket"
        copy_files_to_gcs(_files, _valid_path)

    @patch('google.cloud.storage.Client')
    def test_invalid_gcs_path(self, storage_client):
        _files = ["file1", "file2"]
        _invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            copy_files_to_gcs(_files, _invalid_path)
        _invalid_path = "gs://"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            copy_files_to_gcs(_files, _invalid_path)

    def test__get_details_from_gcs_path(self):
        # Tmp GCS bucket path
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket/")
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "")
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket")
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "")
        # With a tmp root folder in bucket
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket/tmp_space")
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "tmp_space/")
        # Tmp GCS bucket path
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://")
        self.assertEqual(_bucket_name, "")
        self.assertEqual(_sub_folder, "")

    @patch('google.cloud.storage.Client')
    def test_file_uploads(self, storage_client):
        _gcs_bucket_path = "gs://gcs_bucket/tmp_path/"
        _files = ["file1", "file2"]
        _mock_storage = MagicMock()
        storage_client.return_value = _mock_storage
        _mock_blob = MagicMock()
        _mock_bucket = MagicMock()
        _mock_bucket.blob = MagicMock(return_value=_mock_blob)
        _mock_storage.get_bucket = MagicMock(return_value=_mock_bucket)
        _mock_blob.upload_from_filename = MagicMock()

        copy_files_to_gcs(_files, _gcs_bucket_path)
        storage_client.assert_called()
        _mock_storage.get_bucket.assert_called_with("gcs_bucket")
        _call_args = _mock_bucket.blob.call_args_list
        self.assertEqual(
            _call_args, [call("tmp_path/file1"), call("tmp_path/file2")])
        _call_args = _mock_blob.upload_from_filename.call_args_list
        self.assertEqual(
            _call_args, [call(filename='file1'), call(filename='file2')])
