import pytest
import unittest

from unittest.mock import patch, MagicMock, call
from bqsqoop.utils.gcloud.storage import (
    copy_files_to_gcs, _get_details_from_gcs_path, delete_files_in
)


class TestCommonHelpers(unittest.TestCase):
    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    def test__get_details_from_gcs_path(self, mock_uuid):
        # Tmp GCS bucket path
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket/", False)
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "")
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket", False)
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "")
        # With a tmp root folder in bucket
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket/tmp_space", False)
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(_sub_folder, "tmp_space/")
        # Tmp GCS bucket path
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://", False)
        self.assertEqual(_bucket_name, "")
        self.assertEqual(_sub_folder, "")
        # Create new tmp folder under GCS bucket
        _bucket_name, _sub_folder = _get_details_from_gcs_path(
            "gs://gcs_bucket/tmp_space", True)
        self.assertEqual(_bucket_name, "gcs_bucket")
        self.assertEqual(
            _sub_folder, "tmp_space/F43C2651-18C8-4EB0-82D2-10E3C7226015/")


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

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('google.cloud.storage.Client')
    def test_file_uploads(self, storage_client, mock_uuid):
        _gcs_bucket_path = "gs://gcs_bucket/tmp_path/"
        _files = ["file1", "file2"]
        _mock_storage = MagicMock()
        storage_client.return_value = _mock_storage
        _mock_blob = MagicMock()
        _mock_bucket = MagicMock()
        _mock_bucket.blob = MagicMock(return_value=_mock_blob)
        _mock_storage.get_bucket = MagicMock(return_value=_mock_bucket)
        _mock_blob.upload_from_filename = MagicMock()

        copy_files_to_gcs(_files, _gcs_bucket_path, True)
        storage_client.assert_called()
        _mock_storage.get_bucket.assert_called_with("gcs_bucket")
        _call_args = _mock_bucket.blob.call_args_list
        self.assertEqual(
            _call_args,
            [call("tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/file1"),
             call("tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/file2")])
        _call_args = _mock_blob.upload_from_filename.call_args_list
        self.assertEqual(
            _call_args, [call(filename='file1'), call(filename='file2')])


class TestDeleteFilesIn(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    def test_valid_gcs_path(self, storage_client):
        _valid_path = "gs://gcs_bucket"
        delete_files_in(_valid_path)

    @patch('google.cloud.storage.Client')
    def test_invalid_gcs_path(self, storage_client):
        _invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            delete_files_in(_invalid_path)
        _invalid_path = "gs://"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            delete_files_in(_invalid_path)

    @patch('google.cloud.storage.Client')
    def test_file_uploads(self, storage_client):
        _gcs_bucket_path = "gs://gcs_bucket/tmp_path/tmp2/"
        _mock_storage = MagicMock()
        storage_client.return_value = _mock_storage
        _mock_blobs = [MagicMock(), MagicMock()]
        _mock_bucket = MagicMock()
        _mock_bucket.list_blobs = MagicMock(return_value=_mock_blobs)
        _mock_storage.get_bucket = MagicMock(return_value=_mock_bucket)

        delete_files_in(_gcs_bucket_path, "*.log")
        storage_client.assert_called()
        _mock_storage.get_bucket.assert_called_with("gcs_bucket")
        _mock_bucket.list_blobs.assert_called_with(prefix="tmp_path/tmp2/",
                                                   delimiter="*.log")
        _mock_blobs[0].delete.assert_called_once_with()
        _mock_blobs[1].delete.assert_called_once_with()
