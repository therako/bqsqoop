import pytest
import unittest

from unittest.mock import patch, MagicMock, call
from bqsqoop.utils.gcloud.storage import (
    copy_files_to_gcs, _get_details_from_gcs_path, delete_files_in,
    download_file_as_string, parallel_copy_files_to_gcs
)


class TestCommonHelpers(unittest.TestCase):
    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    def test__get_details_from_gcs_path(self, mock_uuid):
        # Tmp GCS bucket path
        bucket_name, folder_path, _ = _get_details_from_gcs_path(
            "gs://gcs_bucket/", False)
        self.assertEqual(bucket_name, "gcs_bucket")
        self.assertEqual(folder_path, "")
        bucket_name, folder_path, _ = _get_details_from_gcs_path(
            "gs://gcs_bucket", False)
        self.assertEqual(bucket_name, "gcs_bucket")
        self.assertEqual(folder_path, "")
        # With a tmp root folder in bucket
        bucket_name, folder_path, _ = _get_details_from_gcs_path(
            "gs://gcs_bucket/tmp_space", False)
        self.assertEqual(bucket_name, "gcs_bucket")
        self.assertEqual(folder_path, "tmp_space/")
        # Tmp GCS bucket path
        bucket_name, folder_path, _ = _get_details_from_gcs_path(
            "gs://", False)
        self.assertEqual(bucket_name, "")
        self.assertEqual(folder_path, "")
        # Create new tmp folder under GCS bucket
        bucket_name, folder_path, _ = _get_details_from_gcs_path(
            "gs://gcs_bucket/tmp_space", True)
        self.assertEqual(bucket_name, "gcs_bucket")
        self.assertEqual(
            folder_path, "tmp_space/F43C2651-18C8-4EB0-82D2-10E3C7226015/")


class TestCopyFiles(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    def test_valid_gcs_path(self, storage_client):
        files = ["file1", "file2"]
        gcs_project = "gcs_project_1"
        valid_path = "gs://gcs_bucket"
        copy_files_to_gcs(files, valid_path, gcs_project)

    @patch('google.cloud.storage.Client')
    def test_invalid_gcs_path(self, storage_client):
        files = ["file1", "file2"]
        gcs_project = "gcs_project_1"
        invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            copy_files_to_gcs(files, invalid_path, gcs_project)
        invalid_path = "gs://"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            copy_files_to_gcs(files, invalid_path, gcs_project)

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('google.cloud.storage.Client')
    def test_file_uploads(self, storage_client, mock_uuid):
        gcs_bucket_path = "gs://gcs_bucket/tmp_path/"
        files = ["file1", "file2"]
        mock_storage = MagicMock()
        storage_client.return_value = mock_storage
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob = MagicMock(return_value=mock_blob)
        mock_storage.get_bucket = MagicMock(return_value=mock_bucket)
        mock_blob.upload_from_filename = MagicMock()

        copy_files_to_gcs(files, gcs_bucket_path, "gcs_project_1", True)
        storage_client.assert_called_with(project="gcs_project_1")
        mock_storage.get_bucket.assert_called_with("gcs_bucket")
        call_args = mock_bucket.blob.call_args_list
        self.assertEqual(
            call_args,
            [call("tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/file1"),
             call("tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/file2")])
        call_args = mock_blob.upload_from_filename.call_args_list
        self.assertEqual(
            call_args, [call(filename='file1'), call(filename='file2')])


class TestParallelCopyFiles(unittest.TestCase):
    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_valid_gcs_path(self, async_worker):
        files = ["file1", "file2"]
        gcs_project = "gcs_project_1"
        valid_path = "gs://gcs_bucket"
        parallel_copy_files_to_gcs(files, valid_path, gcs_project)

    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_invalid_gcs_path(self, async_worker):
        files = ["file1", "file2"]
        gcs_project = "gcs_project_1"
        invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            parallel_copy_files_to_gcs(files, invalid_path, gcs_project)
        invalid_path = "gs://"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            parallel_copy_files_to_gcs(files, invalid_path, gcs_project)

    @patch('uuid.uuid4', return_value="F43C2651-18C8-4EB0-82D2-10E3C7226015")
    @patch('bqsqoop.utils.async_worker.AsyncWorker')
    def test_file_uploads(self, async_worker, mock_uuid):
        gcs_bucket_path = "gs://gcs_bucket/tmp_path/"
        files = ["file1", "file2"]
        mock_worker = MagicMock()
        async_worker.return_value = mock_worker
        mock_worker.send_data_to_worker = MagicMock()

        gcs_path = parallel_copy_files_to_gcs(
            files, gcs_bucket_path, "gcs_project_1", True)
        async_worker.assert_called_with(2)
        self.assertEqual(
            gcs_path,
            "gs://gcs_bucket/tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/")
        call_args = mock_worker.send_data_to_worker.call_args_list
        _, kwargs = call_args[0]
        self.assertEqual(kwargs['files'], ['file1'])
        self.assertEqual(
            kwargs['gcs_bucket_path'],
            "gs://gcs_bucket/tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/")
        self.assertEqual(kwargs['project_id'], 'gcs_project_1')
        self.assertEqual(kwargs['use_new_tmp_folder'], False)
        self.assertEqual(
            kwargs['worker_callback'].__name__, "copy_files_to_gcs")
        _, kwargs = call_args[1]
        self.assertEqual(kwargs['files'], ['file2'])
        self.assertEqual(
            kwargs['gcs_bucket_path'],
            "gs://gcs_bucket/tmp_path/F43C2651-18C8-4EB0-82D2-10E3C7226015/")
        self.assertEqual(kwargs['project_id'], 'gcs_project_1')
        self.assertEqual(kwargs['use_new_tmp_folder'], False)
        self.assertEqual(
            kwargs['worker_callback'].__name__, "copy_files_to_gcs")


class TestDeleteFilesIn(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    def test_valid_gcs_path(self, storage_client):
        valid_path = "gs://gcs_bucket"
        delete_files_in(valid_path, "gcs_project_1")

    @patch('google.cloud.storage.Client')
    def test_invalid_gcs_path(self, storage_client):
        invalid_path = "gcs_bucket"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            delete_files_in(invalid_path, "gcs_project_1")
        invalid_path = "gs://"
        with pytest.raises(
                Exception, match=r'Not a valid GCS tmp path.'):
            delete_files_in(invalid_path, "gcs_project_1")

    @patch('google.cloud.storage.Client')
    def test_file_uploads(self, storage_client):
        gcs_bucket_path = "gs://gcs_bucket/tmp_path/tmp2/"
        mock_storage = MagicMock()
        storage_client.return_value = mock_storage
        mock_blobs = [MagicMock(), MagicMock()]
        mock_bucket = MagicMock()
        mock_bucket.list_blobs = MagicMock(return_value=mock_blobs)
        mock_storage.get_bucket = MagicMock(return_value=mock_bucket)

        delete_files_in(gcs_bucket_path, "gcs_project_1", "*.log")
        storage_client.assert_called_with(project="gcs_project_1")
        mock_storage.get_bucket.assert_called_with("gcs_bucket")
        mock_bucket.list_blobs.assert_called_with(prefix="tmp_path/tmp2/",
                                                  delimiter="*.log")
        mock_blobs[0].delete.assert_called_once_with()
        mock_blobs[1].delete.assert_called_once_with()


class TestDownloadFileAsString(unittest.TestCase):
    @patch('google.cloud.storage.Client')
    def test_download(self, storage_client):
        gcs_uri = "gs://gcs_bucket/some_folder_path/file.toml"
        mock_storage = MagicMock()
        storage_client.return_value = mock_storage
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.get_bucket = MagicMock(return_value=mock_bucket)
        mock_bucket.get_blob = MagicMock(return_value=mock_blob)
        mock_blob.download_as_string = MagicMock(return_value="file_contents")

        self.assertEqual(download_file_as_string(gcs_uri), "file_contents")
        storage_client.assert_called_with()
        mock_bucket.get_blob.assert_called_with("some_folder_path/file.toml")
        mock_blob.download_as_string.assert_called_with()
