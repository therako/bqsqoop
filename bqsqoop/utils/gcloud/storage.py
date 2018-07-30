import re
import uuid
import logging

from google.cloud import storage
from bqsqoop.utils import async_worker


def copy_files_to_gcs(files, gcs_bucket_path, project_id,
                      use_new_tmp_folder=False):
    """copies given files to GCS bucket path

    Args:
        files (list_of_str): List of files to be uploaded.
            Should be full file paths
        gcs_bucket_path (str): Should be a path in GCS.
            Starts with gs://
        use_new_tmp_folder (bool): If set true will upload all files
            under a new folder in gcs_bucket_path

    Returns:
        str: path to the folder in gcs where the files are uploaded
    """
    _validate_gcs_path(gcs_bucket_path)
    client = storage.Client(project=project_id)
    bucket_name, folder_path, _ = _get_details_from_gcs_path(
        gcs_bucket_path, use_new_tmp_folder)
    bucket = client.get_bucket(bucket_name)
    for file in files:
        filename = file.split('/')[-1]
        blob = bucket.blob(folder_path + filename)
        blob.upload_from_filename(filename=file)
    return "gs://" + bucket_name + "/" + folder_path


def parallel_copy_files_to_gcs(files, gcs_bucket_path, project_id,
                               use_new_tmp_folder=False):
    """copies given files to GCS bucket path using multi-process

    Same as copy_files_to_gcs, except this method does a parallel upload
    one file per process.
    """
    _validate_gcs_path(gcs_bucket_path)
    _async_worker = async_worker.AsyncWorker(len(files))
    if use_new_tmp_folder:
        # Add tmp folder outside and make sure all files are in the same path
        gcs_bucket_path = gcs_bucket_path + str(uuid.uuid4()) + "/"
    for file in files:
        _async_worker.send_data_to_worker(
            worker_callback=copy_files_to_gcs,
            files=[file],
            gcs_bucket_path=gcs_bucket_path,
            project_id=project_id,
            use_new_tmp_folder=False
        )
    logging.debug('Waiting for all files to be uploaded to GCS...')
    _async_worker.get_job_results()
    return gcs_bucket_path


def delete_files_in(gcs_bucket_path, project_id, delimiter=None):
    """Deletes all files in given GCS bucket path

    Args:
        gcs_bucket_path (str): Should be a path in GCS.
            Starts with gs://
        delimiter (str): can be used to restrict the results to only the
            "files" in the given "folder". Without the delimiter,
            the entire tree under the prefix is returned.
    Returns:
        None
    """
    _validate_gcs_path(gcs_bucket_path)
    client = storage.Client(project=project_id)
    bucket_name, folder_path, _ = _get_details_from_gcs_path(
        gcs_bucket_path, False)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path, delimiter=delimiter)
    for blob in blobs:
        blob.delete()
        logging.debug("Deleted blob: {}".format(blob.name))


def download_file_as_string(gcs_uri):
    """Download a file from given GCS uri

    Args:
        gcs_uri (str): Should be a path in GCS.
            Starts with gs://

    Returns: (str)
        Files content as a string
    """
    _validate_gcs_path(gcs_uri)
    client = storage.Client()
    bucket_name, _, file_path = _get_details_from_gcs_path(
        gcs_uri, False)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_path)
    return blob.download_as_string()


def _validate_gcs_path(gcs_path):
    gcs_bucket_path_re_pattern = r'gs://.+'
    _match_obj = re.match(gcs_bucket_path_re_pattern, gcs_path)
    if not _match_obj:
        raise Exception("Not a valid GCS tmp path.")


def _get_details_from_gcs_path(gcs_bucket_path, use_new_tmp_folder):
    gcs_bucket_path = gcs_bucket_path.replace("gs://", "")
    bucket_name = gcs_bucket_path.split("/")[0]
    file_path = "/".join(gcs_bucket_path.split("/")[1:])
    folder_path = file_path
    if folder_path != "" and not re.match(r'(.*)\/$', folder_path):
        folder_path = folder_path + '/'
    if use_new_tmp_folder:
        folder_path = folder_path + str(uuid.uuid4()) + "/"
    return bucket_name, folder_path, file_path
