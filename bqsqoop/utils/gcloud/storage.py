import re
import uuid
import logging

from google.cloud import storage


def copy_files_to_gcs(files, gcs_bucket_path, use_new_tmp_folder=False):
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
    _client = storage.Client()
    _bucket_name, _sub_folder = _get_details_from_gcs_path(
        gcs_bucket_path, use_new_tmp_folder)
    _bucket = _client.get_bucket(_bucket_name)
    for _file in files:
        _filename = _file.split('/')[-1]
        _blob = _bucket.blob(_sub_folder + _filename)
        _blob.upload_from_filename(filename=_file)
    return "gs://" + _bucket_name + "/" + _sub_folder


def delete_files_in(gcs_bucket_path, delimiter=None):
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
    _client = storage.Client()
    _bucket_name, _sub_folder = _get_details_from_gcs_path(
        gcs_bucket_path, False)
    _bucket = _client.get_bucket(_bucket_name)
    blobs = _bucket.list_blobs(prefix=_sub_folder, delimiter=delimiter)
    for blob in blobs:
        blob.delete()
        logging.debug("Deleted blob: {}".format(blob.name))


def _validate_gcs_path(gcs_path):
    gcs_bucket_path_re_pattern = r'gs://.+'
    _match_obj = re.match(gcs_bucket_path_re_pattern, gcs_path)
    if not _match_obj:
        raise Exception("Not a valid GCS tmp path.")


def _get_details_from_gcs_path(gcs_bucket_path, use_new_tmp_folder):
    _gcs_bucket_path = gcs_bucket_path.replace("gs://", "")
    _bucket_name = _gcs_bucket_path.split("/")[0]
    _sub_folder = "/".join(_gcs_bucket_path.split("/")[1:])
    if _sub_folder != "" and not re.match(r'(.*)\/$', _sub_folder):
        _sub_folder = _sub_folder + '/'
    if use_new_tmp_folder:
        _sub_folder = _sub_folder + str(uuid.uuid4()) + "/"
    return _bucket_name, _sub_folder
