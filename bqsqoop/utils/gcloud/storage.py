import re
import uuid

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
        # _filename = _file.split('/')[-1]
        _blob = _bucket.blob(_sub_folder + _file)
        _blob.upload_from_filename(filename=_file)
    return "gs://" + _bucket_name + "/" + _sub_folder


def _validate_gcs_path(gcs_path):
    gcs_bucket_path_re_pattern = r'gs://.+'
    _match_obj = re.match(gcs_bucket_path_re_pattern, gcs_path)
    if not _match_obj:
        raise Exception("Not a valid GCS tmp path.")


def _get_details_from_gcs_path(gcs_bucket_path, use_new_tmp_folder):
    gcs_sub_folder_re_pattern = r'gs:\/\/(\w*)\/?(.*)'
    _match_obj = re.match(
        gcs_sub_folder_re_pattern, gcs_bucket_path)
    _bucket_name = ""
    _sub_folder = ""
    if _match_obj:
        _bucket_name = _match_obj.group(1)
        _sub_folder = _match_obj.group(2)
    if _sub_folder != "" and not re.match(r'(.*)\/$', _sub_folder):
        _sub_folder = _sub_folder + '/'
    if use_new_tmp_folder:
        _sub_folder = _sub_folder + str(uuid.uuid4()) + "/"
    return _bucket_name, _sub_folder