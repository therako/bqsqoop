import re
from google.cloud import storage


def copy_files_to_gcs(files, gcs_bucket_path):
    """copies given files to GCS bucket path

    Args:
        files (list_of_str): List of files to be uploaded.
            Should be full file paths
        gcs_bucket_path (str): Should be a path in GCS.
            Starts with gs://

    Returns:
        None
    """
    _validate_gcs_path(gcs_bucket_path)
    _client = storage.Client()
    _bucket_name, _sub_folder = _get_details_from_gcs_path(gcs_bucket_path)
    _bucket = _client.get_bucket(_bucket_name)
    for _file in files:
        # _filename = _file.split('/')[-1]
        _blob = _bucket.blob(_sub_folder + _file)
        _blob.upload_from_filename(filename=_file)


def _validate_gcs_path(gcs_path):
    gcs_bucket_path_re_pattern = r'gs://.+'
    _match_obj = re.match(gcs_bucket_path_re_pattern, gcs_path)
    if not _match_obj:
        raise Exception("Not a valid GCS tmp path.")


def _get_details_from_gcs_path(gcs_bucket_path):
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
    return _bucket_name, _sub_folder
