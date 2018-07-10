import re
from google.cloud import storage

GCS_BUCKET_PATH_RE_PATTERN = r'gs://.*'
GCS_BUCKET_NAME_RE_PATTERN = r'gs://(.*)[/.*]'
GCS_SUB_FOLDER_RE_PATTERN = r'gs:\/\/(\w*)\/(.*)'


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
    _client = storage.Client()
    _bucket_name, _sub_folder = _get_details_from_gcs_path(gcs_bucket_path)
    _bucket = _client.get_bucket(_bucket_name)
    for _file in files:
        # _filename = _file.split('/')[-1]
        _blob = _bucket.blob(_sub_folder + _file)
        _blob.upload_from_filename(filename=_file)


def _get_details_from_gcs_path(gcs_bucket_path):
    _match_obj = re.match(GCS_BUCKET_PATH_RE_PATTERN, gcs_bucket_path)
    if not _match_obj:
        raise Exception("Not a valid GCS tmp path.")
    _bucket_name = re.match(
        GCS_BUCKET_NAME_RE_PATTERN, gcs_bucket_path).group(1)
    _match_obj = re.match(
        GCS_SUB_FOLDER_RE_PATTERN, gcs_bucket_path)
    _sub_folder = ""
    if _match_obj:
        _sub_folder = _match_obj.group(2)
    if not re.match(r'(.*)\/$', _sub_folder):
        _sub_folder = _sub_folder + '/'
    return _bucket_name, _sub_folder
