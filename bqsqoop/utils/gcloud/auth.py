import os


def setup_credentials(key=None):
    """Setups up GCP credentials

    If key's given will setup gcloud utils configs from it,
    else will check for existence default credentials to use

    Args:
        key (str): Google service account key json string

    Returns:
        None
    """
    if key:
        _setup_credentials_from_config(key)
    else:
        _check_for_default_credential()


def _check_for_default_credential():
    _credential_env = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
    if not _credential_env:
        raise Exception(
            "GCP credentials not found in the VM, " +
            "Please set GOOGLE_APPLICATION_CREDENTIALS in global env")


def _setup_credentials_from_config(key):
    _service_account_key_file = '/tmp/gcp_service_account_key.json'
    with open(_service_account_key_file, 'w+') as f:
        f.write(key)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _service_account_key_file
