import os


def setup_credentials(config):
    """Setups up GCP credentials using key from vm or from config

    Args:
        config: Config dict of the Sqoop Job

    Returns:
        None
    """
    _use_default_acc = config.get('use_default_service_account', False)
    if _use_default_acc:
        _check_for_default_credential()
    else:
        _setup_credentials_from_config(config)


def _check_for_default_credential():
    _credential_env = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
    if not _credential_env:
        raise Exception(
            "GCP credentials not found in the VM, " +
            "Please set GOOGLE_APPLICATION_CREDENTIALS in global env")


def _setup_credentials_from_config(config):
    _service_account_key = config.get('service_account_key_file')
    if not _service_account_key:
        raise Exception("service_account_key_file not set in config")
    _service_account_key_file = '/tmp/gcp_service_account_key.json'
    with open(_service_account_key_file, 'w+') as f:
        f.write(_service_account_key)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _service_account_key_file
