import os


def setup_credentials(_config):
    if 'use_default_service_account' not in _config:
        _setup_credentials_from_config(_config)
    elif _config['use_default_service_account'] == True:
        _check_for_default_credential()


def _check_for_default_credential():
    _credential_env = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', None)
    if not _credential_env:
        raise Exception(
            "GCP credentials not found in the VM, " + 
            "Please set GOOGLE_APPLICATION_CREDENTIALS in global env")


def _setup_credentials_from_config(_config):
    _service_account_key = _config.get('service_account_key_file')
    if not _service_account_key:
        raise Exception("service_account_key_file not set in config")
    _service_account_key_file = '/tmp/gcp_service_account_key.json'
    with open(_service_account_key_file, 'w+') as f:
        f.write(_service_account_key)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = _service_account_key_file
