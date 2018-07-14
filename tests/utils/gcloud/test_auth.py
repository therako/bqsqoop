import os
import pytest
import unittest
from bqsqoop.utils.gcloud.auth import setup_credentials


class TestSetupCredentials(unittest.TestCase):
    def test_using_default_service_account(self):
        config = {
            'use_default_service_account': True
        }
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth_file.json"
        setup_credentials(config)

    def test_error_no_default_key(self):
        config = {
            'use_default_service_account': True
        }
        with pytest.raises(
            Exception, match="GCP credentials not found in the VM, " +
                "Please set GOOGLE_APPLICATION_CREDENTIALS in global env"):
            setup_credentials(config)

    def test_service_key_from_config(self):
        _service_account_key_file = '/tmp/gcp_service_account_key.json'
        _key_json = '{"acc": "someone@gcp.com"}'
        config = {
            'use_default_service_account': False,
            'service_account_key_file': _key_json
        }
        setup_credentials(config)
        self.assertEqual(os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
                         _service_account_key_file)
        with open(_service_account_key_file, "r") as f:
            self.assertEqual(f.read(), _key_json)

    def test_error_service_key_not_found_in_config(self):
        config = {
            'service_account_key_file': None
        }
        with pytest.raises(
                Exception, match="service_account_key_file not set in config"):
            setup_credentials(config)
