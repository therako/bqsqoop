import os
import pytest
import unittest
from bqsqoop.utils.gcloud.auth import setup_credentials


class TestSetupCredentials(unittest.TestCase):
    def test_using_default_service_account(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth_file.json"
        setup_credentials()

    def test_error_no_default_key(self):
        with pytest.raises(
            Exception, match="GCP credentials not found in the VM, " +
                "Please set GOOGLE_APPLICATION_CREDENTIALS in global env"):
            setup_credentials()

    def test_service_key_from_config(self):
        _service_account_key_file = '/tmp/gcp_service_account_key.json'
        _key_json = '{"acc": "someone@gcp.com"}'
        setup_credentials(_key_json)
        self.assertEqual(os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
                         _service_account_key_file)
        with open(_service_account_key_file, "r") as f:
            self.assertEqual(f.read(), _key_json)
