from bqsqoop.utils import typed
from bqsqoop.utils.gcloud import auth, storage, bigquery


class BigqueryParquetLoadJob():
    """Loads local parquet files data into bigquery using google storage

    Init will _errors.append(a error if config is invalid)
    If invalid, errors property will contains the config errors
    and a flag `is_config_valid` is updated accordingly

    Args:
        configs (dict): Job's Bigquery sub-section configs as a dict
    """

    def __init__(self, configs):
        self._project_id = configs.get("project_id")
        self._dataset_name = configs.get("dataset_name")
        self._table_name = configs.get("table_name")
        self._gcs_tmp_path = configs.get("gcs_tmp_path")
        self._service_account_key = configs.get('service_account_key')
        self._validate_configs()

    def _validate_configs(self):
        self.errors = {}
        self.is_config_valid = True
        _res = typed.non_empty_string(self._project_id)
        if _res:
            self.errors["project_id"] = _res
        _res = typed.non_empty_string(self._dataset_name)
        if _res:
            self.errors["dataset_name"] = _res
        _res = typed.non_empty_string(self._table_name)
        if _res:
            self.errors["table_name"] = _res
        _res = typed.non_empty_string(self._gcs_tmp_path)
        if _res:
            self.errors["gcs_tmp_path"] = _res
        if self.errors:
            self.is_config_valid = False

    def execute(self, files):
        """Executes the job to load parquet files to Bigquery tables

        Args:
            files (list_of_str): List of files to be uploaded to bigquery.
                Should be full file paths

        Returns:
            None if the job is successful, errors if failed
        """
        auth.setup_credentials(self._service_account_key)
        _gcs_dest_path = storage.copy_files_to_gcs(files, self._gcs_tmp_path,
                                                   use_new_tmp_folder=True)
        bigquery.load_parquet_files(
            _gcs_dest_path + "*.parq",
            self._project_id,
            self._dataset_name,
            self._table_name
        )
